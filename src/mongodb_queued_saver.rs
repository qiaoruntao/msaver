use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use mongodb::bson::Document;
use qrt_log_utils::opentelemetry::KeyValue;
use rusqlite::{Connection, params};
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::{error, info_span, instrument, trace, warn};

use crate::mongodb_saver::{MongodbSaver, RowData};

pub struct MongodbQueuedSaver {
    queue: Arc<Mutex<HashMap<String, Arc<Flusher>>>>,
    saver: Arc<MongodbSaver>,
    sqlite_connection: Arc<Mutex<Connection>>,
}

pub struct Flusher {
    collection_name: String,
    saver: Arc<MongodbSaver>,
    data_array: Arc<Mutex<Vec<Document>>>,
    sqlite_connection: Arc<Mutex<Connection>>,
    max_len: usize,
    batch_len: usize,
    flush_handler: Mutex<Option<JoinHandle<()>>>,
}

impl Flusher {
    pub fn create(collection_name: String, saver: Arc<MongodbSaver>, sqlite_connection: Arc<Mutex<Connection>>, max_len: usize, batch_len: usize) -> Flusher {
        Flusher {
            collection_name,
            max_len,
            batch_len,
            saver,
            sqlite_connection,
            flush_handler: Default::default(),
            data_array: Default::default(),
        }
    }

    pub async fn flush2remote() {}

    pub fn flush2local() {}

    /// handle add new document to write
    pub async fn add_data(&self, doc: impl Into<Document>) -> bool {
        let document = doc.into();
        let current_len = {
            let mutex_guard = self.data_array.lock().unwrap();
            mutex_guard.len()
        };
        // let properties = [KeyValue::new("collection_name", self.collection_name.clone())];
        if current_len >= self.batch_len {
            // if batch size is reached, try to spawn write thread first
            let mut mutex_guard = self.flush_handler.lock().unwrap();
            // dbg!(mutex_guard.is_none());
            if mutex_guard.is_none() || mutex_guard.as_ref().unwrap().is_finished() {
                trace!("spawn write thread now");
                let batch_data_vec = {
                    let mut mutex_guard = self.data_array.lock().unwrap();
                    let mut temp = Vec::with_capacity(self.batch_len);
                    for _ in 0..self.batch_len {
                        match mutex_guard.pop() {
                            None => {
                                break;
                            }
                            Some(v) => {
                                temp.push(v);
                            }
                        }
                    }
                    temp
                };
                let saver = self.saver.clone();
                let collection_name = self.collection_name.clone();
                let handle = tokio::spawn(async move {
                    match saver.save_collection_batch(&collection_name, &batch_data_vec).await {
                        Ok(_) => {
                        }
                        Err(e) => {
                            error!("failed to flush to mongodb, collection is {} e={}",collection_name,e);
                        }
                    }
                    trace!("completed");
                });
                *mutex_guard = Some(handle);
            }
        }
        return if current_len >= self.max_len {
            // queue full, write to local now
            // {
            //     let meter = global::meter("msaver");
            //     let counter = meter.u64_counter("msaver-direct-local-count").init();
            //     counter.add(1, &properties);
            // }
            self.saver.write_local(&self.collection_name, &document).await
        } else {
            trace!("push data to queue now");
            // write to queue now
            self.data_array.lock().unwrap().push(document);
            true
        };
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        let docs = self.data_array.lock().unwrap();
        if docs.is_empty() {
            trace!("buffer is empty, flush service exits now");
            return;
        }
        trace!("start to drop {} docs", docs.len());
        for doc in docs.as_slice() {
            let data = RowData {
                id: 0,
                collection_name: self.collection_name.clone(),
                data: serde_json::to_string(&doc).unwrap(),
            };
            match self.sqlite_connection.lock().unwrap().execute("INSERT INTO saved (collection_name, data) VALUES (?1, ?2)",
                                                                 params![&data.collection_name, &data.data]) {
                Ok(_) => {}
                Err(e) => {
                    error!("failed to insert data into local sqlite, e={}",e);
                    return;
                }
            }
        }
    }
}

impl MongodbQueuedSaver {
    pub fn create(saver: MongodbSaver) -> Self {
        let sqlite_path = env::var("TempSqlitePath").unwrap_or("./sqlite_temp.sqlite".into());
        let sqlite_connection = match Connection::open(&sqlite_path) {
            Ok(v) => { Arc::new(Mutex::new(v)) }
            Err(e) => {
                panic!("failed to open sqlite database {}", e);
            }
        };
        // make sure table is created
        if let Err(e) = sqlite_connection.lock().unwrap().execute(
            "CREATE TABLE saved (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_name  TEXT NOT NULL, data  TEXT NOT NULL)",
            (), // empty list of parameters.
        ) {
            warn!("{}", e);
        }
        MongodbQueuedSaver {
            queue: Arc::new(Default::default()),
            saver: Arc::new(saver),
            sqlite_connection,
        }
    }

    #[instrument(skip(self, obj))]
    pub async fn save_collection<T: Serialize>(&self, collection_name: impl AsRef<str> + Debug, obj: &T) -> bool {
        trace!("save into collection {}",collection_name.as_ref());
        let document = match info_span!("serialize_part").in_scope(|| { mongodb::bson::to_document(obj) }) {
            Ok(v) => { v }
            Err(e) => {
                error!("failed to serialize obj {}",e);
                return false;
            }
        };
        let batch = {
            let mut mutex_guard = self.queue.lock().unwrap();
            let collection_name = collection_name.as_ref().to_string();
            let arc = mutex_guard.entry(collection_name.clone()).or_insert_with(|| {
                trace!("init flush service now");
                let flusher = Flusher::create(collection_name, self.saver.clone(), self.sqlite_connection.clone(), 5000, 100);
                Arc::new(flusher)
            });
            arc.clone()
        };

        batch.add_data(document).await
    }
}


#[cfg(test)]
mod test {
    use std::env;
    use std::time::Duration;

    use mongodb::bson::doc;
    use qrt_log_utils::{init_logger, LoggerConfig};
    use serde::Serialize;

    use crate::mongodb_queued_saver::MongodbQueuedSaver;
    use crate::mongodb_saver::MongodbSaver;

    #[derive(Serialize)]
    struct TestData {
        num: i32,
    }

    #[tokio::test]
    async fn test_bunk() {
        let saver_db_str = env::var("MongoDbSaverStr").expect("need saver db str");
        let mongodb_saver = MongodbSaver::init(saver_db_str.as_str()).await;
        let queued_saver = MongodbQueuedSaver::create(mongodb_saver);
        for index in 0..10 {
            let now = chrono::Local::now();
            let result = queued_saver.save_collection("aaa", &doc! {"time":now, "data":&index}).await;
            assert!(result, "failed to insert value")
        }
        for _ in 0..10 {
            for index in 0..100 {
                let now = chrono::Local::now();
                let result = queued_saver.save_collection("aaa", &doc! {"time":now, "data":&index}).await;
                assert!(result, "failed to insert value")
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        tokio::time::sleep(Duration::from_secs(4)).await;
    }

    // TODO: drop is not necessary since not all messages are sent to flush service
    #[tokio::test]
    async fn test_drop() {
        init_logger("test_drop", LoggerConfig::default());
        let saver_db_str = env::var("MongoDbSaverStr").expect("need saver db str");
        let mongodb_saver = MongodbSaver::init(saver_db_str.as_str()).await;
        let queued_saver = MongodbQueuedSaver::create(mongodb_saver);
        for index in 0..100 {
            let now = chrono::Local::now();
            let result = queued_saver.save_collection("aaa", &doc! {"time":now, "data":&index}).await;
            assert!(result, "failed to insert value")
        }
        // drop(queued_saver);
    }
}