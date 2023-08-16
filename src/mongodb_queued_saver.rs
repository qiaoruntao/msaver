use std::{env, mem};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use mongodb::bson::Document;
use rusqlite::{Connection, params};
use serde::Serialize;
use tokio::sync::Mutex;
use tower::util::ServiceExt;
use tower_batch::{Batch, BatchControl, BoxError};
use tower_service::Service;
use tracing::{error, info, info_span, instrument, trace, warn};

use crate::mongodb_saver::{MongodbSaver, RowData};

pub struct MongodbQueuedSaver {
    queue: Arc<Mutex<HashMap<String, Arc<Mutex<Batch<FlushService, Document>>>>>>,
    saver: MongodbSaver,
}

pub struct FlushService {
    collection_name: String,
    docs: Arc<std::sync::Mutex<Vec<Document>>>,
    saver: Arc<MongodbSaver>,
}

impl FlushService {
    pub fn create(collection_name: impl AsRef<str> + Debug, saver: MongodbSaver) -> Self {
        FlushService {
            collection_name: collection_name.as_ref().to_string(),
            docs: Arc::new(std::sync::Mutex::new(Vec::with_capacity(100))),
            saver: Arc::new(saver),
        }
    }

    async fn save_batch_wrap(vec: Vec<Document>, saver: Arc<MongodbSaver>, str: String) -> Result<(), BoxError> {
        match saver.save_collection_batch(str, vec.as_slice()).await {
            Ok(_) => {
                Ok(())
            }
            Err(e) => {
                Err(BoxError::from(e.to_string()))
            }
        }
    }
}

impl Service<BatchControl<Document>> for FlushService {
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output=Result<(), BoxError>> + Send + Sync + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }


    fn call(&mut self, req: BatchControl<Document>) -> Self::Future {
        match req {
            BatchControl::Item(doc) => {
                {
                    let mut mutex_guard = self.docs.lock().unwrap();
                    mutex_guard.push(doc);
                }
                info!("doc pushed");
                Box::pin(futures::future::ready(Ok(())))
            }
            BatchControl::Flush => {
                let vec: Vec<Document> = {
                    let mut mutex_guard = self.docs.lock().unwrap();
                    mem::replace(mutex_guard.deref_mut(), Vec::<Document>::with_capacity(100))
                };
                info!("flushed");
                Box::pin(Self::save_batch_wrap(vec, self.saver.clone(), self.collection_name.clone()))
            }
        }
    }
}

impl Drop for FlushService {
    fn drop(&mut self) {
        let docs = self.docs.lock().unwrap();
        if docs.is_empty() {
            trace!("buffer is empty, flush service exits now");
            return;
        }
        let sqlite_path = env::var("TempSqlitePath").unwrap_or("./sqlite_temp_drop.sqlite".into());
        let sqlite_connection = match Connection::open(&sqlite_path) {
            Ok(v) => { Arc::new(v) }
            Err(e) => {
                error!("failed to open sqlite database {}", e);
                return;
            }
        };
        if let Err(e) = sqlite_connection.execute(
            "CREATE TABLE saved (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_name  TEXT NOT NULL, data  TEXT NOT NULL)",
            (), // empty list of parameters.
        ) {
            warn!("{}", e);
        }
        for doc in docs.as_slice() {
            let data = RowData {
                id: 0,
                collection_name: self.collection_name.clone(),
                data: serde_json::to_string(&doc).unwrap(),
            };
            match sqlite_connection.as_ref().execute("INSERT INTO saved (collection_name, data) VALUES (?1, ?2)",
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
        MongodbQueuedSaver {
            queue: Arc::new(Default::default()),
            saver,
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
            let mut mutex_guard = self.queue.lock().await;
            let arc = mutex_guard.entry(collection_name.as_ref().to_string()).or_insert_with(|| {
                trace!("init flush service now");
                let flush_service = FlushService::create(collection_name, self.saver.clone());
                let batch: Batch<FlushService, Document> = Batch::new(flush_service, 100, Duration::from_secs(3));
                Arc::new(Mutex::new(batch))
            }).clone();
            arc
        };

        let mut guard = batch.lock().await;
        let batch_guard = guard.deref_mut();
        let flush_service_batch = batch_guard.ready().await.unwrap();
        // flush_service_batch.map_request()
        tokio::spawn(flush_service_batch.call(document));
        true
    }
}


#[cfg(test)]
mod test {
    use std::env;
    use std::time::Duration;

    use mongodb::bson::doc;
    use qrt_log_utils::init_logger;
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
        for index in 0..1000 {
            let now = chrono::Local::now();
            let result = queued_saver.save_collection("aaa", &doc! {"time":now, "data":&index}).await;
            assert!(result, "failed to insert value")
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    // TODO: drop is not necessary since not all messages are sent to flush service
    #[tokio::test]
    async fn test_drop() {
        init_logger("test_drop", None);
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