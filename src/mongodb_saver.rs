use std::borrow::Borrow;
use std::env;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chrono::{DateTime, Local};
use deadpool_sqlite::{Config, Pool, Runtime};
use futures::StreamExt;
use mongodb::{Client, Collection, Database};
use mongodb::bson::{doc, Document};
use mongodb::error::{BulkWriteFailure, Error, ErrorKind, WriteError, WriteFailure};
use mongodb::error::ErrorKind::BulkWrite;
use mongodb::options::{ClientOptions, InsertManyOptions, InsertOneOptions, ResolverConfig, WriteConcern};
use mongodb::results::{InsertManyResult, InsertOneResult};
use once_cell::sync::OnceCell;
use qrt_log_utils::global;
use qrt_log_utils::opentelemetry::KeyValue;
use qrt_log_utils::opentelemetry::metrics::{Histogram, Unit};
use qrt_log_utils::tracing::{error, info, instrument};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::{info_span, warn};
use tracing::log::trace;

#[derive(Clone)]
pub struct MongodbSaver {
    database: Database,
    dirty: Arc<AtomicBool>,
    cleaning: Arc<AtomicBool>,
    sqlite_pool: Pool,
}

#[derive(Debug)]
pub struct RowData {
    pub(crate) id: i32,
    pub(crate) collection_name: String,
    pub(crate) data: String,
}

impl MongodbSaver {
    #[instrument]
    pub async fn init(connection_str: impl AsRef<str> + Debug) -> Self {
        let connection_str = connection_str.as_ref();
        let client_options = if cfg!(windows) && connection_str.contains("+srv") {
            ClientOptions::parse_with_resolver_config(connection_str, ResolverConfig::quad9()).await.unwrap()
        } else {
            ClientOptions::parse(connection_str).await.unwrap()
        };
        let target_database = client_options.default_database.clone().unwrap();
        // Get a handle to the deployment.
        let client = Client::with_options(client_options).unwrap();
        let database = client.database(target_database.as_str());

        // init split database
        let sqlite_path = env::var("TempSqlitePath").unwrap_or("./sqlite_temp.sqlite".into());

        // check if we can open the database, emit error before we really need to insert data
        let sqlite_config = Config::new(&sqlite_path);
        let pool = sqlite_config.create_pool(Runtime::Tokio1).unwrap();
        let conn = pool.get().await.unwrap();
        if let Err(e) = conn.interact(|conn| {
            if let Err(e) = conn.execute(
                "CREATE TABLE saved (id INTEGER PRIMARY KEY AUTOINCREMENT, collection_name  TEXT NOT NULL, data  TEXT NOT NULL)",
                (), // empty list of parameters.
            ) {
                error!("{}", e);
            }
        }).await {
            warn!("failed to create table {}",e);
        }
        MongodbSaver {
            database,
            sqlite_pool: pool,
            // force to check is dirty
            dirty: Arc::new(AtomicBool::new(true)),
            cleaning: Arc::new(AtomicBool::new(false)),
        }
    }

    #[instrument(skip(self))]
    pub fn get_collection<T: Serialize>(&self, collection_name: impl AsRef<str> + Debug) -> Collection<T> {
        self.database.collection(collection_name.as_ref())
    }

    #[instrument(skip(self, obj))]
    pub async fn save_collection<T: Serialize>(&self, collection_name: impl AsRef<str> + Debug, obj: &T) -> anyhow::Result<Option<InsertOneResult>> {
        let document = info_span!("serialize_part").in_scope(|| {
            let result = mongodb::bson::to_bson(obj).unwrap();
            let now = Local::now();
            doc! {"time":now, "data":&result}
        });
        tokio::select! {
            result = MongodbSaver::save_collection_inner(self.get_collection(collection_name.as_ref()), &document)=>{
                if result.is_ok() {
                    // mongodb connection ok, check if we need to clean sqlite database
                    self.clean_local();
                } else {
                    // this function is called outside of this module, can save to local now
                    if MongodbSaver::write_local_inner(self.sqlite_pool.clone(), collection_name.as_ref(), &document).await {
                        self.dirty.store(true, SeqCst);
                    }
                }
                result
            }
            _=tokio::time::sleep(Duration::from_secs(10))=>{
                let msg="mongodb save timeout, write local now";
                warn!(msg);
                if MongodbSaver::write_local_inner(self.sqlite_pool.clone(), collection_name.as_ref(), &document).await {
                    self.dirty.store(true, SeqCst);
                }
                Err(anyhow!(msg))
            }
        }
    }

    #[instrument(skip(self, obj))]
    pub async fn save_collection_with_time<T: Serialize>(&self, collection_name: impl AsRef<str> + Debug, obj: &T, now: DateTime<Local>) -> anyhow::Result<Option<InsertOneResult>> {
        let result = mongodb::bson::to_bson(obj)?;
        let document = doc! {"time":now, "data":&result};
        MongodbSaver::save_collection_inner(self.get_collection(collection_name), &document).await
    }

    #[instrument(skip(self, objs), fields(cnt = objs.len()))]
    pub async fn save_collection_batch<T: Serialize>(&self, collection_name: impl AsRef<str> + Debug, objs: &[T]) -> anyhow::Result<Option<InsertManyResult>> {
        let now = Local::now();
        let documents = info_span!("batch_serialize_part").in_scope(|| {
            objs.iter()
                // TODO
                .map(|obj| doc! {"time":now, "data":mongodb::bson::to_bson(obj).unwrap()})
                .collect::<Vec<_>>()
        });
        tokio::select! {
            result = self.save_collection_inner_batch(collection_name.as_ref(), &documents)=>{
                if result.is_err() {
                    self.dirty.store(true, SeqCst);
                    // TODO: optimize
                    for document in documents {
                        let collection_name=collection_name.as_ref().to_string();
                        let pool=self.sqlite_pool.clone();
                        tokio::spawn(async move{
                            MongodbSaver::write_local_inner(pool, collection_name, &document).await
                        });
                    }
                } else {
                    self.clean_local();
                }
                result
            }
            _=tokio::time::sleep(Duration::from_secs(10))=>{
                let msg="mongodb save batch timeout, write local now";
                warn!(msg);
                self.dirty.store(true, SeqCst);
                for document in documents {
                    let collection_name=collection_name.as_ref().to_string();
                    let pool=self.sqlite_pool.clone();
                    tokio::spawn(async move{
                        MongodbSaver::write_local_inner(pool, collection_name, &document).await
                    });
                }
                Err(anyhow!(msg))
            }
        }
    }

    #[instrument(skip_all)]
    pub(crate) async fn save_collection_inner_batch(&self, collection_name: impl AsRef<str> + Debug, all_documents: &[Document]) -> anyhow::Result<Option<InsertManyResult>> {
        let collection: Collection<Document> = self.get_collection(collection_name);
        let insert_options = {
            let mut temp_write_concern = WriteConcern::default();
            temp_write_concern.w_timeout = Some(Duration::from_secs(3));
            let mut temp = InsertManyOptions::default();
            temp.write_concern = Option::from(temp_write_concern);
            temp.ordered = Some(false);
            temp
        };
        match collection.insert_many(all_documents, Some(insert_options)).await {
            Ok(val) => {
                Ok(Some(val))
            }
            Err(e) => {
                match &e {
                    Error { kind, .. } => {
                        match kind.as_ref() {
                            BulkWrite(BulkWriteFailure { write_errors: Some(write_errors), .. }) => {
                                // we are not sure which document cause the error(though message will contain the detail message ), so we need to retry every failed documents
                                let first_non_duplicate_error = write_errors.iter().find(|write_error| {
                                    write_error.code != 11000
                                });
                                if let Some(err) = first_non_duplicate_error {
                                    // TODO: inserted_ids is private, cannot access it, we need to retry all documents
                                    info!("contain_non_duplicate_error, first one is {:?}", err);
                                    Err(e.into())
                                } else {
                                    // otherwise ignore error
                                    Ok(None)
                                }
                            }
                            _ => {
                                Err(e.into())
                            }
                        }
                    }
                    _ => {
                        Err(e.into())
                    }
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn save_collection_inner(collection: Collection<Document>, full_document: &Document) -> anyhow::Result<Option<InsertOneResult>> {
        static SAVE_DOCUMENT_TIME_INSTANCE: OnceCell<Histogram<u64>> = OnceCell::new();
        let save_document_time = SAVE_DOCUMENT_TIME_INSTANCE.get_or_init(|| {
            let meter = global::meter("msaver");
            let histogram = meter.u64_histogram("msaver-save-document-time").with_unit(Unit::new("ms")).init();
            histogram
        });

        let start_time = Instant::now();

        let insert_one_options = {
            let mut temp_write_concern = WriteConcern::default();
            temp_write_concern.w_timeout = Some(Duration::from_secs(3));

            let mut temp = InsertOneOptions::default();
            temp.write_concern = Option::from(temp_write_concern);
            temp
        };
        let result = match collection.insert_one(full_document, Some(insert_one_options)).await {
            Ok(val) => {
                Ok(Some(val))
            }
            Err(e) => {
                let x = e.kind.borrow();
                match x {
                    // E11000 duplicate key error collection, ignore it
                    // TODO: report to the caller?
                    ErrorKind::Write(WriteFailure::WriteError(WriteError { code: 11000, .. })) => {
                        Ok(None)
                    }
                    _ => {
                        Err(e.into())
                    }
                }
            }
        };
        let success_mark = if result.is_ok() {
            "True"
        } else {
            "False"
        };
        save_document_time.record(start_time.elapsed().as_millis() as u64,
                                  &[KeyValue::new("collection_name", collection.name().to_string()), KeyValue::new("success", success_mark), KeyValue::new("batch_size", 1)],
        );

        result
    }

    #[instrument(skip(self, pipeline))]
    pub async fn aggregate_one<T: Serialize + DeserializeOwned>(&self, collection_name: impl AsRef<str> + Debug, pipeline: impl IntoIterator<Item=Document>) -> anyhow::Result<T> {
        let collection = self.get_collection::<Document>(collection_name);
        let find_result = collection.aggregate(pipeline, None).await;
        if let Err(e) = find_result {
            return Err(e.into());
        }
        let mut cursor = find_result.unwrap();
        let next = cursor.next().await;
        match next {
            None => {
                Err(anyhow!("not found"))
            }
            Some(Err(e)) => {
                Err(anyhow!(e))
            }
            Some(Ok(value)) => {
                match mongodb::bson::from_document(value) {
                    Ok(value) => {
                        Ok(value)
                    }
                    Err(e) => {
                        Err(anyhow!(e))
                    }
                }
            }
        }
    }
    #[instrument(skip(arc, document))]
    pub async fn write_local_inner<DataType: ?Sized + Serialize>(arc: Pool, collection_name: impl AsRef<str> + Debug, document: &DataType) -> bool {
        trace!("write local now");
        static WRITE_LOCAL_TIME_INSTANCE: OnceCell<Histogram<u64>> = OnceCell::new();
        let write_local_time = WRITE_LOCAL_TIME_INSTANCE.get_or_init(|| {
            let meter = global::meter("msaver");
            let histogram = meter.u64_histogram("msaver-write-local-time-ms").with_unit(Unit::new("ms")).init();
            histogram
        });

        let start_time = Instant::now();

        let conn = arc.get().await.unwrap();

        let data = serde_json::to_string(&document).unwrap();
        let data = RowData {
            id: 0,
            collection_name: collection_name.as_ref().to_string(),
            data,
        };
        let result = conn.interact(move |conn| {
            match conn.execute(
                "INSERT INTO saved (collection_name, data) VALUES (?1, ?2)",
                (&data.collection_name, &data.data),
            ) {
                Ok(_) => {
                    true
                }
                Err(e) => {
                    error!("{}", e);
                    false
                }
            }
        }).await.unwrap();

        write_local_time.record(start_time.elapsed().as_millis() as u64, &[KeyValue::new("collection_name", collection_name.as_ref().to_string())]);

        result
    }

    #[instrument(skip(self, document))]
    pub async fn write_local<DataType: ?Sized + Serialize>(&self, collection_name: impl AsRef<str> + Debug, document: &DataType) -> bool {
        let is_written = MongodbSaver::write_local_inner(self.sqlite_pool.clone(), &collection_name, document).await;
        if is_written {
            self.dirty.store(true, SeqCst);
        }
        is_written
    }
    #[instrument(skip(pool))]
    async fn get_sqlite_cnt(pool: Pool) -> Option<u64> {
        let conn = pool.get().await.unwrap();
        conn.interact(|conn| {
            match conn.query_row_and_then(
                "SELECT count(id) FROM saved",
                [],
                |row| row.get(0),
            ) {
                Err(e) => {
                    error!("{}",e);
                    None
                }
                Ok(cnt) => {
                    Some(cnt)
                }
            }
        }).await.unwrap()
    }

    // do some necessary clean up when mongodb connection is restored
    #[instrument(skip(self))]
    pub fn clean_local(&self) -> Option<JoinHandle<()>> {
        if !self.dirty.load(SeqCst) {
            // not dirty
            return None;
        }
        if self.cleaning.load(SeqCst) {
            // still cleaning
            return None;
        }
        self.cleaning.store(true, SeqCst);

        let database = self.database.clone();
        let cleaning = self.cleaning.clone();
        let dirty = self.dirty.clone();
        let pool = self.sqlite_pool.clone();

        let join_handle = tokio::spawn(async move {
            let total_cnt = MongodbSaver::get_sqlite_cnt(pool.clone()).await.unwrap_or(0);
            if total_cnt == 0 {
                return;
            }
            for _ in 0..=(total_cnt / 100) {
                MongodbSaver::pop_local(pool.clone(), database.clone()).await;
                let total_cnt = MongodbSaver::get_sqlite_cnt(pool.clone()).await;
                if let Some(0) = total_cnt {
                    info!("dirty cleaned up");
                    dirty.store(false, SeqCst);
                    break;
                }
            }
            cleaning.store(false, SeqCst);
        });
        Some(join_handle)
    }

    #[instrument(skip_all)]
    pub async fn pop_local(pool: Pool, database: Database) {
        info!("start to pop local");
        let conn = pool.get().await.unwrap();
        let batch_data = conn.interact(|conn| {
            let mut statement = match conn.prepare("SELECT id, collection_name, data FROM saved limit 100") {
                Ok(cursor) => {
                    cursor
                }
                Err(e) => {
                    error!("{}", e);
                    return vec![];
                }
            };
            let cursor = statement
                .query_map([], |row| {
                    Ok(RowData {
                        id: row.get(0)?,
                        collection_name: row.get(1)?,
                        data: row.get(2)?,
                    })
                })
                .unwrap()
                .filter_map(|value| match value {
                    Ok(v) => { Some(v) }
                    Err(e) => {
                        error!("{}", e);
                        None
                    }
                }).collect::<Vec<_>>();
            cursor
        }).await.unwrap();
        info!("start to save {} records into mongodb",  batch_data.len());
        for row_data in batch_data.into_iter() {
            let data = row_data.data;
            let collection_name = row_data.collection_name;
            let result = serde_json::from_str(data.as_str());
            let document = result.unwrap();
            let collection = database.collection(collection_name.as_str());
            let mongodb_write_result = MongodbSaver::save_collection_inner(collection, &document).await;
            match mongodb_write_result {
                Ok(_) => {
                    // remove sqlite record
                    if let Err(e) = conn.interact(move |conn| {
                        match conn.execute(
                            "delete from saved where id=?1;",
                            [row_data.id],
                        ) {
                            Ok(_) => {
                                // info!("deleted");
                            }
                            Err(e) => {
                                error!("{}", e);
                            }
                        }
                    }).await {
                        error!("{}",e);
                    }
                }
                Err(e) => {
                    // something went wrong
                    error!("failed to save into mongodb {}",e);
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn vacuum_local(pool: Pool, _database: Database) {
        info!("start to vacuum local");
        let conn = pool.get().await.unwrap();
        let _ = conn.interact(|conn| {
            conn.execute_batch("VACUUM;")
        }).await;
    }
}

#[cfg(test)]
mod test {
    use std::env;

    use mongodb::bson::doc;
    use qrt_log_utils::init_logger;
    use serde::Serialize;

    use crate::mongodb_saver::MongodbSaver;

    #[derive(Serialize)]
    struct TestData {
        num: i32,
    }

    #[tokio::test]
    async fn test_sqlite() {
        init_logger("msaver", None);
        let result = mongodb::bson::to_bson(&TestData { num: 1 }).unwrap();
        let now = chrono::Local::now();
        let document = doc! {"time":now, "data":&result};

        let saver_db_str = env::var("MongoDbSaverStr").expect("need saver db str");
        let mongodb_saver = MongodbSaver::init(saver_db_str.as_str()).await;
        // tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(MongodbSaver::write_local_inner(mongodb_saver.sqlite_pool.clone(), "aaa", &document).await);
        let option = mongodb_saver.clean_local();
        assert!(option.is_some());
        let join_handle = option.unwrap();
        assert!(join_handle.await.is_ok());
        // MongodbSaver::pop_local(mongodb_saver.sqlite_pool.clone(), mongodb_saver.database.clone()).await;
    }

    #[tokio::test]
    async fn test_bunk() {
        let result = mongodb::bson::to_bson(&TestData { num: 2 }).unwrap();
        let now = chrono::Local::now();
        let document = doc! {"time":now, "data":&result};

        let saver_db_str = env::var("MongoDbSaverStr").expect("need saver db str");
        let mongodb_saver = MongodbSaver::init(saver_db_str.as_str()).await;
        let result = mongodb_saver.save_collection_batch("aaa", &[document.clone()]).await;
        dbg!(&result);
        let result = mongodb_saver.save_collection_batch("aaa", &[document]).await;
        dbg!(&result);
        // tokio::time::sleep(Duration::from_secs(3600)).await;
        // mongodb_saver.write_local("aaa", &document).await;
        // mongodb_saver.pop_local().await;
    }
}