use std::env;
use qrt_log_utils::{init_logger, LoggerConfig};
use tracing::info;
use msaver::mongodb_saver::MongodbSaver;

#[tokio::main]
async fn main() {
    init_logger("msaver", LoggerConfig::default());
    let db_str = env::var("MongoDbSaverStr").expect("need task db str");
    let saver = MongodbSaver::init(&db_str).await;
    let handler = saver.clean_local().unwrap();
    let result = handler.await;
    info!("{:?}", result);
}