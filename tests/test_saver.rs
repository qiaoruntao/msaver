#[cfg(test)]
mod test {
    use std::env;

    use msaver::mongodb_saver::MongodbSaver;

    #[tokio::test]
    pub async fn write_local() {
        let db_str = env::var("MongoDbSaverStr").expect("need task db str");
        let saver = MongodbSaver::init(&db_str).await;
        assert!(saver.write_local("test_aaa", &111).await);
        assert!(saver.write_local("test_aaa", &111).await);
    }

    #[tokio::test]
    pub async fn clean_local() {
        let db_str = env::var("MongoDbSaverStr").expect("need task db str");
        let saver = MongodbSaver::init(&db_str).await;
        let handler = saver.clean_local().unwrap();
        handler.await;
    }
}