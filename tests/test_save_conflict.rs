#[cfg(test)]
mod test {
    use mongodb::bson::{doc, Document};
    use mongodb::options::IndexOptions;
    use mongodb::IndexModel;
    use std::env;

    use msaver::mongodb_saver::MongodbSaver;

    #[tokio::test]
    pub async fn test_save_conflict() {
        let db_str = env::var("MongoDbSaverStr").expect("need task db str");
        let collection_name = "test_aaa";
        let saver = MongodbSaver::init(&db_str).await;
        let collection = saver.get_collection::<Document>(collection_name);
        collection
            .delete_many(doc! {})
            .await
            .expect("failed to clean collection");
        let _ = collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"data.a":1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await;
        assert!(saver
            .save_collection(collection_name, &doc! {"a":1})
            .await
            .is_ok());
        assert!(!saver
            .save_collection(collection_name, &doc! {"a":1})
            .await
            .is_err());
        let result = saver
            .save_collection_batch(collection_name, &[doc! {"a":1}, doc! {"a":2}])
            .await;
        assert!(result.is_ok());
        let batch_save_result = result.unwrap();
        assert_eq!(batch_save_result.failed_index_list.len(), 0);
    }
}
