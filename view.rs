use crate::prelude::*;
use futures::stream::TryStreamExt;
use mongodb::{Client, Database, Collection};

type LooseTypedCollection = Collection<BsonDocument>;

pub struct View {
    conn: Client,
    db_vcs: Database,
    db_latest: Database,
    coll_vcs_objects_data: LooseTypedCollection,
    coll_vcs_objects_page: LooseTypedCollection,
    coll_vcs_commits: LooseTypedCollection,
    coll_latest_data: LooseTypedCollection,
    coll_latest_page: LooseTypedCollection,
}

async fn get_by_hash_id(coll: &LooseTypedCollection, hash: Hash) -> anyhow::Result<BsonDocument> {
    let query = bson_doc! {
        "_id": hash_to_bson_bin(hash),
    };
    let mut cursor = coll.find(query, None).await?;
    let result = cursor.try_next().await?.expect("not found");
    assert!(matches!(cursor.try_next().await?, None));
    assert_eq!(result.get_binary_generic("_id")?, hash.as_bytes());
    Ok(result)
}

impl View {
    pub async fn new(uri: &str) -> anyhow::Result<View> {
        let conn = Client::with_uri_str(uri).await?;
        let db_vcs = conn.database("vcs");
        let db_latest = conn.database("latest");

        Ok(View {
            coll_vcs_objects_data: db_vcs.collection("objects-data"),
            coll_vcs_objects_page: db_vcs.collection("objects-page"),
            coll_vcs_commits: db_vcs.collection("commit"),
            coll_latest_data: db_latest.collection("data"),
            coll_latest_page: db_latest.collection("page"),
            conn, db_vcs, db_latest,
        })
    }

    pub async fn add_page_object(&self, hash: Hash, content: String) -> anyhow::Result<Bson> {
        let doc = bson_doc! {
            "_id": hash_to_bson_bin(hash),
            "content": content,
        };
        let result = self.coll_vcs_objects_page.insert_one(doc, None).await?;
        Ok(result.inserted_id)
    }

    pub async fn get_page_object(&self, hash: Hash) -> anyhow::Result<String> {
        let result = get_by_hash_id(&self.coll_vcs_objects_page, hash).await?;
        Ok(result.get_str("content")?.to_owned())
    }

    pub async fn add_data_object(&self, hash: Hash, content: BsonDocument) -> anyhow::Result<Bson> {
        let doc = bson_doc! {
            "_id": hash_to_bson_bin(hash),
            "content": content,
        };
        let result = self.coll_vcs_objects_data.insert_one(doc, None).await?;
        Ok(result.inserted_id)
    }

    pub async fn get_data_object(&self, hash: Hash) -> anyhow::Result<BsonDocument> {
        let result = get_by_hash_id(&self.coll_vcs_objects_data, hash).await?;
        Ok(result.get_document("content")?.clone())
    }

    pub async fn add_commit(&self, hash: Hash, content: BsonDocument) -> anyhow::Result<Bson> {
        let doc = bson_doc! {
            "_id": hash_to_bson_bin(hash),
            "content": content,
        };
        let result = self.coll_vcs_commits.insert_one(doc, None).await?;
        Ok(result.inserted_id)
    }

    pub async fn get_commit(&self, hash: Hash) -> anyhow::Result<BsonDocument> {
        let result = get_by_hash_id(&self.coll_vcs_commits, hash).await?;
        Ok(result.get_document("content")?.clone())
    }
}
