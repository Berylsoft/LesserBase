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
    coll_vcs_refs: LooseTypedCollection,
    coll_latest_data: LooseTypedCollection,
    coll_latest_page: LooseTypedCollection,
}

async fn insert_content_by_hash_id(coll: &LooseTypedCollection, hash: Hash, content: Bson) -> anyhow::Result<()> {
    let doc = bson_doc! {
        "_id": hash_to_bson_bin(hash),
        "content": content,
    };
    let result = coll.insert_one(doc, None).await?;
    debug_assert_eq!(hash, bson_to_hash(result.inserted_id)?);
    Ok(())
}

async fn get_content_by_hash_id_inner(coll: &LooseTypedCollection, hash: Hash) -> anyhow::Result<BsonDocument> {
    let query = bson_doc! {
        "_id": hash_to_bson_bin(hash),
    };
    let mut cursor = coll.find(query, None).await?;
    let result = cursor.try_next().await?.expect("not found");
    debug_assert!(matches!(cursor.try_next().await?, None));
    debug_assert_eq!(result.get_binary_generic("_id")?, hash.as_bytes());
    Ok(result)
}

#[inline]
async fn get_str_content_by_hash_id(coll: &LooseTypedCollection, hash: Hash) -> anyhow::Result<String> {
    Ok(get_content_by_hash_id_inner(coll, hash).await?.get_str("content")?.to_owned())
}

#[inline]
async fn get_doc_content_by_hash_id(coll: &LooseTypedCollection, hash: Hash) -> anyhow::Result<BsonDocument> {
    Ok(get_content_by_hash_id_inner(coll, hash).await?.get_document("content")?.to_owned())
}

impl View {
    pub async fn new(uri: &str) -> anyhow::Result<View> {
        let conn = Client::with_uri_str(uri).await?;
        let db_vcs = conn.database("vcs");
        let db_latest = conn.database("latest");

        Ok(View {
            coll_vcs_objects_data: db_vcs.collection("objects-data"),
            coll_vcs_objects_page: db_vcs.collection("objects-page"),
            coll_vcs_commits: db_vcs.collection("commits"),
            coll_vcs_refs: db_vcs.collection("refs"),
            coll_latest_data: db_latest.collection("data"),
            coll_latest_page: db_latest.collection("page"),
            conn, db_vcs, db_latest,
        })
    }

    pub async fn create_ref(&self, branch: &str, hash: Hash) -> anyhow::Result<()> {
        let doc = bson_doc! {
            "_id": branch,
            "hashes": [
                hash_to_bson_bin(hash),
            ],
        };
        let result = self.coll_vcs_refs.insert_one(doc, None).await?;
        debug_assert_eq!(branch, result.inserted_id.as_str().unwrap());
        Ok(())
    }

    pub async fn update_ref(&self, branch: &str, hash: Hash) -> anyhow::Result<()> {
        let query = bson_doc! {
            "_id": branch,
        };
        let update = bson_doc! {
            "$push": {
                "hashes": hash_to_bson_bin(hash),
            },
        };
        let result = self.coll_vcs_refs.update_one(query, update, None).await?;
        debug_assert_eq!(branch, result.upserted_id.unwrap().as_str().unwrap());
        debug_assert_eq!(1, result.matched_count);
        debug_assert_eq!(1, result.modified_count);
        Ok(())
    }

    pub async fn get_ref(&self, branch: &str) -> anyhow::Result<Hash> {
        let query = bson_doc! {
            "_id": branch,
        };
        let mut cursor = self.coll_vcs_refs.find(query, None).await?;
        let result = cursor.try_next().await?.expect("not found");
        debug_assert!(matches!(cursor.try_next().await?, None));
        debug_assert_eq!(result.get_str("_id")?, branch);
        Ok(bson_to_hash(result.get_array("hashes")?.last().unwrap().clone())?)
    }

    #[inline]
    pub async fn add_page_object(&self, hash: Hash, content: String) -> anyhow::Result<()> {
        insert_content_by_hash_id(&self.coll_vcs_objects_page, hash, content.into()).await
    }

    #[inline]
    pub async fn get_page_object(&self, hash: Hash) -> anyhow::Result<String> {
        get_str_content_by_hash_id(&self.coll_vcs_objects_page, hash).await
    }

    #[inline]
    pub async fn add_data_object(&self, hash: Hash, content: BsonDocument) -> anyhow::Result<()> {
        insert_content_by_hash_id(&self.coll_vcs_objects_data, hash, content.into()).await
    }

    #[inline]
    pub async fn get_data_object(&self, hash: Hash) -> anyhow::Result<BsonDocument> {
        get_doc_content_by_hash_id(&self.coll_vcs_objects_data, hash).await
    }

    #[inline]
    pub async fn add_commit(&self, hash: Hash, content: BsonDocument) -> anyhow::Result<()> {
        insert_content_by_hash_id(&self.coll_vcs_commits, hash, content.into()).await
    }

    #[inline]
    pub async fn get_commit(&self, hash: Hash) -> anyhow::Result<BsonDocument> {
        get_doc_content_by_hash_id(&self.coll_vcs_commits, hash).await
    }
}