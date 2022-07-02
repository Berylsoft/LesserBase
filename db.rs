use crate::{prelude::*, model::*};
use mongodb::{Client, Database, Collection};

// region: boilerplate code for serializing convert

type DStateMap = HashMap<String, BsonBinary>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DState {
    pub data: DStateMap,
    pub page: DStateMap,
}

fn state_map_to_doc(map: StateMap) -> DStateMap {
    map.into_iter().map(|(path, hash)| (path, hash_to_bson_bin(hash))).collect()
}

fn state_map_from_doc(map: DStateMap) -> StateMap {
    map.into_iter().map(|(path, hash)| (path, bson_bin_to_hash(hash))).collect()
}

impl From<State> for DState {
    fn from(state: State) -> DState {
        let State { data, page } = state;
        DState { data: state_map_to_doc(data), page: state_map_to_doc(page) }
    }
}

impl From<DState> for State {
    fn from(state: DState) -> State {
        let DState { data, page } = state;
        State { data: state_map_from_doc(data), page: state_map_from_doc(page) }
    }
}

// endregion

type LooseTypedCollection = Collection<BsonDocument>;

pub struct Db {
    conn: Client,
    db_vcs: Database,
    db_latest: Database,
    coll_vcs_objects_data: LooseTypedCollection,
    coll_vcs_objects_page: LooseTypedCollection,
    coll_vcs_commits: LooseTypedCollection,
    coll_vcs_refs: LooseTypedCollection,
    coll_vcs_states: LooseTypedCollection,
    coll_latest_data: LooseTypedCollection,
    coll_latest_page: LooseTypedCollection,
}

async fn id_exists(coll: &LooseTypedCollection, id: Bson) -> anyhow::Result<bool> {
    let query = bson_doc! { "_id": id };
    let count = coll.count_documents(query.clone(), None).await?;
    Ok(if count == 0 {
        false
    } else if count == 1 {
        true
    } else {
        panic!()
    })
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
    let query = bson_doc! { "_id": hash_to_bson_bin(hash) };
    let result = coll.find_one(query, None).await?.expect("not found");
    debug_assert_eq!(result.get_binary_generic("_id")?, hash.as_slice());
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

async fn insert_latest_by_path(coll: &LooseTypedCollection, path: String, mut content: BsonDocument) -> anyhow::Result<()> {
    if id_exists(&coll, bson!(&path)).await? {
        let _ = content.insert("id", &path);
        let result = coll.insert_one(content, None).await?;
        debug_assert_eq!(&path, result.inserted_id.as_str().unwrap());
    } else {
        let result = coll.replace_one(bson_doc! { "_id": &path }, content, None).await?;
        debug_assert_eq!(&path, result.upserted_id.unwrap().as_str().unwrap());
    }
    Ok(())
}

async fn delete_latest_by_path(coll: &LooseTypedCollection, path: String) -> anyhow::Result<()> {
    let query = bson_doc! { "_id": &path };
    let result = coll.delete_one(query, None).await?;
    assert_eq!(result.deleted_count, 1);
    Ok(())
}

impl Db {
    pub async fn new(uri: &str) -> anyhow::Result<Db> {
        let conn = Client::with_uri_str(uri).await?;
        let db_vcs = conn.database("vcs");
        let db_latest = conn.database("latest");

        Ok(Db {
            coll_vcs_objects_data: db_vcs.collection("objects-data"),
            coll_vcs_objects_page: db_vcs.collection("objects-page"),
            coll_vcs_commits: db_vcs.collection("commits"),
            coll_vcs_refs: db_vcs.collection("refs"),
            coll_vcs_states: db_vcs.collection("states"),
            coll_latest_data: db_latest.collection("data"),
            coll_latest_page: db_latest.collection("page"),
            conn, db_vcs, db_latest,
        })
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        if !id_exists(&self.coll_vcs_refs, Branch::Main.to_string().into()).await? {
            self.create_ref(&Branch::Main, EMPTY_HASH).await?;
            self.add_state(EMPTY_HASH, State::empty()).await?;
        }
        Ok(())
    }

    pub async fn create_ref(&self, branch: &Branch, hash: Hash) -> anyhow::Result<()> {
        let coll = &self.coll_vcs_refs;
        let branch = branch.to_string();
        let doc = bson_doc! {
            "_id": &branch,
            "hashes": [
                hash_to_bson_bin(hash),
            ],
        };
        let result = coll.insert_one(doc, None).await?;
        debug_assert_eq!(branch, result.inserted_id.as_str().unwrap());
        Ok(())
    }

    pub async fn update_ref(&self, branch: &Branch, hash: Hash) -> anyhow::Result<()> {
        let coll = &self.coll_vcs_refs;
        let branch = branch.to_string();
        let query = bson_doc! { "_id": &branch };
        let update = bson_doc! {
            "$push": {
                "hashes": hash_to_bson_bin(hash),
            },
        };
        let result = coll.update_one(query, update, None).await?;
        debug_assert_eq!(1, result.matched_count);
        debug_assert_eq!(1, result.modified_count);
        Ok(())
    }

    pub async fn get_ref(&self, branch: &Branch) -> anyhow::Result<Hash> {
        let coll = &self.coll_vcs_refs;
        let branch = branch.to_string();
        let query = bson_doc! { "_id": &branch };
        let result = coll.find_one(query, None).await?.expect("not found");
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
    pub async fn add_commit(&self, hash: Hash, content: &Commit) -> anyhow::Result<()> {
        insert_content_by_hash_id(&self.coll_vcs_commits, hash, bson::to_bson(content)?).await
    }

    #[inline]
    pub async fn get_commit(&self, hash: Hash) -> anyhow::Result<BsonDocument> {
        get_doc_content_by_hash_id(&self.coll_vcs_commits, hash).await
    }

    #[inline]
    pub async fn update_page(&self, path: String, content: String) -> anyhow::Result<()> {
        insert_latest_by_path(&self.coll_latest_page, path, bson_doc! { "content": content }).await
    }

    #[inline]
    pub async fn update_data(&self, path: String, content: BsonDocument) -> anyhow::Result<()> {
        insert_latest_by_path(&self.coll_latest_data, path, content).await
    }

    #[inline]
    pub async fn remove_page(&self, path: String) -> anyhow::Result<()> {
        delete_latest_by_path(&self.coll_latest_page, path).await
    }

    #[inline]
    pub async fn remove_data(&self, path: String) -> anyhow::Result<()> {
        delete_latest_by_path(&self.coll_latest_data, path).await
    }

    pub async fn add_state(&self, hash: Hash, state: State) -> anyhow::Result<()> {
        let coll = &self.coll_vcs_states;
        let mut doc = bson::to_document(&DState::from(state))?;
        doc.insert("_id", hash_to_bson_bin(hash));
        let result = coll.insert_one(doc, None).await?;
        debug_assert_eq!(hash, bson_to_hash(result.inserted_id)?);
        Ok(())
    }

    pub async fn get_state(&self, hash: Hash) -> anyhow::Result<State> {
        let coll = &self.coll_vcs_states;
        let query = bson_doc! { "_id": hash_to_bson_bin(hash) };
        let mut result = coll.find_one(query, None).await?.expect("not found");
        debug_assert_eq!(result.get_binary_generic("_id")?, hash.as_slice());
        result.remove("_id");
        let doc: DState = bson::from_document(result)?;
        Ok(State::from(doc))
    }
}
