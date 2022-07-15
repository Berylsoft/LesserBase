use crate::{prelude::*, model::*};
use mongodb::{Client, Database, Collection, bson};
use bson::{Bson, Document as BsonDocument, bson, doc as bson_doc, Binary as BsonBinary};

// region: util

fn hash_to_bson_bin(hash: Hash) -> BsonBinary {
    BsonBinary { subtype: bson::spec::BinarySubtype::Generic, bytes: hash.to_vec() }
}

fn bson_bin_to_hash(raw: BsonBinary) -> Hash {
    let BsonBinary { bytes, subtype } = raw;
    debug_assert_eq!(subtype, bson::spec::BinarySubtype::Generic);
    bytes.try_into().unwrap()
}

fn bson_to_hash(bson: Bson) -> anyhow::Result<Hash> {
    if let Bson::Binary(raw) = bson { Ok(bson_bin_to_hash(raw)) } else { Err(anyhow::anyhow!("bson_to_bin failed")) }
}

fn bson_to_doc(bson: Bson) -> anyhow::Result<BsonDocument> {
    if let Bson::Document(doc) = bson { Ok(doc) } else { Err(anyhow::anyhow!("bson_to_doc failed")) }
}

fn bson_to_string(bson: Bson) -> anyhow::Result<String> {
    if let Bson::String(string) = bson { Ok(string) } else { Err(anyhow::anyhow!("bson_to_string failed")) }
}

// endregion

// region: boilerplate code for serializing convert

type DStateMap = HashMap<String, BsonBinary>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DState {
    data: DStateMap,
    page: DStateMap,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Content {
    #[serde(with = "serde_bytes")]
    _id: Hash,
    content: Bson,
}

type ContentCollection = Collection<Content>;
type LooseTypedCollection = Collection<BsonDocument>;

pub struct Db {
    conn: Client,
    db_vcs: Database,
    db_latest: Database,
    coll_vcs_objects_data: ContentCollection,
    coll_vcs_objects_page: ContentCollection,
    coll_vcs_commits: ContentCollection,
    coll_vcs_refs: LooseTypedCollection,
    coll_vcs_states: LooseTypedCollection,
    coll_latest_data: LooseTypedCollection,
    coll_latest_page: LooseTypedCollection,
}

fn branch_query(branch: &Branch) -> BsonDocument {
    bson_doc! { "_id": &branch.to_string() }
}

fn hash_query(hash: Hash) -> BsonDocument {
    bson_doc! { "_id": hash_to_bson_bin(hash) }
}

fn path_query(path: &str) -> BsonDocument {
    bson_doc! { "_id": path }
}

fn with_path_id(mut doc: BsonDocument, path: &str) -> BsonDocument {
    doc.insert("_id", path);
    doc
}

fn with_hash_id(mut doc: BsonDocument, hash: Hash) -> BsonDocument {
    doc.insert("_id", hash_to_bson_bin(hash));
    doc
}

fn without_id(mut doc: BsonDocument) -> BsonDocument {
    // [opt assert] id == result.get("_id").unwrap()
    doc.remove("_id");
    doc
}

async fn id_exists(coll: &LooseTypedCollection, id: Bson) -> anyhow::Result<bool> {
    let count = coll.count_documents(bson_doc! { "_id": id }, None).await?;
    Ok(if count == 0 { false } else if count == 1 { true } else { panic!() })
}

async fn write_content(coll: &ContentCollection, hash: Hash, content: Bson) -> anyhow::Result<()> {
    let _ = coll.insert_one(Content { _id: hash, content }, None).await?;
    // [opt assert] id == result.inserted_id
    Ok(())
}

async fn read_content(coll: &ContentCollection, hash: Hash) -> anyhow::Result<Bson> {
    let Content { _id, content } = coll.find_one(hash_query(hash), None).await?.expect("not found");
    Ok(content)
}

async fn write_latest(coll: &LooseTypedCollection, path: String, content: BsonDocument) -> anyhow::Result<()> {
    if id_exists(&coll, bson!(&path)).await? {
        let _ = coll.replace_one(path_query(&path), content, None).await?;
        // [opt assert] id == result.upserted_id.unwrap()
    } else {
        let _ = coll.insert_one(with_path_id(content, &path), None).await?;
        // [opt assert] id == result.inserted_id
    }
    Ok(())
}

async fn delete_latest(coll: &LooseTypedCollection, path: String) -> anyhow::Result<()> {
    let result = coll.delete_one(path_query(&path), None).await?;
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
        let _ = coll.insert_one(doc, None).await?;
        // [opt assert] id == result.inserted_id
        Ok(())
    }

    pub async fn update_ref(&self, branch: &Branch, hash: Hash) -> anyhow::Result<()> {
        let coll = &self.coll_vcs_refs;
        let update = bson_doc! {
            "$push": {
                "hashes": hash_to_bson_bin(hash),
            },
        };
        let result = coll.update_one(branch_query(branch), update, None).await?;
        assert_eq!(1, result.matched_count);
        assert_eq!(1, result.modified_count);
        Ok(())
    }

    pub async fn get_ref(&self, branch: &Branch) -> anyhow::Result<Hash> {
        let coll = &self.coll_vcs_refs;
        let result = coll.find_one(branch_query(branch), None).await?.expect("not found");
        // [opt assert] id == result.get("_id").unwrap()
        Ok(bson_to_hash(result.get_array("hashes")?.last().unwrap().clone())?)
    }

    #[inline]
    pub async fn add_page_object(&self, hash: Hash, content: String) -> anyhow::Result<()> {
        write_content(&self.coll_vcs_objects_page, hash, content.into()).await
    }

    #[inline]
    pub async fn get_page_object(&self, hash: Hash) -> anyhow::Result<String> {
        bson_to_string(read_content(&self.coll_vcs_objects_page, hash).await?)
    }

    #[inline]
    pub async fn add_data_object(&self, hash: Hash, content: Json) -> anyhow::Result<()> {
        let bson = Bson::try_from(content)?;
        assert!(matches!(bson, Bson::Document(_)));
        write_content(&self.coll_vcs_objects_data, hash, bson).await
    }

    #[inline]
    pub async fn get_data_object(&self, hash: Hash) -> anyhow::Result<Json> {
        let bson = read_content(&self.coll_vcs_objects_data, hash).await?;
        assert!(matches!(bson, Bson::Document(_)));
        Ok(bson.try_into()?)
    }

    #[inline]
    pub async fn add_commit(&self, hash: Hash, content: &Commit) -> anyhow::Result<()> {
        write_content(&self.coll_vcs_commits, hash, bson::to_bson(content)?).await
    }

    #[inline]
    pub async fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
        Ok(bson::from_bson(read_content(&self.coll_vcs_commits, hash).await?)?)
    }

    #[inline]
    pub async fn update_page(&self, path: String, content: String) -> anyhow::Result<()> {
        write_latest(&self.coll_latest_page, path, bson_doc! { "content": content }).await
    }

    #[inline]
    pub async fn update_data(&self, path: String, content: Json) -> anyhow::Result<()> {
        let bson = Bson::try_from(content)?;
        assert!(matches!(bson, Bson::Document(_)));
        write_latest(&self.coll_latest_data, path, bson_to_doc(bson)?).await
    }

    #[inline]
    pub async fn remove_page(&self, path: String) -> anyhow::Result<()> {
        delete_latest(&self.coll_latest_page, path).await
    }

    #[inline]
    pub async fn remove_data(&self, path: String) -> anyhow::Result<()> {
        delete_latest(&self.coll_latest_data, path).await
    }

    pub async fn add_state(&self, hash: Hash, state: State) -> anyhow::Result<()> {
        let coll = &self.coll_vcs_states;
        let doc = with_hash_id(bson::to_document(&DState::from(state))?, hash);
        let _ = coll.insert_one(doc, None).await?;
        // [opt assert] id == result.inserted_id
        Ok(())
    }

    pub async fn get_state(&self, hash: Hash) -> anyhow::Result<State> {
        let coll = &self.coll_vcs_states;
        let result = coll.find_one(hash_query(hash), None).await?.expect("not found");
        let doc: DState = bson::from_document(without_id(result))?;
        Ok(State::from(doc))
    }
}
