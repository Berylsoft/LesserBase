use crate::prelude::*;
use num_enum::{TryFromPrimitive, IntoPrimitive};

const OBJECT_KIND_DATA_STR: &'static str = "data";
const OBJECT_KIND_PAGE_STR: &'static str = "page";

#[derive(Debug, Clone)]
pub struct Commit {
    pub prev: Hash,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub merge: Option<Branch>,
    pub rev: Vec<Rev>,
}

#[derive(Debug, Clone)]
pub struct Rev {
    pub kind: RevKind,
    pub hash: Hash,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug, Clone, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum ObjectKind {
    Data,
    Page,
}

impl ObjectKind {
    pub fn to_sign(&self) -> &'static str {
        match self {
            ObjectKind::Data => OBJECT_KIND_DATA_STR,
            ObjectKind::Page => OBJECT_KIND_PAGE_STR,
        }
    }
}

#[derive(Debug, Clone, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum RevKind {
    Update,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Branch {
    Main,
    Common(CommonBranch),
}

pub use Branch::Main;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonBranch {
    pub ts: u64,
    pub author: String,
}

impl Branch {
    pub fn to_string(&self) -> String {
        match self {
            Branch::Main => "main".to_owned(),
            Branch::Common(CommonBranch { ts, author }) => format!("{}-{}", ts, author),
        }
    }
}

pub type StateMap = HashMap<String, Hash>;

#[derive(Debug, Clone)]
pub struct State {
    pub commit: Hash,
    pub data: StateMap,
    pub page: StateMap,
}

// region: boilerplate code for serializing convert

#[derive(Serialize, Deserialize)]
pub struct CommitDocument {
    pub prev: BsonBinary,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub merge: Option<Branch>,
    pub rev: Vec<RevDocument>,
}

#[derive(Serialize, Deserialize)]
pub struct RevDocument {
    pub kind: u8,
    pub hash: BsonBinary,
    pub object_kind: u8,
    pub path: String,
}

impl From<Commit> for CommitDocument {
    fn from(commit: Commit) -> CommitDocument {
        CommitDocument {
            prev: hash_to_bson_bin(commit.prev),
            ts: commit.ts,
            author: commit.author,
            comment: commit.comment,
            merge: commit.merge,
            rev: commit.rev.into_iter().map(|r| RevDocument {
                kind: r.kind.into(),
                hash: hash_to_bson_bin(r.hash),
                object_kind: r.object_kind.into(),
                path: r.path,
            }).collect(),
        }
    }
}

impl From<CommitDocument> for Commit {
    fn from(doc: CommitDocument) -> Commit {
        Commit {
            prev: bson_bin_to_hash(doc.prev),
            ts: doc.ts,
            author: doc.author,
            comment: doc.comment,
            merge: doc.merge,
            rev: doc.rev.into_iter().map(|r| Rev {
                kind: RevKind::try_from(r.kind).unwrap(),
                hash: bson_bin_to_hash(r.hash),
                object_kind: ObjectKind::try_from(r.object_kind).unwrap(),
                path: r.path,
            }).collect(),
        }
    }
}

pub type StateDocumentMap = HashMap<String, BsonBinary>;

#[derive(Serialize, Deserialize)]
pub struct StateDocument {
    pub _id: BsonBinary,
    pub data: StateDocumentMap,
    pub page: StateDocumentMap,
}

fn state_map_to_doc(map: StateMap) -> StateDocumentMap {
    let mut map2 = HashMap::new();
    for (path, hash) in map.into_iter() {
        let result = map2.insert(path, hash_to_bson_bin(hash));
        debug_assert!(matches!(result, None));
    }
    map2
}

fn state_map_from_doc(map: StateDocumentMap) -> StateMap {
    let mut map2 = HashMap::new();
    for (path, hash) in map.into_iter() {
        let result = map2.insert(path, bson_bin_to_hash(hash));
        debug_assert!(matches!(result, None));
    }
    map2
}

impl From<State> for StateDocument {
    fn from(state: State) -> StateDocument {
        let State { commit, data, page } = state;
        StateDocument { _id: hash_to_bson_bin(commit), data: state_map_to_doc(data), page: state_map_to_doc(page) }
    }
}

impl From<StateDocument> for State {
    fn from(state: StateDocument) -> State {
        let StateDocument { _id, data, page } = state;
        State { commit: bson_bin_to_hash(_id), data: state_map_from_doc(data), page: state_map_from_doc(page) }
    }
}

// endregion
