use crate::prelude::*;
use num_enum::{TryFromPrimitive, IntoPrimitive};

const OBJECT_KIND_DATA_STR: &'static str = "data";
const OBJECT_KIND_PAGE_STR: &'static str = "page";

#[derive(Debug)]
pub struct Commit {
    pub prev: Hash,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub merge: Option<Branch>,
    pub rev: Vec<Rev>,
}

#[derive(Debug)]
pub struct Rev {
    pub kind: RevKind,
    pub hash: Hash,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug, TryFromPrimitive, IntoPrimitive)]
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

#[derive(Debug, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum RevKind {
    Update,
    Remove,
}

// region: serde helper

#[derive(Serialize, Deserialize)]
pub struct CommitDocument {
    pub prev: bson::Binary,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub merge: Option<Branch>,
    pub rev: Vec<RevDocument>,
}

#[derive(Serialize, Deserialize)]
pub struct RevDocument {
    pub kind: u8,
    pub hash: bson::Binary,
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

// endregion

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
            Branch::Common(b) => b.to_string(),
        }
    }
}

impl CommonBranch {
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ts, self.author)
    }
}
