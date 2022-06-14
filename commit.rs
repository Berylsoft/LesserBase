use crate::prelude::*;

#[derive(Debug)]
pub struct Commit {
    pub prev: Hash,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub rev: Vec<Rev>,
}

#[derive(Debug)]
pub struct Rev {
    pub kind: RevKind,
    pub hash: Hash,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug)]
pub enum ObjectKind {
    Data,
    Page,
}

impl ObjectKind {
    pub fn from_digit(digit: u8) -> ObjectKind {
        match digit {
            0 => ObjectKind::Page,
            1 => ObjectKind::Data,
            _ => unreachable!(),
        }
    }

    pub fn to_digit(&self) -> u8 {
        match self {
            ObjectKind::Page => 0,
            ObjectKind::Data => 1,
        }
    }

    pub fn from_sign(sign: char) -> ObjectKind {
        match sign {
            'D' => ObjectKind::Data,
            'P' => ObjectKind::Page,
            _ => unreachable!(),
        }
    }

    pub fn to_sign(&self) -> char {
        match self {
            ObjectKind::Data => 'D',
            ObjectKind::Page => 'P',
        }
    }
}

#[derive(Debug)]
pub enum RevKind {
    Update,
    Remove,
}

impl RevKind {
    pub fn from_digit(digit: u8) -> RevKind {
        match digit {
            0 => RevKind::Update,
            1 => RevKind::Remove,
            _ => unreachable!(),
        }
    }

    pub fn to_digit(&self) -> u8 {
        match self {
            RevKind::Update => 0,
            RevKind::Remove => 1,
        }
    }
}

// region: serde helper

#[derive(Serialize, Deserialize)]
pub struct CommitDocument {
    pub prev: bson::Binary,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub rev: Vec<RevDocument>,
}

#[derive(Debug, Serialize, Deserialize)]
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
            rev: commit.rev.into_iter().map(|r| RevDocument {
                kind: r.kind.to_digit(),
                hash: hash_to_bson_bin(r.hash),
                object_kind: r.object_kind.to_digit(),
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
            rev: doc.rev.into_iter().map(|r| Rev {
                kind: RevKind::from_digit(r.kind),
                hash: bson_bin_to_hash(r.hash),
                object_kind: ObjectKind::from_digit(r.object_kind),
                path: r.path,
            }).collect(),
        }
    }
}

// endregion
