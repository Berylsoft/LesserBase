use crate::{prelude::*, model::*};

pub trait Request {
    type Response;
}

#[derive(Debug, Deserialize)]
pub struct Command {
    pub ts: u64,
    pub author: String,
    pub inner: CommandInner,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", content = "inner")]
pub enum CommandInner {
    Commit(CCommit),
    CreateCommonBranch(CCreateCommonBranch),
    MergeBranch(CMergeBranch),
}

#[derive(Debug, Deserialize)]
pub struct CCommit {
    pub comment: String,
    pub branch: Branch,
    pub prev: String,
    pub rev: Vec<CRev>,
}

#[derive(Debug, Deserialize)]
pub struct CRev {
    #[serde(flatten)]
    pub inner: CRevInner,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum CRevInner {
    Update {
        content: Json,
    },
    Remove,
}

#[derive(Debug, Deserialize)]
pub struct CCreateCommonBranch {
    pub prev: String,
}

#[derive(Debug, Deserialize)]
pub struct CMergeBranch {
    pub from: Branch,
    pub to: Branch,
    pub comment: String,
}
