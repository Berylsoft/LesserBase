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
#[serde(tag = "type", content = "inner")]
pub enum CommandInner {
    Commit(CCommit),
    CreateCommonBranch(CCreateCommonBranch),
    MergeCommonBranchToMain(CMergeCommonBranchToMain),
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
    pub kind: RevKind,
    pub object_kind: ObjectKind,
    pub path: String,
    pub content: Option<Json>,
}

#[derive(Debug, Deserialize)]
pub struct CCreateCommonBranch {
    pub prev: String,
}

#[derive(Debug, Deserialize)]
pub struct CMergeCommonBranchToMain {
    pub branch: CommonBranch,
    pub comment: String,
}
