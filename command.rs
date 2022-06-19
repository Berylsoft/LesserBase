use crate::{prelude::*, commit::*, local::Repo, view::View};

pub trait Request {
    type Response;
}

#[derive(Debug, Deserialize)]
pub struct Command {
    author: String,
    ts: u64,
    inner: CommandInner,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "inner")]
pub enum CommandInner {
    Commit(CmdCommit)
}

#[derive(Debug, Deserialize)]
pub struct CmdCommit {
    comment: String,
    branch: String,
    prev: String,
    rev: Vec<CmdRev>,
}

#[derive(Debug, Deserialize)]
pub struct CmdRev {
    kind: u8,
    object_kind: u8,
    path: String,
    content: Json,
}

pub async fn proc(cmd: Command, repo: &Repo, view: &View) -> anyhow::Result<()> {
    let Command { author, ts, inner } = cmd;
    match inner {
        CommandInner::Commit(CmdCommit { comment, branch, prev, rev }) => {
            let prev = Hash::from_hex(prev)?;
            // TODO use State
            assert_eq!(repo.get_ref(&branch)?, prev);
            let mut _rev = Vec::new();
            for CmdRev { kind, object_kind, path, content } in rev {
                let kind = kind.try_into()?;
                let object_kind = object_kind.try_into()?;
                let hash = match object_kind {
                    ObjectKind::Data => {
                        // TODO schema check
                        let content = bson_to_doc(Bson::try_from(content)?)?;
                        let blob = bson::to_vec(&content)?;
                        let hash = hash_all(&blob);
                        repo.add_data_object(hash, &blob)?;
                        println!("{:?}", view.add_data_object(hash, content).await?);
                        hash
                    },
                    ObjectKind::Page => {
                        let content = json_to_string(content)?;
                        let blob = content.as_bytes();
                        let hash = hash_all(blob);
                        repo.add_page_object(hash, blob)?;
                        println!("{:?}", view.add_page_object(hash, content).await?);
                        hash
                    },
                };
                _rev.push(Rev { kind, hash, object_kind, path });
            }
            let commit = Commit { prev, ts, author, comment, rev: _rev };
            let commit_doc = CommitDocument::from(commit);
            let blob = bson::to_vec(&commit_doc)?;
            let hash = hash_all(&blob);
            repo.add_commit(hash, &blob)?;
            println!("{:?}", view.add_commit(hash, bson::to_document(&commit_doc)?).await?);
            repo.update_ref(&branch, hash)?;
        }
    }
    Ok(())
}
