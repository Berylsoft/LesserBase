use crate::{prelude::*, commit::*, command::*, local::Repo, state::State, view::View};

pub struct Context {
    repo: Repo,
    state: State,
    view: View,
}

impl Context {
    pub async fn init(path: PathBuf, state_uri: &str, view_uri: &str) -> anyhow::Result<Context> {
        let repo = Repo::new(path)?;
        repo.init()?;
        let state = State::new(&state_uri).await?;
        let view = View::new(&view_uri).await?;
        Ok(Context { repo, state, view })
    }

    pub async fn exec(&mut self, cmd: Command) -> anyhow::Result<()> {
        let Context { repo, state, view } = self;
        let Command { ts, author, inner } = cmd;
        match inner {
            CommandInner::Commit(CCommit { comment, branch, prev, rev }) => {
                // TODO prem check
                let prev = Hash::from_hex(prev)?;
                // TODO use State
                assert_eq!(repo.get_ref(&branch)?, prev);
                let mut _rev = Vec::new();
                for CRev { kind, object_kind, path, content } in rev {
                    let kind = kind.try_into()?;
                    let object_kind = object_kind.try_into()?;
                    let hash = match object_kind {
                        ObjectKind::Data => {
                            // TODO schema check
                            let content = bson_to_doc(Bson::try_from(content)?)?;
                            let blob = bson::to_vec(&content)?;
                            let hash = hash_all(&blob);
                            repo.add_data_object(hash, &blob)?;
                            view.add_data_object(hash, content).await?;
                            hash
                        },
                        ObjectKind::Page => {
                            let content = json_to_string(content)?;
                            let blob = content.as_bytes();
                            let hash = hash_all(blob);
                            repo.add_page_object(hash, blob)?;
                            view.add_page_object(hash, content).await?;
                            hash
                        },
                    };
                    _rev.push(Rev { kind, hash, object_kind, path });
                }
                let commit = Commit { prev, ts, author, comment, merge: None, rev: _rev };
                let commit_doc = CommitDocument::from(commit);
                let blob = bson::to_vec(&commit_doc)?;
                let hash = hash_all(&blob);
                repo.add_commit(hash, &blob)?;
                repo.update_ref(&branch, hash)?;
                view.add_commit(hash, bson::to_document(&commit_doc)?).await?;
                view.update_ref(&branch, hash).await?;
            },
            CommandInner::CreateCommonBranch(CCreateCommonBranch { prev }) => {
                let prev = Hash::from_hex(prev)?;
                let branch = Branch::Common(CommonBranch { ts, author });
                repo.create_ref(&branch, prev)?;
                view.create_ref(&branch, prev).await?;
            },
            CommandInner::MergeCommonBranchToMain(CMergeCommonBranchToMain { branch, comment }) => {
                // TODO prem check
                let branch = Branch::Common(branch);
                let commit = if repo.get_ref(&Main)? == repo.get_root_ref(&branch)? {
                    // fast-forward
                    Commit { prev: repo.get_ref(&branch)?, ts, author, comment, merge: Some(branch.clone()), rev: Vec::new() }
                } else {
                    // 3-way
                    unimplemented!()
                };
                let commit_doc = CommitDocument::from(commit);
                let blob = bson::to_vec(&commit_doc)?;
                let hash = hash_all(&blob);
                repo.add_commit(hash, &blob)?;
                repo.update_ref(&branch, hash)?;
                repo.update_ref(&Main, hash)?;
                view.add_commit(hash, bson::to_document(&commit_doc)?).await?;
                view.update_ref(&branch, hash).await?;
                view.update_ref(&Main, hash).await?;
            }
        }
        Ok(())
    }
}
