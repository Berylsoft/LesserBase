use crate::{prelude::*, model::*, command::*, fs::Repo, db::Db};

async fn calc_state(hash: Hash, rev: Vec<Rev>, prev_state: State) -> State {
    let State { mut data, mut page, .. } = prev_state;
    for r in rev.into_iter() {
        match r.kind {
            RevKind::Update => match r.object_kind {
                ObjectKind::Data => { let _ = data.insert(r.path, r.hash); },
                ObjectKind::Page => { let _ = page.insert(r.path, r.hash); },
            },
            RevKind::Remove => match r.object_kind {
                ObjectKind::Data => { let _ = data.remove(&r.path); },
                ObjectKind::Page => { let _ = page.remove(&r.path); },
            },
        }
    }
    State { commit: hash, data, page }
}

async fn get_prev_state(db: &Db, hash: Hash) -> anyhow::Result<State> {
    Ok(if hash != Hash::from(EMPTY_HASH) {
        db.get_state(hash).await?
    } else {
        State { commit: Hash::from(EMPTY_HASH), data: HashMap::new(), page: HashMap::new() }
    })
}

fn proc_commit(commit: Commit) -> anyhow::Result<(Hash, Vec<u8>, CommitDocument)> {
    let commit_doc = CommitDocument::from(commit);
    let blob = bson::to_vec(&commit_doc)?;
    Ok((hash_all(&blob), blob, commit_doc))
}

pub struct Context {
    repo: Repo,
    db: Db,
}

impl Context {
    pub async fn init(path: PathBuf, db_uri: &str) -> anyhow::Result<Context> {
        let repo = Repo::new(path)?;
        repo.init()?;
        let db = Db::new(db_uri).await?;
        Ok(Context { repo, db })
    }

    pub async fn exec(&self, cmd: Command) -> anyhow::Result<()> {
        let Context { repo, db } = self;
        let Command { ts, author, inner } = cmd;
        match inner {
            CommandInner::Commit(CCommit { comment, branch, prev, rev: _rev }) => {
                // TODO prem check
                let prev = Hash::from_hex(prev)?;
                // TODO use State
                assert_eq!(repo.get_ref(&branch)?, prev);
                let mut rev = Vec::new();
                for CRev { kind, object_kind, path, content } in _rev {
                    let kind = kind.try_into()?;
                    let object_kind = object_kind.try_into()?;
                    let hash = match object_kind {
                        ObjectKind::Data => {
                            // TODO schema check
                            let content = bson_to_doc(Bson::try_from(content)?)?;
                            let blob = bson::to_vec(&content)?;
                            let hash = hash_all(&blob);
                            repo.add_data_object(hash, &blob)?;
                            db.add_data_object(hash, content).await?;
                            hash
                        },
                        ObjectKind::Page => {
                            let content = json_to_string(content)?;
                            let blob = content.as_bytes();
                            let hash = hash_all(blob);
                            repo.add_page_object(hash, blob)?;
                            db.add_page_object(hash, content).await?;
                            hash
                        },
                    };
                    rev.push(Rev { kind, hash, object_kind, path });
                }
                let (hash, blob, content) = proc_commit(Commit { prev, ts, author, comment, merge: None, rev })?;
                repo.add_commit(hash, &blob)?;
                repo.update_ref(&branch, hash)?;
                db.add_commit(hash, content).await?;
                db.update_ref(&branch, hash).await?;
            },
            CommandInner::CreateCommonBranch(CCreateCommonBranch { prev }) => {
                let prev = Hash::from_hex(prev)?;
                let branch = Branch::Common(CommonBranch { ts, author });
                repo.create_ref(&branch, prev)?;
                db.create_ref(&branch, prev).await?;
            },
            CommandInner::MergeCommonBranchToMain(CMergeCommonBranchToMain { branch, comment }) => {
                // TODO prem check
                let branch = Branch::Common(branch);
                let (hash, blob, content) = proc_commit(if repo.get_ref(&Main)? == repo.get_root_ref(&branch)? {
                    // fast-forward
                    Commit { prev: repo.get_ref(&branch)?, ts, author, comment, merge: Some(branch.clone()), rev: Vec::new() }
                } else {
                    // 3-way
                    unimplemented!()
                })?;
                repo.add_commit(hash, &blob)?;
                repo.update_ref(&branch, hash)?;
                repo.update_ref(&Main, hash)?;
                db.add_commit(hash, content).await?;
                db.update_ref(&branch, hash).await?;
                db.update_ref(&Main, hash).await?;
            }
        }
        Ok(())
    }
}
