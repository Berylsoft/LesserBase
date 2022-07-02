use crate::{prelude::*, model::*, command::*, fs::Repo, db::Db};

impl State {
    pub fn update(&mut self, rev: Vec<Rev>) {
        for Rev { inner, object_kind, path } in rev {
            let dest = match object_kind {
                ObjectKind::Data => &mut self.data,
                ObjectKind::Page => &mut self.page,
            };
            let _ = match inner {
                RevInner::Update { hash } => dest.insert(path, hash),
                RevInner::Remove => dest.remove(&path),
            };
        }
    }
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
        db.init().await?;
        Ok(Context { repo, db })
    }

    pub async fn add_data_object(&self, content: BsonDocument) -> anyhow::Result<Hash> {
        let Context { repo, db } = self;

        // TODO schema check

        let blob = bson::to_vec(&content)?;
        let hash = hash_all(&blob);

        repo.add_data_object(hash, &blob)?;
        db.add_data_object(hash, content).await?;

        Ok(hash)
    }

    pub async fn add_page_object(&self, content: String) -> anyhow::Result<Hash> {
        let Context { repo, db } = self;

        let blob = content.as_bytes();
        let hash = hash_all(blob);

        repo.add_page_object(hash, blob)?;
        db.add_page_object(hash, content).await?;

        Ok(hash)
    }

    pub async fn create_branch(&self, prev: Hash, branch: &Branch) -> anyhow::Result<()> {
        let Context { repo, db } = self;

        repo.create_ref(branch, prev)?;
        db.create_ref(branch, prev).await?;

        Ok(())
    }

    pub async fn commit(&self, commit: Commit, branches: Vec<&Branch>) -> anyhow::Result<()> {
        let Context { repo, db } = self;

        let blob = bson::to_vec(&commit)?;
        let hash = hash_all(&blob);

        repo.add_commit(hash, &blob)?;
        db.add_commit(hash, &commit).await?;

        for branch in branches {
            repo.update_ref(branch, hash)?;
            db.update_ref(branch, hash).await?;
        }

        let mut state = db.get_state(commit.prev).await?;
        state.update(commit.rev);
        db.add_state(hash, state).await?;

        Ok(())
    }

    pub async fn exec(&self, cmd: Command) -> anyhow::Result<()> {
        let Context { repo, .. } = self;
        let Command { ts, author, inner } = cmd;
        match inner {
            CommandInner::Commit(CCommit { comment, branch, prev, rev: crev }) => {
                // TODO prem check
                let prev = hex_to_hash(prev)?;
                assert_eq!(repo.get_ref(&branch)?, prev);
                let mut rev = Vec::new();
                for CRev { kind, object_kind, path, content } in crev {
                    let inner = match kind {
                        RevKind::Update => {
                            let content = content.unwrap();
                            let hash = match object_kind {
                                ObjectKind::Data => self.add_data_object(bson_to_doc(Bson::try_from(content)?)?).await?,
                                ObjectKind::Page => self.add_page_object(json_to_string(content)?).await?,
                            };
                            RevInner::Update { hash }
                        },
                        RevKind::Remove => {
                            assert!(matches!(content, None));
                            RevInner::Remove
                        },
                    };
                    rev.push(Rev { inner, object_kind, path });
                }
                self.commit(Commit { prev, ts, author, comment, merge: None, rev }, vec![&branch]).await?;
            },
            CommandInner::CreateCommonBranch(CCreateCommonBranch { prev }) => {
                self.create_branch(hex_to_hash(prev)?, &Branch::Common(CommonBranch { ts, author })).await?;
            },
            CommandInner::MergeCommonBranchToMain(CMergeCommonBranchToMain { branch, comment }) => {
                // TODO prem check
                let branch = Branch::Common(branch);
                let prev = repo.get_ref(&branch)?;
                let commit = if repo.get_ref(&Main)? == repo.get_root_ref(&branch)? {
                    // fast-forward
                    Commit { prev, ts, author, comment, merge: Some(branch.clone()), rev: Vec::new() }
                } else {
                    // 3-way
                    unimplemented!()
                };
                self.commit(commit, vec![&Main]).await?;
            }
        }
        Ok(())
    }
}
