use crate::{prelude::*, model::*, command::*, fs::Repo, db::Db};

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

        let commit_doc = CommitDocument::from(commit.clone());
        let blob = bson::to_vec(&commit_doc)?;
        let hash = hash_all(&blob);

        repo.add_commit(hash, &blob)?;
        db.add_commit(hash, commit_doc).await?;

        for branch in branches {
            repo.update_ref(branch, hash)?;
            db.update_ref(branch, hash).await?;
        }

        let State { mut data, mut page, .. } = db.get_state(commit.prev).await?;
        for r in commit.rev {
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
        db.add_state(hash, State { commit: hash, data, page }).await?;

        Ok(())
    }

    pub async fn exec(&self, cmd: Command) -> anyhow::Result<()> {
        let Context { repo, db } = self;
        let Command { ts, author, inner } = cmd;
        match inner {
            CommandInner::Commit(CCommit { comment, branch, prev, rev: _rev }) => {
                // TODO prem check
                let prev = Hash::from_hex(prev)?;
                assert_eq!(repo.get_ref(&branch)?, prev);
                let mut rev = Vec::new();
                for CRev { kind, object_kind, path, content } in _rev {
                    let kind = kind.try_into()?;
                    let object_kind = object_kind.try_into()?;
                    let hash = match object_kind {
                        ObjectKind::Data => self.add_data_object(bson_to_doc(Bson::try_from(content)?)?).await?,
                        ObjectKind::Page => self.add_page_object(json_to_string(content)?).await?,
                    };
                    rev.push(Rev { kind, hash, object_kind, path });
                }
                self.commit(Commit { prev, ts, author, comment, merge: None, rev }, vec![&branch]).await?;
            },
            CommandInner::CreateCommonBranch(CCreateCommonBranch { prev }) => {
                self.create_branch(Hash::from_hex(prev)?, &Branch::Common(CommonBranch { ts, author })).await?;
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
                self.commit(commit, vec![&Main, &branch]).await?;
            }
        }
        Ok(())
    }
}
