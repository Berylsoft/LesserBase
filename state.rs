use crate::{prelude::*, commit::*};

const STATE_CACHE_MAX_BYTE: u64 = 1024 * 1024 * 16;
const STATE_FLUSH_INTERVAL_MS: u64 = 1000;

pub struct State {
    db: sled::Db,
    path: sled::Tree,
    head: sled::Tree,
}

impl State {
    pub fn new(path: PathBuf) -> anyhow::Result<State> {
        let db = sled::Config::default()
            .path(path)
            .cache_capacity(STATE_CACHE_MAX_BYTE)
            .flush_every_ms(Some(STATE_FLUSH_INTERVAL_MS))
            .open()?;
        let path = db.open_tree("path")?;
        let head = db.open_tree("head")?;
        Ok(State { db, path, head })
    }

    pub fn apply_rev(&self, branch: &str, rev: &Vec<Rev>) -> anyhow::Result<()> {
        for r in rev {
            let path = format!("{}|{}|{}", branch, r.object_kind.to_sign(), r.path);
            match r.kind {
                RevKind::Update => {
                    self.path.insert(path.as_bytes(), r.hash.as_bytes())?;
                }
                RevKind::Remove => {
                    self.path.remove(path.as_bytes())?;
                }
            }
        }
        Ok(())
    }

    pub fn query_path(&self, branch: &str, kind: ObjectKind, path: &str) -> anyhow::Result<Hash> {
        let hash = self.path.get(format!("{}|{}|{}", branch, kind.to_sign(), path))?.ok_or_else(|| anyhow::anyhow!(""))?;
        Ok(ivec_to_hash(hash))
    }

    pub fn update_head(&self, branch: &str, hash: Hash) -> anyhow::Result<()> {
        self.head.insert(branch, hash.as_bytes())?;
        Ok(())
    }

    pub fn get_head(&self, branch: &str) -> anyhow::Result<Hash> {
        let hash = self.head.get(branch)?.ok_or_else(|| anyhow::anyhow!(""))?;
        Ok(ivec_to_hash(hash))
    }
}
