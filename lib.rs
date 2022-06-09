#![allow(dead_code)]

const VERSION: &str = "0.1-alpha";
const EMPTY_HASH: [u8; HASH_LEN] = [0u8; HASH_LEN];

use std::{path::{Path, PathBuf}, fs, io::{self, Write}};
use serde::{Serialize, Deserialize};
use blake3::{Hasher, Hash, OUT_LEN as HASH_LEN};

// region: util

pub fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()
}

fn file_detected(path: &Path) -> io::Result<bool> {
    if let Err(err) = fs::metadata(path) {
        if let io::ErrorKind::NotFound = err.kind() {
            Ok(false)
        } else {
            Err(err)
        }
    } else {
        Ok(true)
    }
}

// endregion

#[derive(Debug, Serialize, Deserialize)]
struct RepoConfig {
    version: String,
}

struct PathBuilder {
    root: PathBuf,
    config: PathBuf,
    objects: PathBuf,
    commits: PathBuf,
    refs: PathBuf,
}

// TODO: macro
impl PathBuilder {
    fn new(root: PathBuf) -> PathBuilder {
        PathBuilder {
            config: (&root).join("config"),
            objects: (&root).join("objects"),
            commits: (&root).join("commits"),
            refs: (&root).join("refs"),
            root,
        }
    }

    fn root(&self) -> &PathBuf { &self.root }
    fn config(&self) -> &PathBuf { &self.config }
    fn objects(&self) -> &PathBuf { &self.objects }
    fn commits(&self) -> &PathBuf { &self.commits }
    fn refs(&self) -> &PathBuf { &self.refs }

    fn object(&self, hash: Hash) -> PathBuf {
        self.objects.join(hash.to_hex().as_str())
    }

    fn commit(&self, hash: Hash) -> PathBuf {
        self.commits.join(hash.to_hex().as_str())
    }

    fn aref(&self, branch: &str) -> PathBuf {
        self.refs.join(branch)
    }
}

pub struct Repo {
    config: RepoConfig,
    path: PathBuilder,
}

impl Repo {
    pub fn new(path: PathBuf) -> anyhow::Result<Repo> {
        let path = PathBuilder::new(path);
        if file_detected(&path.config())? {
            let config: RepoConfig = toml::from_str(fs::read_to_string(path.config())?.as_str())?;
            if config.version != VERSION {
                Err(anyhow::anyhow!("config version {} != currect version {}", config.version, VERSION))
            } else {
                Ok(Repo { config, path })
            }
        } else {
            Err(anyhow::anyhow!("config not exist"))
        }
    }

    pub fn init(&self) -> anyhow::Result<()> {
        let ref_main = self.path.aref("main");

        if !file_detected(&ref_main)? {
            fs::create_dir_all(self.path.objects())?;
            fs::create_dir_all(self.path.commits())?;
            fs::create_dir_all(self.path.refs())?;
            {
                let mut ref_main = fs::OpenOptions::new().create_new(true).write(true).open(&ref_main)?;
                ref_main.write(Hash::from(EMPTY_HASH).to_hex().as_bytes())?;
                ref_main.write(b"\n")?;
                ref_main.flush()?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum Command {}
