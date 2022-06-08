#![allow(dead_code)]

const VERSION: &str = "0.1-alpha";

use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct RepoConfig {
    version: String,
}

struct Repo {
    config: RepoConfig,
    path: PathBuilder,
}

impl Repo {
    fn new(path: PathBuf) -> anyhow::Result<Repo> {
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

    fn init(&self) -> anyhow::Result<()> {
        let ref_main = self.path.aref("main");

        if !file_detected(&ref_main)? {
            fs::create_dir_all(self.path.objects())?;
            fs::create_dir_all(self.path.commits())?;
            fs::create_dir_all(self.path.refs())?;
            {
                let mut ref_main = fs::OpenOptions::new().create_new(true).write(true).open(&ref_main)?;
                ref_main.write(Hash::empty().to_hex().as_bytes())?;
                ref_main.write(b"\n")?;
                ref_main.flush()?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Command {}

use std::{path::{Path, PathBuf}, fs, io::{self, Write}};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
}

const HASH_LEN: usize = 32;

struct Hash([u8; HASH_LEN]);

impl Hash {
    #[inline]
    fn as_bytes(&self) -> &[u8; HASH_LEN] {
        &self.0
    }

    fn empty() -> Hash {
        Hash([0u8; HASH_LEN])
    }

    fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
    }

    fn from_str(s: &str) -> Result<Hash, hex::FromHexError> {
        let mut hash = [0u8; HASH_LEN];
        hex::decode_to_slice(s, &mut hash)?;
        Ok(Hash(hash))
    }
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

// enum Directory {
//     Objects,
//     Commits,
//     Refs,
// }

struct PathBuilder {
    root: PathBuf,
}

impl PathBuilder {
    fn new(root: PathBuf) -> PathBuilder {
        PathBuilder { root }
    }

    fn config(&self) -> PathBuf {
        self.root.clone().join("config")
    }

    fn objects(&self) -> PathBuf {
        self.root.clone().join("objects")
    }

    fn object(&self, hash: Hash) -> PathBuf {
        self.root.clone().join("objects").join(hash.to_hex())
    }

    fn commits(&self) -> PathBuf {
        self.root.clone().join("commits")
    }

    fn commit(&self, hash: Hash) -> PathBuf {
        self.root.clone().join("commits").join(hash.to_hex())
    }
    fn refs(&self) -> PathBuf {
        self.root.clone().join("refs")
    }

    fn aref(&self, branch: &str) -> PathBuf {
        self.root.clone().join("refs").join(branch)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path } = Args::from_args();

    let repo = Repo::new(path)?;
    repo.init()?;

    Ok(())
}
