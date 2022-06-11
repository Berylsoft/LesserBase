#![allow(dead_code)]

const VERSION: &str = "0.1-alpha";
type HashInner = [u8; HASH_LEN];
const EMPTY_HASH: HashInner = [0u8; HASH_LEN];
const HASH_LEN_I64: i64 = HASH_LEN as i64;

use std::{path::{Path, PathBuf}, fs::{self, OpenOptions}, io::{self, Read, Write, Seek}};
use serde::{Serialize, Deserialize};
// use serde_with::serde_as;
use blake3::{Hash, OUT_LEN as HASH_LEN, hash as hash_all};

// region: util

fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()
}

fn is_file_not_found(err: &io::Error) -> bool {
    if let io::ErrorKind::NotFound = err.kind() { true } else { false }
}

fn file_detected(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(if metadata.is_file() { true } else { false }),
        Err(err) => if is_file_not_found(&err) { Ok(false) } else { Err(err) },
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
        if !file_detected(&self.path.aref("main"))? {
            fs::create_dir_all(self.path.objects())?;
            fs::create_dir_all(self.path.commits())?;
            fs::create_dir_all(self.path.refs())?;
            self.update_ref("main", Hash::from(EMPTY_HASH))?;
        }

        let object = self.add_object(&fs::read(r"D:\root\repo\Berylsoft\lesserbase\lib.rs")?)?;
        let c = Commit {
            prev: Hash::from(EMPTY_HASH),
            ts: now(),
            author: "stackinspector".to_owned(),
            rev: vec![
                Rev {
                    kind: RevKind::Update,
                    hash: object,
                    path: "/test".to_owned(),
                }
            ]
        };
        let commit = self.add_commit(c)?;
        self.update_ref("main", commit)?;
        println!("{:?}", self.get_ref("main"));
        println!("{:?}", self.get_commit(commit));
        Ok(())
    }

    fn update_ref(&self, branch: &str, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(self.path.aref(branch))?;
        file.write(hash.to_hex().as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    fn get_ref(&self, branch: &str) -> anyhow::Result<Hash> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        file.seek(io::SeekFrom::End(-(HASH_LEN_I64 * 2 + 1)))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf.len(), HASH_LEN * 2 + 1);
        Ok(Hash::from_hex(&buf[0..HASH_LEN * 2])?)
    }

    fn add_object(&self, blob: &[u8]) -> io::Result<Hash> {
        let hash = hash_all(blob);
        // TODO: err: hash collision
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.object(hash))?;
        file.write_all(blob)?;
        Ok(hash)
    }

    fn get_object(&self, hash: Hash) -> io::Result<Vec<u8>> {
        fs::read(self.path.object(hash))
    }

    fn add_commit(&self, commit: Commit) -> anyhow::Result<Hash> {
        let blob = bson::to_vec(&CommitDocument::from(commit))?;
        let hash = hash_all(&blob);
        // TODO: err: hash collision
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.commit(hash))?;
        file.write_all(&blob)?;
        Ok(hash)
    }

    fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
        let file = OpenOptions::new().read(true).open(self.path.commit(hash))?;
        let doc: CommitDocument = bson::from_reader(file)?;
        Ok(Commit::from(doc))
    }
}

#[derive(Debug)]
struct Commit {
    pub prev: Hash,
    pub ts: u64,
    pub author: String,
    pub rev: Vec<Rev>,
}

#[derive(Debug)]
struct Rev {
    pub kind: RevKind,
    pub hash: Hash,
    pub path: String,
}

#[derive(Debug)]
enum RevKind {
    Update,
    Remove,
}

// region: serde helper

#[derive(Serialize, Deserialize)]
struct CommitDocument {
    pub prev: bson::Binary,
    pub ts: u64,
    pub author: String,
    pub rev: Vec<RevDocument>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RevDocument {
    pub kind: u8,
    pub hash: bson::Binary,
    pub path: String,
}

fn hash_to_bson_bin(hash: Hash) -> bson::Binary {
    bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: hash.as_bytes().to_vec() }
}

fn bson_bin_to_hash(raw: bson::Binary) -> Hash {
    let inner: HashInner = raw.bytes.try_into().unwrap();
    Hash::from(inner)
}

impl From<Commit> for CommitDocument {
    fn from(commit: Commit) -> CommitDocument {
        CommitDocument {
            prev: hash_to_bson_bin(commit.prev),
            ts: commit.ts,
            author: commit.author,
            rev: commit.rev.into_iter().map(|r| RevDocument {
                kind: r.kind.into(),
                hash: hash_to_bson_bin(r.hash),
                path: r.path,
            }).collect(),
        }
    }
}

impl From<CommitDocument> for Commit {
    fn from(doc: CommitDocument) -> Commit {
        Commit {
            prev: bson_bin_to_hash(doc.prev),
            ts: doc.ts,
            author: doc.author,
            rev: doc.rev.into_iter().map(|r| Rev {
                kind: r.kind.into(),
                hash: bson_bin_to_hash(r.hash),
                path: r.path,
            }).collect(),
        }
    }
}

impl From<u8> for RevKind {
    fn from(digit: u8) -> Self {
        match digit {
            0 => RevKind::Update,
            1 => RevKind::Remove,
            _ => unreachable!(),
        }
    }
}

impl From<RevKind> for u8 {
    fn from(kind: RevKind) -> Self {
        match kind {
            RevKind::Update => 0,
            RevKind::Remove => 1,
        }
    }
}

// endregion

#[derive(Debug)]
pub enum Command {}
