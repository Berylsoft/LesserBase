#![allow(dead_code)]

const VERSION: &str = "0.1-alpha";

use std::{path::{Path, PathBuf}, fs::{self, OpenOptions}, io::{self, Read, Write, Seek}};
use serde::{Serialize, Deserialize};
use blake3::{Hash, OUT_LEN as HASH_LEN, hash as hash_all};

type HashInner = [u8; HASH_LEN];
const EMPTY_HASH: HashInner = [0u8; HASH_LEN];
const HASH_LEN_I64: i64 = HASH_LEN as i64;

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

fn hash_to_bson_bin(hash: Hash) -> bson::Binary {
    bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: hash.as_bytes().to_vec() }
}

fn bson_bin_to_hash(raw: bson::Binary) -> Hash {
    let inner: HashInner = raw.bytes.try_into().unwrap();
    Hash::from(inner)
}

fn ivec_to_hash(raw: sled::IVec) -> Hash {
    let inner: HashInner = raw.as_ref().try_into().unwrap();
    Hash::from(inner)
}

fn as_one_char(s: &str) -> char {
    let mut iter = s.chars();
    let elem = iter.next().unwrap();
    assert!(matches!(iter.next(), None));
    elem
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

macro_rules! path_builder_get_impl {
    ($($x:ident,)*) => {
        impl PathBuilder {
            $(
            #[inline]
            fn $x(&self) -> &PathBuf { &self.$x }
            )*
        }
    };
}

path_builder_get_impl!(root, config, objects, commits, refs, );

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
    pub comment: String,
    pub rev: Vec<Rev>,
}

#[derive(Debug)]
struct Rev {
    pub kind: RevKind,
    pub hash: Hash,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug)]
enum ObjectKind {
    Data,
    Page,
}

impl ObjectKind {
    fn from_digit(digit: u8) -> ObjectKind {
        match digit {
            0 => ObjectKind::Page,
            1 => ObjectKind::Data,
            _ => unreachable!(),
        }
    }

    fn to_digit(&self) -> u8 {
        match self {
            ObjectKind::Page => 0,
            ObjectKind::Data => 1,
        }
    }

    fn from_sign(sign: char) -> ObjectKind {
        match sign {
            'D' => ObjectKind::Data,
            'P' => ObjectKind::Page,
            _ => unreachable!(),
        }
    }

    fn to_sign(&self) -> char {
        match self {
            ObjectKind::Data => 'D',
            ObjectKind::Page => 'P',
        }
    }
}

#[derive(Debug)]
enum RevKind {
    Update,
    Remove,
}

impl RevKind {
    fn from_digit(digit: u8) -> RevKind {
        match digit {
            0 => RevKind::Update,
            1 => RevKind::Remove,
            _ => unreachable!(),
        }
    }

    fn to_digit(&self) -> u8 {
        match self {
            RevKind::Update => 0,
            RevKind::Remove => 1,
        }
    }
}

// region: serde helper

#[derive(Serialize, Deserialize)]
struct CommitDocument {
    pub prev: bson::Binary,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub rev: Vec<RevDocument>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RevDocument {
    pub kind: u8,
    pub hash: bson::Binary,
    pub object_kind: u8,
    pub path: String,
}

impl From<Commit> for CommitDocument {
    fn from(commit: Commit) -> CommitDocument {
        CommitDocument {
            prev: hash_to_bson_bin(commit.prev),
            ts: commit.ts,
            author: commit.author,
            comment: commit.comment,
            rev: commit.rev.into_iter().map(|r| RevDocument {
                kind: r.kind.to_digit(),
                hash: hash_to_bson_bin(r.hash),
                object_kind: r.object_kind.to_digit(),
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
            comment: doc.comment,
            rev: doc.rev.into_iter().map(|r| Rev {
                kind: RevKind::from_digit(r.kind),
                hash: bson_bin_to_hash(r.hash),
                object_kind: ObjectKind::from_digit(r.object_kind),
                path: r.path,
            }).collect(),
        }
    }
}

// endregion

pub const STATE_CACHE_MAX_BYTE: u64 = 1024 * 1024 * 16;
pub const STATE_FLUSH_INTERVAL_MS: u64 = 1000;

struct State {
    db: sled::Db,
    path: sled::Tree,
    head: sled::Tree,
}

impl State {
    fn new(path: PathBuf) -> anyhow::Result<State> {
        let db = sled::Config::default()
            .path(path)
            .cache_capacity(STATE_CACHE_MAX_BYTE)
            .flush_every_ms(Some(STATE_FLUSH_INTERVAL_MS))
            .open()?;
        let path = db.open_tree("path")?;
        let head = db.open_tree("head")?;
        Ok(State { db, path, head })
    }

    fn apply_rev(&self, branch: &str, rev: &Vec<Rev>) -> anyhow::Result<()> {
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

    fn query_path(&self, branch: &str, kind: ObjectKind, path: &str) -> anyhow::Result<Hash> {
        let hash = self.path.get(format!("{}|{}|{}", branch, kind.to_sign(), path))?.ok_or_else(|| anyhow::anyhow!(""))?;
        Ok(ivec_to_hash(hash))
    }

    fn update_head(&self, branch: &str, hash: Hash) -> anyhow::Result<()> {
        self.head.insert(branch, hash.as_bytes())?;
        Ok(())
    }

    fn get_head(&self, branch: &str) -> anyhow::Result<Hash> {
        let hash = self.head.get(branch)?.ok_or_else(|| anyhow::anyhow!(""))?;
        Ok(ivec_to_hash(hash))
    }
}

pub struct Command {
    author: String,
    ts: u64,
    inner: CommandInner,
}

#[derive(Debug)]
pub enum CommandInner {
    Commit { comment: String,  }
}
