use crate::{prelude::*, commit::*};

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

    pub fn update_ref(&self, branch: &str, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(self.path.aref(branch))?;
        file.write(hash.to_hex().as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    pub fn get_ref(&self, branch: &str) -> anyhow::Result<Hash> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        file.seek(io::SeekFrom::End(-(HASH_LEN_I64 * 2 + 1)))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf.len(), HASH_LEN * 2 + 1);
        Ok(Hash::from_hex(&buf[0..HASH_LEN * 2])?)
    }

    pub fn add_object(&self, blob: &[u8]) -> io::Result<Hash> {
        let hash = hash_all(blob);
        // TODO: err: hash collision
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.object(hash))?;
        file.write_all(blob)?;
        Ok(hash)
    }

    pub fn get_object(&self, hash: Hash) -> io::Result<Vec<u8>> {
        fs::read(self.path.object(hash))
    }

    pub fn add_commit(&self, commit: Commit) -> anyhow::Result<Hash> {
        let blob = bson::to_vec(&CommitDocument::from(commit))?;
        let hash = hash_all(&blob);
        // TODO: err: hash collision
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.commit(hash))?;
        file.write_all(&blob)?;
        Ok(hash)
    }

    pub fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
        let file = OpenOptions::new().read(true).open(self.path.commit(hash))?;
        let doc: CommitDocument = bson::from_reader(file)?;
        Ok(Commit::from(doc))
    }
}
