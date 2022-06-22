use crate::{prelude::*, commit::*};

#[derive(Debug, Serialize, Deserialize)]
struct RepoConfig {
    version: String,
}

struct PathBuilder {
    root: PathBuf,
    config: PathBuf,
    objects: PathBuf,
    data_objects: PathBuf,
    page_objects: PathBuf,
    commits: PathBuf,
    refs: PathBuf,
}

impl PathBuilder {
    fn new(root: PathBuf) -> PathBuilder {
        let objects = (&root).join("objects");
        PathBuilder {
            config: (&root).join("config"),
            data_objects: (&objects).join("data"),
            page_objects: (&objects).join("page"),
            commits: (&root).join("commits"),
            refs: (&root).join("refs"),
            root, objects,
        }
    }

    fn data_object(&self, hash: Hash) -> PathBuf {
        self.data_objects.join(hash.to_hex().as_str())
    }

    fn page_object(&self, hash: Hash) -> PathBuf {
        self.page_objects.join(hash.to_hex().as_str())
    }

    fn commit(&self, hash: Hash) -> PathBuf {
        self.commits.join(hash.to_hex().as_str())
    }

    fn aref(&self, branch: &Branch) -> PathBuf {
        self.refs.join(branch.to_string())
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

path_builder_get_impl!(root, config, objects, data_objects, page_objects, commits, refs,);

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
        if !file_detected(&self.path.aref(&Main))? {
            println!("{:?}", self.path.data_objects());
            fs::create_dir_all(self.path.data_objects())?;
            fs::create_dir_all(self.path.page_objects())?;
            fs::create_dir_all(self.path.commits())?;
            fs::create_dir_all(self.path.refs())?;
            self.update_ref(&Main, Hash::from(EMPTY_HASH))?;
        }
        Ok(())
    }

    pub fn create_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.aref(branch))?;
        file.write(hash.to_hex().as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    pub fn update_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(self.path.aref(branch))?;
        file.write(hash.to_hex().as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    pub fn get_ref(&self, branch: &Branch) -> anyhow::Result<Hash> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        file.seek(io::SeekFrom::End(-(HASH_LEN_I64 * 2 + 1)))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf.len(), HASH_LEN * 2 + 1);
        Ok(Hash::from_hex(&buf[0..HASH_LEN * 2])?)
    }

    pub fn get_root_ref(&self, branch: &Branch) -> anyhow::Result<Hash> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        let mut buf = [0u8; HASH_LEN * 2];
        file.read(&mut buf)?;
        Ok(Hash::from_hex(&buf)?)
    }

    pub fn get_all_ref(&self, branch: &Branch) -> anyhow::Result<Vec<Hash>> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let mut result = Vec::new();
        for h in buf.split('\n') {
            result.push(Hash::from_hex(h)?)
        }
        Ok(result)
    }

    pub fn add_data_object(&self, hash: Hash, blob: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.data_object(hash))?;
        file.write_all(blob)?;
        Ok(())
    }

    pub fn get_data_object(&self, hash: Hash) -> io::Result<Vec<u8>> {
        fs::read(self.path.data_object(hash))
    }

    pub fn add_page_object(&self, hash: Hash, blob: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.page_object(hash))?;
        file.write_all(blob)?;
        Ok(())
    }

    pub fn get_page_object(&self, hash: Hash) -> io::Result<Vec<u8>> {
        fs::read(self.path.page_object(hash))
    }

    pub fn add_commit(&self, hash: Hash, blob: &[u8]) -> anyhow::Result<Hash> {
        // TODO: err: hash collision
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.commit(hash))?;
        file.write_all(blob)?;
        Ok(hash)
    }

    pub fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
        let file = OpenOptions::new().read(true).open(self.path.commit(hash))?;
        let doc: CommitDocument = bson::from_reader(file)?;
        Ok(Commit::from(doc))
    }
}
