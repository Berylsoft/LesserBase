use crate::{prelude::*, model::*};

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

struct PathBuilder {
    root: PathBuf,
    config: PathBuf,
    objects: PathBuf,
    data_objects: PathBuf,
    page_objects: PathBuf,
    commits: PathBuf,
    states: PathBuf,
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
            states: (&root).join("states"),
            refs: (&root).join("refs"),
            root, objects,
        }
    }

    fn data_object(&self, hash: Hash) -> PathBuf {
        self.data_objects.join(hash_to_hex(hash).as_ref())
    }

    fn page_object(&self, hash: Hash) -> PathBuf {
        self.page_objects.join(hash_to_hex(hash).as_ref())
    }

    fn commit(&self, hash: Hash) -> PathBuf {
        self.commits.join(hash_to_hex(hash).as_ref())
    }

    fn state(&self, hash: Hash) -> PathBuf {
        self.states.join(hash_to_hex(hash).as_ref())
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

fn write_blob(path: PathBuf, blob: &[u8]) -> io::Result<()> {
    // TODO: err: hash collision
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(blob)?;
    Ok(())
}

// use fs::read as read_blob;

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

    pub fn init(&self) -> io::Result<()> {
        if !file_detected(&self.path.aref(&Main))? {
            println!("{:?}", self.path.data_objects());
            fs::create_dir_all(self.path.data_objects())?;
            fs::create_dir_all(self.path.page_objects())?;
            fs::create_dir_all(self.path.commits())?;
            fs::create_dir_all(self.path.refs())?;
            self.update_ref(&Main, EMPTY_HASH)?;
        }
        Ok(())
    }

    pub fn create_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.aref(branch))?;
        file.write(hash_to_hex(hash).as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    pub fn update_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(self.path.aref(branch))?;
        file.write(hash_to_hex(hash).as_bytes())?;
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
        Ok(hex_to_hash(&buf[0..HASH_LEN * 2])?)
    }

    pub fn get_root_ref(&self, branch: &Branch) -> anyhow::Result<Hash> {
        let mut file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        let mut buf = [0u8; HASH_LEN * 2];
        file.read(&mut buf)?;
        Ok(hex_to_hash(&buf)?)
    }

    pub fn get_all_ref(&self, branch: &Branch) -> anyhow::Result<Vec<Hash>> {
        let file = OpenOptions::new().read(true).open(self.path.aref(branch))?;
        let buf = BufReader::new(file);
        let mut result = Vec::new();
        for h in buf.lines() {
            result.push(hex_to_hash(h?)?)
        }
        Ok(result)
    }

    // pub fn get_data_object(&self, hash: Hash) -> anyhow::Result<Json> {
    //     Ok(msgpack_to_json(msgpack_decode(read_blob(self.path.data_object(hash))?)?))
    // }

    // pub fn get_page_object(&self, hash: Hash) -> anyhow::Result<String> {
    //     Ok(String::from_utf8(read_blob(self.path.page_object(hash))?)?)
    // }

    // pub fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
    //     Ok(rmp_serde::from_slice(&read_blob(self.path.commit(hash))?)?)
    // }

    pub fn add_data_object(&self, content: Json) -> anyhow::Result<Hash> {
        // TODO schema check
        let blob = msgpack_encode(json_to_msgpack(content.clone()))?;
        let hash = hash_all(&blob);
        write_blob(self.path.data_object(hash), &blob)?;
        // db.add_data_object(hash, content).await?;
        Ok(hash)
    }

    pub fn add_page_object(&self, content: String) -> anyhow::Result<Hash> {
        let blob = content.as_bytes();
        let hash = hash_all(blob);
        write_blob(self.path.page_object(hash), blob)?;
        // db.add_page_object(hash, content).await?;
        Ok(hash)
    }

    pub fn create_branch(&self, branch: &Branch, prev: Hash) -> anyhow::Result<()> {
        self.create_ref(branch, prev)?;
        // db.create_ref(branch, prev).await?;
        Ok(())
    }

    pub fn commit(&self, commit: Commit, branches: Vec<&Branch>) -> anyhow::Result<()> {
        let blob = rmp_serde::to_vec_named(&commit)?;
        let hash = hash_all(&blob);

        write_blob(self.path.commit(hash), &blob)?;
        // db.add_commit(hash, &commit).await?;

        for branch in branches {
            self.update_ref(branch, hash)?;
            // db.update_ref(branch, hash).await?;
        }

        // let mut state = self.get_state(commit.prev).await?;
        // state.update(commit.rev);
        // self.add_state(hash, state).await?;
        // let mut state = db.get_state(commit.prev).await?;
        // state.update(commit.rev);
        // db.add_state(hash, state).await?;

        Ok(())
    }
}
