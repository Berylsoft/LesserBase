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

path_builder_get_impl!(
    root,
    config,
    objects,
    data_objects,
    page_objects,
    commits,
    states,
    refs,
);

pub enum DbOp {
    AddDataObject { hash: Hash, content: Json },
    AddPageObject { hash: Hash, content: String },
    AddCommit { hash: Hash, commit: Commit },
    AddState { hash: Hash, state: State },
    CreateRef { branch: Branch, hash: Hash },
    UpdateRef { branch: Branch, hash: Hash },
}

type DbTx = Sender<DbOp>;
type DbRx = Receiver<DbOp>;

pub struct Repo {
    config: RepoConfig,
    path: PathBuilder,
    db_tx: DbTx,
}

use fs::read as read_blob;

fn write_blob<P: AsRef<Path>>(path: P, blob: &[u8]) -> io::Result<()> {
    // TODO: err: hash collision
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(blob)?;
    Ok(())
}

impl Repo {
    pub fn new(path: PathBuf) -> anyhow::Result<(Repo, DbRx)> {
        let path = PathBuilder::new(path);
        if file_detected(&path.config())? {
            let config: RepoConfig = toml::from_str(fs::read_to_string(path.config())?.as_str())?;
            if config.version != VERSION {
                Err(anyhow::anyhow!("config version {} != currect version {}", config.version, VERSION))
            } else {
                let (db_tx, db_rx) = channel();
                let repo = Repo { config, path, db_tx };
                if !file_detected(&repo.path.aref(&repo.config.online_branch))? {
                    repo.init()?;
                }
                Ok((repo, db_rx))
            }
        } else {
            Err(anyhow::anyhow!("config not exist"))
        }
    }

    pub fn new_with_default_config(path: PathBuf) -> anyhow::Result<(Repo, DbRx)> {
        fs::create_dir_all(&path)?;
        let path = PathBuilder::new(path);
        let config = RepoConfig::default();
        write_blob(path.config(), &toml::to_vec(&config)?)?;
        let (db_tx, db_rx) = channel();
        let repo = Repo { config, path, db_tx };
        repo.init()?;
        Ok((repo, db_rx))
    }

    pub fn init(&self) -> io::Result<()> {
        fs::create_dir_all(self.path.data_objects())?;
        fs::create_dir_all(self.path.page_objects())?;
        fs::create_dir_all(self.path.commits())?;
        fs::create_dir_all(self.path.states())?;
        fs::create_dir_all(self.path.refs())?;
        self.fs_create_ref(&self.config.online_branch, EMPTY_HASH)?;
        Ok(())
    }

    // region: get from fs

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

    pub fn get_data_object(&self, hash: Hash) -> anyhow::Result<Json> {
        Ok(msgpack_to_json(msgpack_decode(read_blob(self.path.data_object(hash))?)?))
    }

    pub fn get_page_object(&self, hash: Hash) -> anyhow::Result<String> {
        Ok(String::from_utf8(read_blob(self.path.page_object(hash))?)?)
    }

    pub fn get_commit(&self, hash: Hash) -> anyhow::Result<Commit> {
        Ok(rmp_serde::from_slice(&read_blob(self.path.commit(hash))?)?)
    }

    pub fn get_state(&self, hash: Hash) -> anyhow::Result<State> {
        Ok(rmp_serde::from_slice(&read_blob(self.path.state(hash))?)?)
    }

    // endregion

    // region: add to fs

    pub fn fs_create_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(self.path.aref(branch))?;
        file.write(hash_to_hex(hash).as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    pub fn fs_update_ref(&self, branch: &Branch, hash: Hash) -> io::Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(self.path.aref(branch))?;
        file.write(hash_to_hex(hash).as_bytes())?;
        file.write(b"\n")?;
        file.flush()?;
        Ok(())
    }

    // endregion

    // region: add all

    pub fn add_data_object(&self, content: Json) -> anyhow::Result<Hash> {
        // TODO content: HashMap<String, Json>
        // TODO schema check
        let blob = msgpack_encode(json_to_msgpack(content.clone()))?;
        let hash = hash_all(&blob);
        write_blob(self.path.data_object(hash), &blob)?;
        self.db_tx.send(DbOp::AddDataObject { hash, content })?;
        Ok(hash)
    }

    pub fn add_page_object(&self, content: String) -> anyhow::Result<Hash> {
        let blob = content.as_bytes();
        let hash = hash_all(blob);
        write_blob(self.path.page_object(hash), blob)?;
        self.db_tx.send(DbOp::AddPageObject { hash, content })?;
        Ok(hash)
    }

    pub fn add_commit(&self, commit: &Commit) -> anyhow::Result<Hash> {
        let blob = rmp_serde::to_vec_named(commit)?;
        let hash = hash_all(&blob);
        write_blob(self.path.commit(hash), &blob)?;
        self.db_tx.send(DbOp::AddCommit { hash, commit: commit.clone() })?;
        Ok(hash)
    }

    pub fn add_state(&self, hash: Hash, state: State) -> anyhow::Result<()> {
        let blob = rmp_serde::to_vec_named(&state)?;
        write_blob(self.path.state(hash), &blob)?;
        self.db_tx.send(DbOp::AddState { hash, state })?;
        Ok(())
    }

    pub fn create_ref(&self, branch: &Branch, hash: Hash) -> anyhow::Result<()> {
        self.fs_create_ref(branch, hash)?;
        self.db_tx.send(DbOp::CreateRef { branch: branch.clone(), hash })?;
        Ok(())
    }

    pub fn update_ref(&self, branch: &Branch, hash: Hash) -> anyhow::Result<()> {
        self.fs_update_ref(branch, hash)?;
        self.db_tx.send(DbOp::UpdateRef { branch: branch.clone(), hash })?;
        Ok(())
    }

    // endregion

    // region: high level methods

    pub fn commit(&self, commit: Commit, branches: Vec<&Branch>) -> anyhow::Result<()> {
        let hash = self.add_commit(&commit)?;

        for branch in branches {
            self.update_ref(branch, hash)?;
        }

        let mut state = self.get_state(commit.prev)?;
        state.update(commit.rev);
        self.add_state(hash, state)?;

        Ok(())
    }

    // endregion
}
