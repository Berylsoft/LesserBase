use crate::prelude::*;

const OBJECT_KIND_DATA_STR: &'static str = "data";
const OBJECT_KIND_PAGE_STR: &'static str = "page";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub prev: Hash,
    pub ts: u64,
    pub author: String,
    pub comment: String,
    pub merge: Option<Branch>,
    pub rev: Vec<Rev>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rev {
    #[serde(flatten)]
    pub inner: RevInner,
    pub object_kind: ObjectKind,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectKind {
    Data,
    Page,
}

impl ObjectKind {
    pub fn to_sign(&self) -> &'static str {
        match self {
            ObjectKind::Data => OBJECT_KIND_DATA_STR,
            ObjectKind::Page => OBJECT_KIND_PAGE_STR,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum RevInner {
    Update {
        hash: Hash,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RevKind {
    Update,
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Branch {
    Main,
    Common(CommonBranch),
}

pub use Branch::Main;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonBranch {
    pub ts: u64,
    pub author: String,
}

impl Branch {
    pub fn to_string(&self) -> String {
        match self {
            Branch::Main => "main".to_owned(),
            Branch::Common(CommonBranch { ts, author }) => format!("{}-{}", ts, author),
        }
    }
}

pub type StateMap = HashMap<String, Hash>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub data: StateMap,
    pub page: StateMap,
}

impl State {
    pub fn empty() -> State {
        State { data: HashMap::new(), page: HashMap::new() }
    }
}
