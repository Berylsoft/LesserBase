pub use std::{path::{Path, PathBuf}, fs::{self, OpenOptions}, io::{self, Read, Write, Seek}, collections::HashMap};
pub use serde::{Serialize, Deserialize};
pub use serde_json::{Value as Json, json};
pub use bson::{Bson, Document as BsonDocument, bson, doc as bson_doc, Binary as BsonBinary};
pub use blake3::{Hash, OUT_LEN as HASH_LEN, hash as hash_all};

pub use crate::VERSION;

pub type HashInner = [u8; HASH_LEN];
pub const EMPTY_HASH: HashInner = [0u8; HASH_LEN];
pub const HASH_LEN_I64: i64 = HASH_LEN as i64;

pub fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()
}

pub fn as_one_char(s: &str) -> char {
    let mut iter = s.chars();
    let elem = iter.next().unwrap();
    assert!(matches!(iter.next(), None));
    elem
}

pub fn is_file_not_found(err: &io::Error) -> bool {
    if let io::ErrorKind::NotFound = err.kind() { true } else { false }
}

pub fn file_detected(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(if metadata.is_file() { true } else { false }),
        Err(err) => if is_file_not_found(&err) { Ok(false) } else { Err(err) },
    }
}

pub const BSON_BIN_TYPE_GENERIC: bson::spec::BinarySubtype = bson::spec::BinarySubtype::Generic;

pub fn hash_to_bson_bin(hash: Hash) -> BsonBinary {
    BsonBinary { subtype: BSON_BIN_TYPE_GENERIC, bytes: hash.as_bytes().to_vec() }
}

pub fn bson_bin_to_hash(raw: BsonBinary) -> Hash {
    let BsonBinary { bytes, subtype } = raw;
    debug_assert_eq!(subtype, BSON_BIN_TYPE_GENERIC);
    let inner: HashInner = bytes.try_into().unwrap();
    Hash::from(inner)
}

pub fn bson_to_hash(bson: Bson) -> anyhow::Result<Hash> {
    if let Bson::Binary(raw) = bson { Ok(bson_bin_to_hash(raw)) } else { Err(anyhow::anyhow!("bson_to_bin failed")) }
}

pub fn bson_to_doc(bson: Bson) -> anyhow::Result<BsonDocument> {
    if let Bson::Document(doc) = bson { Ok(doc) } else { Err(anyhow::anyhow!("bson_to_doc failed")) }
}

pub fn json_to_string(json: Json) -> anyhow::Result<String> {
    match json {
        Json::String(string) => Ok(string),
        _ => Err(anyhow::anyhow!("json_to_string failed")),
    }
}
