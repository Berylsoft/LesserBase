pub use std::{path::{Path, PathBuf}, fs::{self, OpenOptions}, io::{self, Read, Write, Seek}};
pub use serde::{Serialize, Deserialize};
pub use blake3::{Hash, OUT_LEN as HASH_LEN, hash as hash_all};

pub use crate::VERSION;

pub type HashInner = [u8; HASH_LEN];
pub const EMPTY_HASH: HashInner = [0u8; HASH_LEN];
pub const HASH_LEN_I64: i64 = HASH_LEN as i64;

pub fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()
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

pub fn hash_to_bson_bin(hash: Hash) -> bson::Binary {
    bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: hash.as_bytes().to_vec() }
}

pub fn bson_bin_to_hash(raw: bson::Binary) -> Hash {
    let inner: HashInner = raw.bytes.try_into().unwrap();
    Hash::from(inner)
}

pub fn ivec_to_hash(raw: sled::IVec) -> Hash {
    let inner: HashInner = raw.as_ref().try_into().unwrap();
    Hash::from(inner)
}

pub fn as_one_char(s: &str) -> char {
    let mut iter = s.chars();
    let elem = iter.next().unwrap();
    assert!(matches!(iter.next(), None));
    elem
}
