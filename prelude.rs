pub use std::{path::{Path, PathBuf}, fs::{self, OpenOptions}, io::{self, Read, Write, Seek}, collections::HashMap};
pub use serde::{Serialize, Deserialize};
pub use serde_json::{Value as Json, json, value::Number as JsonNumber};
pub use rmpv::{Value as Msgpack, Integer as MsgpackInt};
pub use blake3::OUT_LEN as HASH_LEN;

pub use crate::VERSION;

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

pub type Hash = [u8; HASH_LEN];
pub const EMPTY_HASH: Hash = [0u8; HASH_LEN];
pub const HASH_LEN_I64: i64 = HASH_LEN as i64;

pub fn hash_all(input: &[u8]) -> Hash {
    *blake3::hash(input).as_bytes()
}

pub fn hash_to_hex(hash: Hash) -> Box<str> {
    Box::from(blake3::Hash::from(hash).to_hex().as_str())
}

pub fn hex_to_hash<B: AsRef<[u8]>>(hex: B) -> Result<Hash, blake3::HexError> {
    Ok(*blake3::Hash::from_hex(hex)?.as_bytes())
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

pub fn msgpack_to_json(msgpack: Msgpack) -> Json {
    match msgpack {
        Msgpack::Nil => Json::Null,
        Msgpack::Boolean(boolean) => Json::Bool(boolean),
        Msgpack::String(may_string) => Json::String(may_string.into_str().unwrap()),
        Msgpack::Integer(int) => {
            if let Some(int) = int.as_u64() {
                Json::Number(JsonNumber::from(int))
            } else if let Some(int) = int.as_i64() {
                Json::Number(JsonNumber::from(int))
            } else {
                unreachable!()
            }
        },
        Msgpack::F32(float) => Json::Number(JsonNumber::from_f64(float.into()).unwrap()),
        Msgpack::F64(float) => Json::Number(JsonNumber::from_f64(float).unwrap()),
        Msgpack::Array(vec) => Json::Array(vec.into_iter().map(msgpack_to_json).collect()),
        Msgpack::Map(map) => Json::Object(map.into_iter().map(|(k, v)| (k.try_into().unwrap(), msgpack_to_json(v))).collect()),
        Msgpack::Binary(_) | Msgpack::Ext(_, _) => unreachable!(),
    }
}

pub fn json_to_msgpack(json: Json) -> Msgpack {
    match json {
        Json::Null => Msgpack::Nil,
        Json::Bool(boolean) => Msgpack::Boolean(boolean),
        Json::String(string) => Msgpack::String(string.into()),
        Json::Number(number) => {
            if let Some(number) = number.as_u64() {
                Msgpack::Integer(MsgpackInt::from(number))
            } else if let Some(number) = number.as_i64() {
                Msgpack::Integer(MsgpackInt::from(number))
            } else if let Some(number) = number.as_f64() {
                Msgpack::F64(number)
            } else {
                unreachable!()
            }
        },
        Json::Array(vec) => Msgpack::Array(vec.into_iter().map(json_to_msgpack).collect()),
        Json::Object(map) => Msgpack::Map(map.into_iter().map(|(k, v)| (k.into(), json_to_msgpack(v))).collect()),
    }
}

pub fn msgpack_encode(msgpack: Msgpack) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &msgpack)?;
    Ok(buf)
}

pub fn msgpack_decode(raw: Vec<u8>) -> anyhow::Result<Msgpack> {
    Ok(rmpv::decode::read_value(&mut raw.as_slice())?)
}

pub fn json_to_string(json: Json) -> anyhow::Result<String> {
    match json {
        Json::String(string) => Ok(string),
        _ => Err(anyhow::anyhow!("json_to_string failed")),
    }
}
