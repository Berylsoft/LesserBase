use crate::{prelude::*, commit::*};
use redis::{Client, Connection, RedisResult};

pub struct State {
    conn: Connection,
}

impl State {
    pub async fn new(uri: &str) -> RedisResult<State> {
        let client = Client::open(uri)?;
        let conn = client.get_connection()?;

        Ok(State { conn })
    }
}
