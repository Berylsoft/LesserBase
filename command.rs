pub struct Command {
    author: String,
    ts: u64,
    inner: CommandInner,
}

#[derive(Debug)]
pub enum CommandInner {
    Commit { comment: String,  }
}
