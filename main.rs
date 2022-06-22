use std::path::PathBuf;
use structopt::StructOpt;
use lesserbase::{command::Command, executor::Context};

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "v", long)]
    db_uri: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path, db_uri } = Args::from_args();

    let ctx = Context::init(path, &db_uri).await?;
    let cmd: Command = serde_json::from_str(r#"{"author":"stackinspector","ts":1655561482939,"inner":{"type":"Commit","inner":{"comment":"hello lesserbase","branch":"main","prev":"0000000000000000000000000000000000000000000000000000000000000000","rev":[{"kind":0,"object_kind":0,"path":"test","content":{"testtrait.testattr":"test"}},{"kind":0,"object_kind":1,"path":"test","content":"<h1>testpage</h1>"}]}}}"#)?;
    println!("{:#?}", ctx.exec(cmd).await);

    Ok(())
}
