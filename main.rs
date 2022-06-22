use std::path::PathBuf;
use structopt::StructOpt;
use lesserbase::{command::Command, executor::Context};

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "s", long)]
    state_uri: String,
    #[structopt(short = "v", long)]
    view_uri: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path, state_uri, view_uri } = Args::from_args();

    let mut ctx = Context::init(path, &state_uri, &view_uri).await?;
    // view.get_commit(Hash::from(EMPTY_HASH)).await?;
    let cmd: Command = serde_json::from_str(r#"{"author":"stackinspector","ts":1655561482939,"inner":{"type":"Commit","inner":{"comment":"hello lesserbase","branch":"main","prev":"0000000000000000000000000000000000000000000000000000000000000000","rev":[{"kind":0,"object_kind":0,"path":"test","content":{"testtrait.testattr":"test"}},{"kind":0,"object_kind":1,"path":"test","content":"<h1>testpage</h1>"}]}}}"#)?;
    println!("{:#?}", ctx.exec(cmd).await);

    Ok(())
}
