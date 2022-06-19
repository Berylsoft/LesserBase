use std::path::PathBuf;
use structopt::StructOpt;
use lesserbase::{local::Repo, view::View, command::{Command, proc}};

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "v", long)]
    view_uri: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path, view_uri } = Args::from_args();

    let repo = Repo::new(path)?;
    repo.init()?;
    let view = View::new(&view_uri).await?;
    // view.get_commit(Hash::from(EMPTY_HASH)).await?;
    let cmd: Command = serde_json::from_str(r#"{"author":"stackinspector","ts":1655561482939,"inner":{"type":"Commit","inner":{"comment":"hello lesserbase","branch":"main","prev":"0000000000000000000000000000000000000000000000000000000000000000","rev":[{"kind":0,"object_kind":0,"path":"test","content":{"testtrait.testattr":"test"}},{"kind":0,"object_kind":1,"path":"test","content":"<h1>testpage</h1>"}]}}}"#)?;
    println!("{:#?}", proc(cmd, &repo, &view).await);

    Ok(())
}
