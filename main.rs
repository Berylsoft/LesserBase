use std::path::PathBuf;
use structopt::StructOpt;
use lesserbase::Repo;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path } = Args::from_args();

    let repo = Repo::new(path)?;
    repo.init()?;

    Ok(())
}
