use std::path::PathBuf;
use structopt::StructOpt;
use lesserbase::executor::Context;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "d", long)]
    db_uri: String,
    #[structopt(short = "c", long)]
    command: String,
}

#[tokio::main]
async fn main() {
    let Args { path, db_uri, command } = Args::from_args();
    let ctx = Context::init(path, &db_uri).await.unwrap();
    println!("{:?}", ctx.exec(serde_json::from_str(&command).unwrap()).await.unwrap());
}
