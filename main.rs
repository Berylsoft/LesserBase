use std::{path::PathBuf, fs};
use structopt::StructOpt;
use lesserbase::executor::Context;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "d", long)]
    db_uri: String,
    #[structopt(short = "c", long, parse(from_os_str))]
    command: PathBuf,
}

#[tokio::main]
async fn main() {
    let Args { path, db_uri, command } = Args::from_args();
    let ctx = Context::init(path, &db_uri).await.unwrap();
    let cmd = serde_json::from_str(&fs::read_to_string(command).unwrap()).unwrap();
    println!("{:?}", cmd);
    println!("{:?}", ctx.exec(cmd).await.unwrap());
}
