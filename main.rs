use std::{path::PathBuf, fs};
use structopt::StructOpt;
use lesserbase::repo::Repo;

#[derive(StructOpt)]
struct Args {
    #[structopt(short = "p", long, parse(from_os_str))]
    path: PathBuf,
    // #[structopt(short = "d", long)]
    // db_uri: String,
    #[structopt(short = "c", long, parse(from_os_str))]
    command: PathBuf,
}

fn main() {
    let Args { path, command } = Args::from_args();
    let repo = Repo::new(path).unwrap();
    repo.init().unwrap();
    let cmd = serde_json::from_str(&fs::read_to_string(command).unwrap()).unwrap();
    println!("{:?}", cmd);
    println!("{:?}", repo.exec(cmd).unwrap());
}
