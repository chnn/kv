use kv::{KvError, KvStore};
use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "get", about = "Get the value for a key")]
    Get { key: String },
    #[structopt(name = "set", about = "Set the value for a key")]
    Set { key: String, value: String },
    #[structopt(name = "rm", about = "Remove the value for a key")]
    Remove { key: String },
}

#[derive(Debug, StructOpt)]
struct Arguments {
    #[structopt(subcommand)]
    command: Command,

    #[structopt(short = "d", long, parse(from_os_str), default_value = ".kv")]
    data_dir: PathBuf,
}

fn main() -> Result<(), KvError> {
    let options = Arguments::from_args();
    let mut kv = KvStore::open(&options.data_dir)?;

    match options.command {
        Command::Get { key } => match kv.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("null"),
        },
        Command::Remove { key } => kv.remove(key)?,
        Command::Set { key, value } => kv.set(key, value)?,
    }

    Ok(())
}
