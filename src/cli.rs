use clap::Parser;

#[derive(Parser, Debug)]
#[command()]
pub struct Args {
    #[arg()]
    pub source_root: String,
    #[arg()]
    pub mount_point: String,
}
