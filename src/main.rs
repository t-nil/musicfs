#![deny(clippy::all)]
#![deny(clippy::pedantic)]

use clap::Parser as _;
use cli::Args;
use env_logger::Env;
use fs::MusicFS;
use futures::future::err;
use log::{debug, error, Level};

mod cli;
mod fs;
mod media;
mod types;

fn main() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();
    error!("test main");

    let args = Args::parse();
    let music_fs = MusicFS::from_dir(args.source_root);

    eprintln!("{music_fs:#?}");
}
