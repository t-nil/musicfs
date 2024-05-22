#![deny(clippy::all)]
#![deny(clippy::pedantic)]

use clap::Parser as _;
use cli::Args;
use env_logger::Env;
use fs::MusicFS;
use fuser::MountOption;
use futures::future::err;
use log::{debug, error, Level};

mod cli;
mod fs;
mod media;

#[cfg(test)]
mod test;

fn main() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let mut music_fs = MusicFS::from_dir(args.source_root);

    let result = music_fs
        .0
        .add_vpath("test", "%album_artist%/%album%/%track_number% - %title%");

    music_fs.1.iter().for_each(|err| eprintln!("{err}"));

    let mut mount_options = vec![
        MountOption::RO,
        MountOption::FSName("musicfs".to_string()),
        MountOption::AutoUnmount,
    ];

    let fs = fuser::mount2(music_fs.0, args.mount_point, &mount_options);

    //for file in music_fs.0.
}
