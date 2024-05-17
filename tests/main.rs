#![cfg(test)]

use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::Result;
use once_cell::sync::Lazy;
use testcontainers::{core::Mount, runners::AsyncRunner, *};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

use futures::prelude::*;

static PROJECT_PATH: Lazy<&Path> = Lazy::new(|| Path::new(env!("CARGO_MANIFEST_DIR")));
static TESTDATA_PATH: Lazy<PathBuf> = Lazy::new(|| PROJECT_PATH.join("testdata"));
static INTEGRATION_TESTS_TMPDIR: Lazy<&Path> = Lazy::new(|| Path::new(env!("CARGO_TARGET_TMPDIR")));

#[tokio::test]
async fn main() -> Result<()> {
    let mount_from = &(*TESTDATA_PATH);
    let mount_to = *INTEGRATION_TESTS_TMPDIR;

    let mut musicfs_process = Command::new("cargo")
        .current_dir(*PROJECT_PATH)
        .args(["run", "--"])
        .arg(mount_from)
        .arg(mount_to)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn musicfs process");

    let (r#in, out, err) = (
        musicfs_process.stdin.take().unwrap(),
        musicfs_process.stdout.take().unwrap(),
        musicfs_process.stderr.take().unwrap(),
    );

    let mut outbuf = BufReader::new(out).lines();
    while let Some(line) = outbuf.next_line().await? {
        println!("[STDOUT] {line}");
    }

    // Ensure the child process is spawned in the runtime so it can
    // make progress on its own while we await for any output.
    let exitcode = tokio::spawn(async move {
        let status = musicfs_process
            .wait()
            .await
            .expect("child process encountered an error");

        println!("child status was: {}", status);
    });

    exitcode.await?;

    Ok(())
}
