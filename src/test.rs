use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;

pub static PROJECT_PATH: Lazy<&Path> = Lazy::new(|| Path::new(env!("CARGO_MANIFEST_DIR")));
pub static TESTDATA_PATH: Lazy<PathBuf> = Lazy::new(|| PROJECT_PATH.join("testdata"));
