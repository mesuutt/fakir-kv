use std::fs;
use std::fs::OpenOptions;
use std::path::{Path};

pub(crate) fn open_file_for_write<P>(dir: P, file_name: &str) -> anyhow::Result<fs::File> where P: AsRef<Path> {
    let new_file = OpenOptions::new()
        .append(true)
        // .read(true)
        .create(true)
        .open(dir.as_ref().join(file_name))?;

    Ok(new_file)
}

pub(crate) fn open_file_for_read<P>(dir: P, file_name: &str) -> anyhow::Result<fs::File> where P: AsRef<Path> {
    Ok(OpenOptions::new()
        .read(true)
        .open(dir.as_ref().join(file_name))?)
}