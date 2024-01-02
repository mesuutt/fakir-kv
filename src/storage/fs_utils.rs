use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;

pub(crate) fn open_file_for_write(dir: &PathBuf, file_name: &str) -> anyhow::Result<fs::File> {
    let path = dir.join(file_name);
    let new_file = OpenOptions::new()
        .append(true)
        // .read(true)
        .create(true)
        .open(&path)?;

    Ok(new_file)
}

pub(crate) fn open_file_for_read(dir: &PathBuf, file_name: &str) -> anyhow::Result<fs::File> {
    let path = dir.join(file_name);

    Ok(OpenOptions::new()
        .read(true)
        .open(path)?)
}