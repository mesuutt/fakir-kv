use std::fs;
use std::fs::OpenOptions;
use std::path::{Path};
use std::time::{SystemTime, UNIX_EPOCH};


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

pub(crate) fn build_data_file_name(file_id: u64) -> String {
    format!("{file_id}.bitcask.data")
}

#[inline]
pub(crate) fn timestamp() -> u32 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32
}

pub(crate) fn expiry_time(expire_secs: u32) -> u32 {
    if expire_secs > 0 {
        timestamp() - expire_secs
    } else {
        0
    }
}
