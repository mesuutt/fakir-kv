use std::cell::RefCell;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use log::Log;

use crate::storage::utils::{build_data_file_name, open_file_for_read};

pub struct LogReader {
    file: RefCell<fs::File>,
}

impl LogReader {
    pub fn new<P>(dir: P, file_id: u64) -> anyhow::Result<Self> where P: AsRef<Path> {
        let file = open_file_for_read(dir, &build_data_file_name(file_id))?;
        Ok(LogReader { file: RefCell::new(file) })
    }

    pub fn read(&self, offset: u32, size: u32) -> anyhow::Result<Vec<u8>> {
        let mut f = self.file.borrow_mut();
        f.seek(SeekFrom::Start(offset as u64))?;

        let mut buf = vec![0u8; size as usize];
        f.read_exact(&mut buf)?;

        Ok(buf)
    }
}



