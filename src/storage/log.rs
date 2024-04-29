// [crc|ts_tamp|ksz|vsz|key|val]

use std::{fs, io, str};
use std::fmt::{Debug, Formatter};
use std::io::{ErrorKind, Read, Seek};
use std::mem::size_of;

use anyhow::Error;

use crate::storage::Header;

pub const CRC_SIZE: usize = size_of::<u32>();
pub const TS_SIZE: usize = size_of::<u32>();
pub const KEY_SIZE: usize = size_of::<u32>();
pub const VAL_SIZE: usize = size_of::<u32>();

pub const CRC_OFFSET: usize = 0;
pub const KEY_SIZE_OFFSET: usize = CRC_SIZE + TS_SIZE;
pub const VAL_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE;
pub const KEY_OFFSET: usize = VAL_SIZE_OFFSET + VAL_SIZE;

// use backspace char as tombstone marker
pub const TOMBSTONE_MARKER_CHAR: u8 = 8;

pub struct LogEntry {
    // pub crc: [u8; CRC_SIZE],
    pub crc: Vec<u8>,
    // TODO: [u8;4] gibi kullanabiliriz.
    pub ts_tamp: u32,
    pub val_size: u32,
    pub val_offset: u32,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogEntry<key={}, val={}>", str::from_utf8(&self.key).unwrap(), str::from_utf8(&self.val).unwrap())
    }
}

pub struct LogIterator(fs::File);

impl LogIterator {
    pub fn new(f: fs::File) -> Self {
        Self { 0: f }
    }

    fn read_to(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match self.0.read(buf) {
            Ok(size) => return Ok(size),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(0),
            Err(e) => Err(e),
        }
    }
}

impl Iterator for LogIterator {
    type Item = anyhow::Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: crc check?
        let mut crc = [0u8; CRC_SIZE];
        let result = self.read_to(&mut crc);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        // CRC is first byte of the entry, so If 0 byte consumed, it means EOF
        if result.unwrap() == 0 {
            return None;
        }

        let mut timestamp = [0u8; TS_SIZE];
        let result = self.read_to(&mut timestamp);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        let mut key_size_bytes = [0u8; KEY_SIZE];
        let result = self.read_to(&mut key_size_bytes);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        let key_size = u32::from_be_bytes(key_size_bytes);

        let mut val_size_bytes = [0u8; VAL_SIZE];
        let result = self.read_to(&mut val_size_bytes);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        let val_size = u32::from_be_bytes(val_size_bytes);

        let mut key = vec![0u8; key_size as usize];

        let result = self.read_to(&mut key);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        let stream_pos = match self.0.stream_position() {
            Ok(pos) => { pos }
            Err(e) => return Some(Err(Error::from(result.unwrap_err()))),
        };

        let val_offset = match u32::try_from(stream_pos) {
            Ok(x) => { x }
            Err(e) => {
                return Some(Err(Error::from(e)));
            }
        };

        let mut val = vec![0u8; val_size as usize];
        let result = self.read_to(&mut val);
        if result.is_err() {
            return Some(Err(Error::from(result.unwrap_err())));
        }

        Some(Ok(LogEntry {
            crc: crc.to_vec(),
            ts_tamp: u32::from_be_bytes(timestamp),
            val_size,
            val_offset,
            key,
            val,
        }))
    }
}

fn byte_to_header(buf: &[u8]) -> anyhow::Result<Header> {
    todo!()
}

fn byte_to_entry(buf: &[u8]) -> anyhow::Result<Header> {
    todo!()
}
