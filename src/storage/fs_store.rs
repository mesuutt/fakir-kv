use std::fs;
use std::io::{Read, Seek, SeekFrom, stderr, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};


use anyhow::{Context, Result};
use bytes::BufMut;

use crate::storage::{CRC_OFFSET, CRC_SIZE, file_lock, utils, FsBackend, FsReader, FsWriter, Header, KEY_OFFSET, KEY_SIZE_OFFSET, KeyDir, Reader, TOMBSTONE_MARKER_CHAR, VAL_SIZE_OFFSET, Writer};
use crate::storage::config::Config;
use crate::storage::utils::build_data_file_name;

#[derive(Debug)]
pub struct FsStorage {
    active_file: fs::File,
    active_file_id: u64,
    position: u32,
    key_dir: KeyDir,
    conf: Config,
    mu: Mutex<u64>, // keeps active_file_id
}

impl FsStorage {

    // log_writer'a tasindi
    pub fn open(conf: Config) -> Result<Self> {
        fs::create_dir_all(&conf.path).context("data directory creation failed")?;
        file_lock::try_lock_db(&conf.path)?;

        let active_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let filename = format!("{}.bitcask.data", active_file_id);
        let active_file = utils::open_file_for_write(&conf.path, &filename)?;

        let bitcask = FsStorage {
            active_file,
            active_file_id,
            position: 0,
            key_dir: Default::default(),
            mu: Mutex::new(active_file_id),
            conf,
        };

        Ok(bitcask)
    }

    // log_writer'a tasindi
    #[inline]
    fn sync(&mut self) -> Result<()> {
        // TODO: we can create flush_on_put config for flushing after puts.
        // flushing content can be not needed for some use cases?
        self.active_file.flush()?;
        if self.conf.sync_on_put {
            self.active_file.sync_data()?;
        }
        Ok(())
    }

    fn write_to_file(&mut self, buf: &[u8]) -> Result<()> {
        self.active_file.write_all(buf).context("file write failed")?;
        self.position += buf.len() as u32;

        Ok(())
    }

}



#[cfg(test)]
mod test {
    use std::io::{Read, Seek, SeekFrom};

    use tempdir::TempDir;

    use super::{Config, Reader, Writer};
    use crate::storage::{CRC_OFFSET, CRC_SIZE, utils, FsBackend, KEY_OFFSET, KEY_SIZE_OFFSET, VAL_SIZE_OFFSET, Writer};
    use crate::storage::config::Config;

    use super::FsStorage;

    #[test]
    fn it_should_load_cask_from_file() {
        let cask_result = FsStorage::open(Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        });

        assert!(cask_result.is_ok());
        assert_eq!(0, cask_result.unwrap().position);
    }

    #[test]
    fn it_should_put_to_file() {
        // given
        let dir = TempDir::new("bitcask-").unwrap().into_path();
        let mut write_cask = FsStorage::open(Config {
            path: dir.clone(),
            ..Default::default()
        }).unwrap();
        let key = b"foo";
        let val = b"bar";

        // when
        write_cask.put(key, val).unwrap();

        // then
        let mut payload = vec![0; KEY_OFFSET + key.len() + val.len()];

        // let mut cask_file = File::open(Path::join(dir.path(), FsStorage::make_filename(write_cask.active_file_id))).unwrap();
        let filename = format!("{}.bitcask.data", write_cask.active_file_id);
        let mut cask_file = utils::open_file_for_read(&dir, &filename).unwrap();

        cask_file.seek(SeekFrom::Start(0)).unwrap();
        cask_file.read_exact(&mut payload).unwrap();

        let payload_without_crc = payload[CRC_OFFSET + CRC_SIZE..].to_vec();
        let checksum = crc32fast::hash(&payload_without_crc);
        assert_eq!(u32::from_be_bytes(payload[CRC_OFFSET..CRC_OFFSET + CRC_SIZE].try_into().unwrap()), checksum);

        assert_eq!(u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()), key.len() as u32);
        assert_eq!(u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()), val.len() as u32);

        assert_eq!(payload[KEY_OFFSET..KEY_OFFSET + key.len()], *key.as_slice());

        let val_offset = KEY_OFFSET + key.len();
        assert_eq!(payload[val_offset..val_offset + val.len()], *val.as_slice());

        let header = write_cask.key_dir.get(key.as_slice()).unwrap();
        assert_eq!(header.file_id, write_cask.active_file_id);
        assert_eq!(header.val_size, val.len() as u32);
        assert_eq!(header.val_offset, (KEY_OFFSET + key.len()).try_into().unwrap());
        assert_eq!(write_cask.position, (KEY_OFFSET + key.len() + val.len()).try_into().unwrap());
    }

    #[test]
    fn it_should_get() {
        // TODO: move to integration test

        // given
        let mut cask = FsStorage::open(Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        }).unwrap();

        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"val1"),
            (b"key2", b"val2"),
            (b"key1", b"val3"),
        ];

        for (key, val) in pairs {
            cask.put(key, val).unwrap();

            let actual = cask.get(key).unwrap().unwrap();
            assert_eq!(val, actual.as_slice());
        }
    }

    #[test]
    fn it_should_delete() {
        // TODO: move to integration test
        // write a unit test for here

        // given
        let mut cask = FsStorage::open(Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        }).unwrap();

        cask.put(b"k1", b"v1").unwrap();

        assert!(cask.delete(b"k1").is_ok());
        assert!(cask.get(b"k1").unwrap().is_none());
    }
}