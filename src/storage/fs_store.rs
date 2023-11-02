use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use bytes::BufMut;

use crate::error::BitcaskError;
use crate::storage::{Header, KEY_SIZE, KeyDir, Reader, Storage, VAL_SIZE};

#[derive(Debug)]
pub struct FsStorage {
    active_file: fs::File,
    active_file_id: u64,
    position: u32,
    // current position
    key_dir: KeyDir,
    dir: PathBuf,
    read_file: fs::File,
}

impl FsStorage {
    pub fn load(dir: &str) -> Result<Self> {
        fs::create_dir_all(dir).context("key dir creation failed")?;

        let active_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let path = Path::new(dir).join(FsStorage::make_filename(active_file_id));
        if let Ok(_) = fs::metadata(&path) {
            return Err(anyhow!("not implemented yet"));
        }

        // println!("CASK FILE: {:?}", &path);
        let active_file = OpenOptions::new()
            .append(true)
            // .read(true)
            .create(true)
            .open(&path)?;

        let read_file = OpenOptions::new()
            .read(true)
            .open(path)?;

        let bitcask = FsStorage {
            active_file,
            read_file,
            active_file_id,
            position: 0,
            key_dir: Default::default(),
            dir: dir.parse()?,
        };

        Ok(bitcask)
    }

    fn make_filename(file_id: u64) -> String {
        format!("{}.bitcask.data", file_id)
    }
}

impl Storage for FsStorage {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.active_file.seek(SeekFrom::Start(self.position as u64))?;
        let mut payload = Vec::with_capacity(KEY_SIZE + VAL_SIZE + key.len() + val.len()); // key_size + val_size + key+val

        payload.put_u32(key.len() as u32);
        payload.put_u32(val.len() as u32);
        payload.put(key);
        payload.put(val);

        let ts_tamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.active_file.write_all(&payload).context("file write failed")?;
        // self.active_file.sync_data()?;
        self.key_dir.insert(key.to_vec(), Header {
            file_id: self.active_file_id,
            val_size: val.len() as u32,
            val_offset: self.position + (KEY_SIZE + VAL_SIZE + key.len()) as u32,
            ts_tamp,
        });

        self.position += payload.len() as u32;

        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> where Self: Reader {
        match self.key_dir.get(key) {
            None => { Err(BitcaskError::NotFound.into()) }
            Some(h) => {
                Ok(self.read_val(h.file_id, h.val_offset, h.val_size)?)
            }
        }
    }
}

impl Reader for FsStorage {
    fn read_val(&mut self, file_id: u64, offset: u32, size: u32) -> Result<Vec<u8>> {
        /*let mut file = OpenOptions::new()
            .read(true)
            .open(self.dir.join(FsStorage::make_filename(file_id)))?; //1698702893
*/
        self.read_file.seek(SeekFrom::Start(offset as u64))?;

        let mut buf = vec![0u8; size as usize];
        self.read_file.read_exact(&mut buf)?;
        // TODO: get Data struct for check expirity, CRC.

        Ok(buf)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    use tempdir::TempDir;

    use crate::storage::{KEY_OFFSET, KEY_SIZE, KEY_SIZE_OFFSET, Storage, VAL_SIZE, VAL_SIZE_OFFSET};

    use super::FsStorage;

    #[test]
    fn it_should_load_cask_from_file() {
        let dir = TempDir::new("bitcask-").unwrap();
        let cask_result = FsStorage::load(dir.path().to_str().unwrap());

        assert!(cask_result.is_ok());
        assert_eq!(0, cask_result.unwrap().position);
    }

    #[test]
    fn it_should_put_to_file() {
        // given
        let dir = TempDir::new("bitcask-").unwrap();
        let mut write_cask = FsStorage::load(dir.path().to_str().unwrap()).unwrap();
        let key = b"foo";
        let val = b"bar";

        // when
        write_cask.put(key, val).unwrap();

        // then
        let mut payload = vec![0; KEY_SIZE + VAL_SIZE + key.len() + val.len()]; // ksz + vsz + k + v

        let mut cask_file = File::open(Path::join(dir.path(), FsStorage::make_filename(write_cask.active_file_id))).unwrap();
        cask_file.seek(SeekFrom::Start(0)).unwrap();
        cask_file.read_exact(&mut payload).unwrap();

        // write_cask.active_file.seek(SeekFrom::Start(0)).unwrap();
        // write_cask.active_file.read_exact(&mut payload).unwrap();

        assert_eq!(u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()), key.len() as u32);
        assert_eq!(u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()), val.len() as u32);
        let val_offset = KEY_OFFSET + key.len();
        assert_eq!(payload[KEY_OFFSET..KEY_OFFSET + key.len()], *key.as_slice());
        assert_eq!(payload[val_offset..val_offset + val.len()], *val.as_slice());
        dbg!(write_cask.key_dir.get(&key.to_vec()).unwrap());

        let header = write_cask.key_dir.get(key.as_slice()).unwrap();
        assert_eq!(header.file_id, write_cask.active_file_id);
        assert_eq!(header.val_size, val.len() as u32);
        assert_eq!(header.val_offset, (KEY_SIZE + VAL_SIZE + key.len()).try_into().unwrap());
        assert_eq!(write_cask.position, (KEY_SIZE + VAL_SIZE + key.len() + val.len()).try_into().unwrap());
    }

    #[test]
    fn it_should_get() {
        // given
        let dir = TempDir::new("bitcask-").unwrap();
        let mut cask = FsStorage::load(dir.path().to_str().unwrap()).unwrap();

        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"val1"),
            (b"key2", b"val2"),
            (b"key1", b"val3"),
        ];

        for (key, val) in pairs {
            cask.put(key, val).unwrap();

            let actual = cask.get(key).unwrap();
            assert_eq!(val, actual.as_slice());
        }
    }
}