use std::fs;
use std::io::{stderr, Write};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::BufMut;

use crate::storage::{Config, Header, KeyDir, utils};
use crate::storage::log::{CRC_OFFSET, CRC_SIZE, KEY_OFFSET, KEY_SIZE_OFFSET, TOMBSTONE_MARKER_CHAR, VAL_SIZE_OFFSET};
use crate::storage::utils::{build_data_file_name, open_file_for_write};

pub struct LogWriter<'a> {
    file_id: u64,
    file: fs::File,
    position: u32,
    conf: &'a Config,
    key_dir: Arc<RwLock<KeyDir>>,
    // ctx: &'a WriteContext,
}

impl<'a> LogWriter<'a> {
    pub fn new(conf: &'a Config, key_dir: Arc<RwLock<KeyDir>>) -> anyhow::Result<Self> {
        let file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let file = open_file_for_write(&conf.path, &build_data_file_name(file_id))?;

        Ok(LogWriter { file_id, file, conf, key_dir, position: 0 })
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        let header = self.write_content(key, val)?;
        self.key_dir.write().unwrap().insert(key.to_vec(), header);

        if self.position > self.conf.max_file_size {
            self.new_active_file()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.write_content(key, &[TOMBSTONE_MARKER_CHAR; 1]).context("key deletion failed")?;
        self.key_dir.write().unwrap().remove(key);
        Ok(())
    }

    fn write_content(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<Header> {
        /*
        dbg!(CRC_SIZE);
        dbg!(TS_SIZE);
        dbg!(KEY_SIZE);
        dbg!(KEY_SIZE_OFFSET, KEY_OFFSET);
        dbg!(VAL_SIZE, VAL_SIZE_OFFSET);
        dbg!(CRC_SIZE + TS_SIZE + KEY_SIZE + VAL_SIZE);
        */

        let ts_tamp = utils::timestamp();
        let entry_bytes = create_entry(key, val, ts_tamp);
        let entry_start_pos = self.position;

        self.write_to_file(&entry_bytes)?;
        self.sync()?;

        let header = Header {
            file_id: self.file_id,
            val_size: val.len() as u32,
            val_offset: entry_start_pos + (KEY_OFFSET + key.len()) as u32,
            ts_tamp,
        };

        /*if key == &[107, 95, 50] {
            println!("{:?}", header);
            debug_entry(&entry_bytes);
        }*/

        // println!("{:?}", header);
        // debug_entry(&entry_bytes);

        Ok(header)
    }


    fn new_active_file(&mut self) -> anyhow::Result<()> {
        let new_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let new_filename = build_data_file_name(new_file_id);

        self.file.sync_all()?;
        self.file = open_file_for_write(&self.conf.path, &new_filename)?;
        self.file_id = new_file_id;
        self.position = 0;

        Ok(())
    }


    #[inline]
    fn sync(&mut self) -> anyhow::Result<()> {
        // TODO: we can create flush_on_put config for flushing after puts.
        // flushing content can be not needed for some use cases?
        self.file.flush()?;
        if self.conf.sync_on_put {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn write_to_file(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        self.file.write_all(buf).context("file write failed")?;
        self.position += buf.len() as u32;

        Ok(())
    }
}

impl Drop for LogWriter<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.file.sync_all() {
            write!(stderr(), "error while closing active file: {:?}", e).expect("error writing to stderr");
        }
    }
}

fn create_entry(key: &[u8], val: &[u8], ts_tamp: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(KEY_OFFSET + key.len() + val.len());

    payload.put_u32(0); // empty space for crc
    payload.put_u32(ts_tamp);
    payload.put_u32(key.len() as u32);
    payload.put_u32(val.len() as u32);
    payload.put(key);
    payload.put(val);

    let checksum = crc32fast::hash(&payload[CRC_OFFSET + CRC_SIZE..]);
    payload.splice(0..CRC_SIZE, checksum.to_be_bytes());

    payload
}


fn debug_entry(payload: &[u8]) {
    let key_size = u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()) as usize;
    let val_size = u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()) as usize;

    let key = &payload[KEY_OFFSET..KEY_OFFSET + key_size];

    let val_offset = KEY_OFFSET + key_size;
    let val = &payload[val_offset..val_offset + val_size];
    if val == &[8] {
        println!("DeleteEntry<key={}>", std::str::from_utf8(&key).unwrap())
    } else {
        println!("PutEntry<key={}, val={}>", std::str::from_utf8(&key).unwrap(), std::str::from_utf8(&val).unwrap())
    }
}


#[cfg(test)]
mod test {
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::{Arc, RwLock};

    use tempdir::TempDir;

    use crate::storage::{Config, utils};
    use crate::storage::log_reader::LogReader;

    use super::{CRC_OFFSET, CRC_SIZE, KEY_OFFSET, KEY_SIZE_OFFSET, LogWriter, VAL_SIZE_OFFSET};

    #[test]
    fn it_should_create_new_log() {
        let conf = Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        };

        let writer = LogWriter::new(&conf, Default::default()).unwrap();

        assert_eq!(0, writer.position);
        assert_ne!(0, writer.file_id);
    }


    #[test]
    fn it_should_write_to_file() {
        // given
        let dir = TempDir::new("bitcask-").unwrap().into_path();
        let conf = Config {
            path: dir.clone(),
            ..Default::default()
        };

        let mut writer = LogWriter::new(&conf, Default::default()).unwrap();

        let key = b"foo";
        let val = b"bar";

        // when
        writer.put(key, val).unwrap();

        // then
        let mut payload = vec![0; KEY_OFFSET + key.len() + val.len()];

        // let mut cask_file = File::open(Path::join(dir.path(), FsStorage::make_filename(writer.active_file_id))).unwrap();
        let filename = format!("{}.bitcask.data", writer.file_id);
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

        let key_dir = writer.key_dir.read().unwrap();
        let header = key_dir.get(key.as_slice()).unwrap();
        assert_eq!(header.file_id, writer.file_id);
        assert_eq!(header.val_size, val.len() as u32);
        assert_eq!(header.val_offset, (KEY_OFFSET + key.len()).try_into().unwrap());
        assert_eq!(writer.position, (KEY_OFFSET + key.len() + val.len()).try_into().unwrap());
    }


    #[test]
    fn it_should_get() {
        // TODO: move to integration test
        // given
        let conf = Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        };

        let key_dir = Arc::new(RwLock::new(Default::default()));
        let mut writer = LogWriter::new(&conf, key_dir.clone()).unwrap();
        let reader = LogReader::new(&conf.path, writer.file_id).unwrap();

        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"val1"),
            (b"key2", b"val2"),
            (b"key1", b"val3"),
        ];

        for (key, val) in pairs {
            writer.put(key, val).unwrap();
            let key_dir_guard = key_dir.read().unwrap();
            let actual = reader.read(key_dir_guard.get(key).unwrap().val_offset, key_dir_guard.get(key).unwrap().val_size).unwrap();
            assert_eq!(val, actual.as_slice());
        }
    }


    #[test]
    fn it_should_delete() {
        // TODO: move to integration test
        // write a unit test for here

        // given
        let conf = Config {
            path: TempDir::new("bitcask-").unwrap().into_path(),
            ..Default::default()
        };

        let key_dir = Arc::new(RwLock::new(Default::default()));
        let mut writer = LogWriter::new(&conf, key_dir.clone()).unwrap();
        let reader = LogReader::new(&conf.path, writer.file_id).unwrap();

        let key = b"k1";

        // when
        writer.put(key, b"val1").unwrap();

        // then
        assert!(writer.delete(key).is_ok());

        let key_dir_guard = key_dir.read().unwrap();
        assert!(key_dir_guard.get(key.as_slice()).is_none());
    }
}