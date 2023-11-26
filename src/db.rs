use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::Path,
};

use crate::log_file::{LogFile, Record};

pub struct RevelDB {
    log_file: LogFile,
    directory: Box<Path>,
    memtable: HashMap<Vec<u8>, Vec<u8>>,
}

// TODO should be a pattern or use MANIFEST or something
const LOG_PATH: &str = "000001.log";

impl RevelDB {
    fn new(directory: &Path) -> std::io::Result<Self> {
        let log_path = directory.join(LOG_PATH);
        let mut log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_path)?;
        let mut db = Self {
            log_file: LogFile::new(log_file),
            directory: directory.to_path_buf().into_boxed_path(),
            memtable: HashMap::new(),
        };
        // RECOVERY
        db.log_file.recover()?;
        Ok(db)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        // TODO implement varint for this to work with sizes > 127
        let mut v: Vec<u8> = Vec::new();
        // TODO varint
        v.extend_from_slice(&[key.len() as u8]);
        v.extend_from_slice(key);
        v.extend_from_slice(&[value.len() as u8]);
        v.extend_from_slice(value);
        // TODO we're copying this data too many times. I think I can do lifetimes
        // on Records because they're always shortlived.
        // Or maybe I just have a RecordHeader plus a borrowed slice?
        //let r = Record::new(v, crate::log_file::RecordType::FULL);
        self.log_file.write(&v)?;
        // TODO varint
        // Change v from the full payload we store in the LogFile to just value
        v.drain(0..1 + 1 + key.len());
        self.memtable.insert(key.to_vec(), v);
        Ok(())
    }
}
