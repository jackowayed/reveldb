use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::Path,
};

use crate::log_file::{Record, LOG_RECORD_HEADER_SIZE};

pub struct RevelDB {
    log_file: File,
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
            log_file,
            directory: directory.to_path_buf().into_boxed_path(),
            memtable: HashMap::new(),
        };
        // RECOVERY
        loop {
            let mut record_header = [0u8; LOG_RECORD_HEADER_SIZE];
            if let Err(e) = log_file.read_exact(&mut record_header) {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e);
            }
            // TODO need to do a lot more Record logic:
            //    test checksum, handle multi-record payloads
            let length = u16::from_le_bytes([record_header[4], record_header[5]]);
            //let mut content = [0u8; length];
            let mut content: Vec<u8> = vec![0u8; length as usize];
            log_file.read_exact(&mut content)?;
            // TODO varint
            // TODO has to be a better way re int types
            let key_length = content[0] as usize;
            let value_offset = 1 + key_length;
            let value_length = content[value_offset] as usize;
            assert_eq!(length as usize, 1 + 1 + key_length + value_length);
        }
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
        let r = Record::new(v, crate::log_file::RecordType::FULL);
        self.log_file.write(&v)?;
        self.memtable.insert(key.to_vec(), v);
        Ok(())
    }
}
