use std::{collections::BTreeMap, fs::OpenOptions, io::Write, path::Path};

use crate::log_file::LogFile;

pub struct RevelDB {
    log_file: LogFile,
    directory: Box<Path>,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
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
            memtable: BTreeMap::new(),
        };
        db.log_file.recover(&mut db.memtable)?;
        Ok(db)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
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

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.memtable.get(key).map(|vec| vec.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    const KEY1: [u8; 3] = [65, 66, 67];
    const KEY2: [u8; 1] = [1];
    const KEY3: [u8; 2] = [5, 6];

    const VAL1: [u8; 8] = [1, 2, 3, 4, 5, 5, 5, 9];
    const VAL2: [u8; 1] = [17];
    const VAL3: [u8; 2] = [22, 22];

    #[test]
    fn test_put() -> std::io::Result<()> {
        let td = TempDir::new("tmp-test_put")?;
        let mut db = RevelDB::new(td.path())?;
        db.put(&KEY1, &VAL1)?;
        assert_eq!(Some(VAL1.as_slice()), db.get(&KEY1));
        db.put(&KEY1, &VAL2)?;
        assert_eq!(Some(VAL2.as_slice()), db.get(&KEY1));
        Ok(())
    }

    #[test]
    fn test_recovery() -> std::io::Result<()> {
        let td = TempDir::new("tmp-test_recovery")?;
        {
            let mut db = RevelDB::new(td.path())?;
            db.put(&KEY1, &VAL1)?;
            db.put(&KEY2, &VAL2)?;
            db.put(&KEY1, &VAL3)?;
        }
        let mut db = RevelDB::new(td.path())?;
        assert_eq!(VAL3, db.get(&KEY1).unwrap());
        assert_eq!(VAL2, db.get(&KEY2).unwrap());
        Ok(())
    }
}
