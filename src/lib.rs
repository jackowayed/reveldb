use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use crc::{Crc, CRC_32_ISCSI};
use integer_encoding::VarInt;

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum RecordType {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
}
#[derive(Debug)]
pub struct Record {
    checksum: u32,           // crc32c of type and data[] ; little-endian
    length: u16,             // little-endian
    record_type: RecordType, // One of FULL, FIRST, MIDDLE, LAST
    data: Box<[u8]>,
}

impl Record {
    pub fn new(data: &[u8], record_type: RecordType) -> Self {
        let length = data.len() as u16;
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&length.to_le_bytes());
        digest.update(data);
        Record {
            checksum: digest.finalize(),
            length,
            record_type,
            data: Box::from(data),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut ret = Vec::new();
        self.encode_to_file(&mut ret);
        ret
    }

    pub fn encode_to_file(&self, f: &mut impl Write) {
        f.write(&self.checksum.to_le_bytes());
        f.write(&self.length.to_le_bytes());
        f.write(&[self.record_type as u8]);
        f.write(&self.data);
    }
}

fn put(key: &[u8], value: &[u8], f: &mut impl Write) {
    // TODO imlement varint for thi to work with sizes > 127
    f.write(&[key.len() as u8]);
    f.write(key);
    f.write(&[value.len() as u8]);
    f.write(value);
}

pub fn newFileLog(directory: String) -> File {
    let path = Path::new(&directory);
    fs::create_dir_all(path).unwrap();
    let log_path = path.join("000001.log"); // TODO number is variable
    File::create(log_path).unwrap()
}

#[cfg(test)]
mod tests {
    use crate::put;

    use super::*;

    #[test]
    fn encode() {
        let r = Record::new(&[1, 4, 9, 16], RecordType::FULL);
        let encoded = r.encode();
        assert_eq!(encoded.len(), 11)
    }

    #[test]
    fn test_put() {
        let mut v: Vec<u8> = Vec::new();
        put(&[1, 2, 3], &[9, 10], &mut v);
        assert_eq!(&[3, 1, 2, 3, 2, 9, 10], v.as_slice());
    }
}
