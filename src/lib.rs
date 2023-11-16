use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use crc::{Crc, CRC_32_ISCSI};

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
enum RecordType {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
}
#[derive(Debug)]
struct Record {
    checksum: u32,           // crc32c of type and data[] ; little-endian
    length: u16,             // little-endian
    record_type: RecordType, // One of FULL, FIRST, MIDDLE, LAST
    data: Box<[u8]>,
}

impl Record {
    fn new(data: &[u8], record_type: RecordType) -> Self {
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

    fn encode_to_file(&self, f: &mut dyn Write) {
        f.write(&self.checksum.to_le_bytes());
        f.write(&self.length.to_le_bytes());
        f.write(&[self.record_type as u8]);
        f.write(&self.data);
    }
}

struct RevelDB {
    directory: String,
    log_file: File,
}

impl RevelDB {
    fn new(directory: String) -> Self {
        let log_path = Path::new(&directory).join("000001.log"); // TODO number is variable
        Self {
            directory,
            log_file: File::create(log_path).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode() {
        let r = Record::new(&[1, 4, 9, 16], RecordType::FULL);
        let encoded = r.encode();
        assert_eq!(encoded.len(), 11)
    }
}
