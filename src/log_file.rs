use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, Write},
};

use crc::{Crc, CRC_32_ISCSI};

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
        self.encode_to_file(&mut ret).unwrap();
        ret
    }

    pub fn encode_to_file(&self, f: &mut impl Write) -> std::io::Result<usize> {
        // TODO mask checksum
        let mut bytes_written = 0;
        bytes_written += f.write(&self.checksum.to_le_bytes())?;
        bytes_written += f.write(&self.length.to_le_bytes())?;
        bytes_written += f.write(&[self.record_type as u8])?;
        bytes_written += f.write(&self.data)?;
        Ok(bytes_written)
    }
}

const LOG_BLOCK_SIZE: usize = 32 * 1024;
pub const LOG_RECORD_HEADER_SIZE: usize = 7;
pub struct LogFile {
    f: File,
}

impl LogFile {
    pub fn new(f: File) -> Self {
        Self { f }
    }

    fn block_capacity(&mut self) -> usize {
        LOG_BLOCK_SIZE - self.block_cursor()
    }

    /// Subtract the header size
    fn logical_block_capacity(&mut self) -> usize {
        self.block_capacity() - LOG_RECORD_HEADER_SIZE
    }

    fn offset(&mut self) -> usize {
        self.f.stream_position().unwrap() as usize
    }

    fn block_cursor(&mut self) -> usize {
        self.offset() as usize % LOG_BLOCK_SIZE
    }

    fn start_new_block(&mut self) {
        assert!(self.block_capacity() < LOG_RECORD_HEADER_SIZE);
        while self.block_cursor() % LOG_BLOCK_SIZE != 0 {
            // TODO could avoid looping by hardcoding a 7-byte slice and then doing math to slice into it
            self.f.write(&[0]).unwrap();
        }
    }
    pub fn recover(&mut self) -> std::io::Result<()> {
        assert_eq!(0, self.offset());
        loop {
            let mut record_header = [0u8; LOG_RECORD_HEADER_SIZE];
            if let Err(e) = self.f.read_exact(&mut record_header) {
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
            self.f.read_exact(&mut content)?;
            // TODO varint
            // TODO has to be a better way re int types
            let key_length = content[0] as usize;
            let value_offset = 1 + key_length;
            let value_length = content[value_offset] as usize;
            assert_eq!(length as usize, 1 + 1 + key_length + value_length);
        }
        Ok(())
    }
}

impl Write for LogFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.block_capacity() < LOG_RECORD_HEADER_SIZE {
            self.start_new_block();
        }
        let mut offset = 0;
        let mut bytes_written = 0;
        while offset < buf.len() {
            let record_type = if offset == 0 && self.logical_block_capacity() >= buf.len() {
                RecordType::FULL
            } else if offset == 0 {
                RecordType::FIRST
            } else if self.logical_block_capacity() >= buf.len() - offset {
                RecordType::LAST
            } else {
                RecordType::MIDDLE
            };
            let end = std::cmp::min(offset + self.logical_block_capacity(), buf.len());
            let r = Record::new(&buf[offset..end], record_type);
            offset = end;
            bytes_written += r.encode_to_file(&mut self.f)?;
        }
        return Ok(bytes_written);
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.f.flush()
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn encode() {
        let r = Record::new(&[1, 4, 9, 16], RecordType::FULL);
        let encoded = r.encode();
        assert_eq!(encoded.len(), 11)
    }

    #[test]
    fn log_writer_record_chunking() {
        let tmp_dir = TempDir::new("testing").unwrap();
        let mut lf = LogFile {
            f: File::create(tmp_dir.path().join("log")).unwrap(),
        };
        let buf = [0u8; 3 * LOG_BLOCK_SIZE];

        let mut logical_written = 10;
        lf.write(&buf[..10]).unwrap();
        assert_eq!(logical_written + LOG_RECORD_HEADER_SIZE, lf.offset());

        logical_written += LOG_BLOCK_SIZE;
        lf.write(&buf[..LOG_BLOCK_SIZE]).unwrap();
        assert_eq!(
            logical_written + 3 * LOG_RECORD_HEADER_SIZE,
            lf.offset(),
            "This write will be broken into two records"
        );

        let almost_finish_block = lf.logical_block_capacity() - 5;
        logical_written += almost_finish_block;
        lf.write(&buf[..almost_finish_block]).unwrap();
        assert_eq!(logical_written + LOG_RECORD_HEADER_SIZE * 4, lf.offset());

        lf.write(&buf[..1]).unwrap();
        logical_written += 1;
        assert_eq!(
            logical_written + LOG_RECORD_HEADER_SIZE * 5 + 5,
            lf.offset(),
            "rest of block needs to be padded"
        );
    }
}
