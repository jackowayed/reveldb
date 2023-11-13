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
    checksum: u32,     // crc32c of type and data[] ; little-endian
    length: u16,       // little-endian
    record_type: RecordType,   // One of FULL, FIRST, MIDDLE, LAST
    data: Box<[u8]>
  
}

impl Record {
    fn new(data: &[u8], record_type: RecordType) -> Self {
        Record {checksum: 1, // TODO
            length: data.len() as u16,
            record_type,
            data: Box::from(data)}
    }

    fn encode(&self) -> Vec<u8> {
        let mut ret = Vec::new();
        ret.extend(self.checksum.to_le_bytes());
        ret.extend(self.length.to_le_bytes());
        ret.push(self.record_type as u8);
        ret.extend_from_slice(&self.data);
        ret
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
