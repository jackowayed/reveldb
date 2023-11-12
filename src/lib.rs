#[repr(u8)]
enum RecordType {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
}
struct Record {
    checksum: u32,     // crc32c of type and data[] ; little-endian
    length: u16,       // little-endian
    record_type: RecordType,   // One of FULL, FIRST, MIDDLE, LAST
    data: Box<[u8]>
  
}

impl Record {
    fn new(data: &[u8], record_type: RecordType) -> Self {
        Record {checksum: 0, // TODO
            length: data.len() as u16,
            record_type,
            data: Box::from(data)}
    }
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
