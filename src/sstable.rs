use std::io::Read;

use integer_encoding::VarInt;

use crate::varint;

struct BlockHandle {
    offset: usize,
    size: usize,
}

impl BlockHandle {
    pub fn new<R>(mut reader: &mut R) -> Self
    where
        R: Read,
    {
        let offset = varint::read_varint(&mut reader).unwrap();
        let size = varint::read_varint(&mut reader).unwrap();
        Self { offset, size }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let required_space = self.offset.required_space() + self.size.required_space();
        let mut r = vec![0u8; required_space];
        let offset_bytes = self.offset.encode_var(r.as_mut_slice());
        self.size.encode_var(&mut r.as_mut_slice()[offset_bytes..]);
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() {
        let bh = BlockHandle {
            offset: 6,
            size: 150,
        };
        assert_eq!(vec![0b00000110u8, 0b10010110, 0b00000001], bh.serialize());
    }
}
