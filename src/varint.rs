use std::io::Read;

use integer_encoding::VarInt;

pub fn read_varint<R>(reader: &mut R) -> Option<usize>
where
    R: Read,
{
    let mut buf = [0u8; 10];
    for i in 1..buf.len() + 1 {
        match reader.read(&mut buf[i - 1..i]) {
            Ok(1) => {}
            _ => {
                return None;
            }
        }
        if let Some((num, _)) = usize::decode_var(&buf[..i]) {
            return Some(num);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        let mut eleven_msbs = [0b10000000u8; 11];
        assert_eq!(None, read_varint(&mut eleven_msbs.as_slice()));

        let mut one_byte = [0b00000110u8];
        assert_eq!(Some(6), read_varint(&mut one_byte.as_slice()));

        let mut one_byte_but_with_extras_in_slice = [0b00000110u8, 0b00000110u8];
        assert_eq!(
            Some(6),
            read_varint(&mut one_byte_but_with_extras_in_slice.as_slice())
        );

        // Same example here https://protobuf.dev/programming-guides/encoding/#varints
        let mut two_byte_150 = [0b10010110, 0b00000001];
        assert_eq!(Some(150), read_varint(&mut two_byte_150.as_slice()));

        //[0b10000001u8, ]
    }
}
