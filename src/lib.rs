use std::{
    fs::{self, File},
    path::Path,
};

mod db;
mod log_file;

use integer_encoding::VarInt;

pub fn new_file_log(directory: String) -> File {
    let path = Path::new(&directory);
    fs::create_dir_all(path).unwrap();
    let log_path = path.join("000001.log"); // TODO number is variable
    File::create(log_path).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put() {
        let mut v: Vec<u8> = Vec::new();
        put(&[1, 2, 3], &[9, 10], &mut v);
        assert_eq!(&[3, 1, 2, 3, 2, 9, 10], v.as_slice());
    }
}
