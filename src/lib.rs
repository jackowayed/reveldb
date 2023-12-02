use std::{
    fs::{self, File},
    path::Path,
};

mod db;
mod log_file;
mod varint;

pub fn new_file_log(directory: String) -> File {
    let path = Path::new(&directory);
    fs::create_dir_all(path).unwrap();
    let log_path = path.join("000001.log"); // TODO number is variable
    File::create(log_path).unwrap()
}
