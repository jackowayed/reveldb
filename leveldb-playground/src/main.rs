use std::fs;
use std::path::Path;
use leveldb;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::{Options,WriteOptions,ReadOptions};

fn main() {

  let path = Path::new("./dbs/basic");
  fs::create_dir_all(path).unwrap();
  let mut options = Options::new();
  options.create_if_missing = true;
  let mut database = match Database::open(path, options) {
      Ok(db) => { db },
      Err(e) => { panic!("failed to open database: {:?}", e) }
  };
  let write_opts = WriteOptions::new();
  match database.put(write_opts, 1, &[1]) {
      Ok(_) => { () },
      Err(e) => { panic!("failed to write to database: {:?}", e) }
  };
}
