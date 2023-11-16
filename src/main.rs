use reveldb::{Record, RecordType, RevelDB};

fn main() {
    let mut db = RevelDB::new("dbs/demo/".to_owned());
    let record = Record::new(&[1], RecordType::FULL);
    record.encode_to_file(&mut db)
}
