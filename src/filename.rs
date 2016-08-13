// use slice;
// use status;

use ::errors::RubbleResult;
use ::util;

pub struct Env;

pub struct FileNameDetails {
    pub number: u64,
    pub file_type: FileType,
}

pub enum FileType {
    LogFile,
    DBLockFile,
    TableFile,
    DescriptorFile,
    CurrentFile,
    TempFile,
    InfoLogFile,  // Either the current one, or an old one
}

// extern std::string LogFileName(const std::string& dbname, uint64_t number);


pub fn make_file_name(name: &str, number: u64, suffix: &str) -> String
{
    format!("{}/{:06}.{}", name, number, suffix)
}

/// Return the name of the log file with the specified number
/// in the db named by "dbname".  The result will be prefixed with
/// "dbname".
pub fn log_file_name(name: &str, number: u64) -> String
{
    assert!(number > 0);
    make_file_name(name, number, "log")
}

/// Return the name of the sstable with the specified number
/// in the db named by "dbname".  The result will be prefixed with
/// "dbname".
pub fn table_file_name(name: &str, number: u64) -> String
{
    assert!(number > 0);
    return make_file_name(name, number, "ldb");
}


/// Return the legacy file name for an sstable with the specified number
/// in the db named by "dbname". The result will be prefixed with
/// "dbname".
fn ssttable_file_name(name: &str, number: u64) -> String
{
    assert!(number > 0);
    make_file_name(name, number, "sst")
}

/// Return the name of the descriptor file for the db named by
/// "dbname" and the specified incarnation number.  The result will be
/// prefixed with "dbname".
fn descriptor_file_name(dbname: &str, number: u64) -> String
{
    assert!(number > 0);
    format!("{}//MANIFEST-{:06}", dbname, number)
}

/// Return the name of the current file.  This file contains the name
/// of the current manifest file.  The result will be prefixed with
/// "dbname".
fn current_file_name(dbname: &str) -> String
{
    format!("{}/CURRENT", dbname)
}

/// Return the name of the lock file for the db named by
/// "dbname".  The result will be prefixed with "dbname".
fn lock_file_name(dbname: &str) -> String
{
    format!("{}/LOCK", dbname)
}

/// Return the name of a temporary file owned by the db named "dbname".
/// The result will be prefixed with "dbname".
fn temp_file_name(dbname: &str, number: u64) -> String
{
    assert!(number > 0);
    make_file_name(dbname, number, "dbtmp")
}

/// Return the name of the info log file for "dbname".
fn info_log_file_name(dbname: &str) -> String
{
    format!("{}/LOG", dbname)
}

/// Return the name of the old info log file for "dbname".
fn old_info_log_file_name(dbname: &str) -> String
{
    format!("{}/LOG.old", dbname)
}

/// If filename is a leveldb file, store the type of the file in *type.
/// The number encoded in the filename is stored in *number.  If the
/// filename was successfully parsed, returns true.  Else return false.
fn parse_file_name(fname: &str) -> RubbleResult<FileNameDetails>
{
    Ok(match fname {
        "CURRENT" => FileNameDetails {
            number: 0,
            file_type: FileType::CurrentFile,
        },
        "LOCK" => FileNameDetails {
            number: 0,
            file_type: FileType::DBLockFile,
        },
        "LOG" | "LOG.old" => FileNameDetails {
            number: 0,
            file_type: FileType::InfoLogFile,
        },
        name if name.starts_with("MANIFEST-") => {
            FileNameDetails {
                number: try!(util::coding::parse_u64(&fname["MANIFEST-".len()..])).number,
                file_type: FileType::DescriptorFile,
            }
        }
        _ => {
            let parsed = try!(util::coding::parse_u64(fname));
            let suffix = &fname[parsed.offset..];
            FileNameDetails {
                number: parsed.number,
                file_type: match suffix {
                    ".log"          => FileType::LogFile,
                    ".sst" | ".ldb" => FileType::TableFile,
                    ".dbtmp"        => FileType::LogFile,
                    _               => return Err("unknown file type".into()),
                }
            }
        }
    })
}

// /// TODO
// ///
// /// Make the CURRENT file point to the descriptor file with the
// /// specified number.
// fn set_current_file(env: &mut Env, dbname: &str, descriptor_number: u64) -> Status
// {
// }
