use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Status {
    Ok,
    NotFound(String),
    Corruption(String),
    NotSupported(String),
    InvalidArgument(String),
    IOError(String),
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Status error: {:?}", self)
    }
}

impl error::Error for Status {
    fn description(&self) -> &str {
        match *self {
            Status::Ok => "no error",
            Status::NotFound(ref s) => &*s,
            Status::Corruption(ref s) => &*s,
            Status::NotSupported(ref s) => &*s,
            Status::InvalidArgument(ref s) => &*s,
            Status::IOError(ref s) => &*s,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}
