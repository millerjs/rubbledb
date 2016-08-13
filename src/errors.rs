use std::io;
use std::num::ParseIntError;

pub type RubbleResult<T> = Result<T, RubbleError>;

quick_error! {
    #[derive(Debug)]
    pub enum RubbleError {
        ParseIntError(err: ParseIntError) {
            from()
        }
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        Other(descr: &'static str) {
            description(descr)
            display("Error {}", descr)
        }
        Discard {
            from(&'static str)
        }
    }
}
