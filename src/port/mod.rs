pub enum Endian {
    Little,
    Big,
}

// #[cfg(port = "posix")]
// TODO ADD cfg for ports
pub const ENDIANNESS: Endian = Endian::Little;
