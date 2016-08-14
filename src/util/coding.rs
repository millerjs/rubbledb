use ::slice::Slice;
use ::errors::RubbleResult;
use ::status::Status;
use ::port;
use regex::Regex;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use std::io::Cursor;

lazy_static! {
    static ref REGEX_U64: Regex = Regex::new(r"(\d+)(.*)").unwrap();
}

pub struct ParseU64Result {
    pub number: u64,
    pub offset: usize,
}

/// Parse a human-readable int from "*in" into *value.  On success,
/// advances "*in" past the consumed int and sets "*val" to the
/// numeric value.  Otherwise, returns false and leaves *in in an
/// unspecified state.
pub fn parse_u64(text: &str) -> RubbleResult<ParseU64Result>
{
    match REGEX_U64.captures(text).and_then(|c| c.at(0)) {
        Some(substring) => Ok(
            ParseU64Result{
                number: try!(substring.parse()),
                offset: substring.len(),
            }
        ),
        None => Err("No integer found in string".into()),
    }
}

pub struct FallbackResult<'a> {
    pub slice: Slice<'a>,
    pub value: u32,
}

/// I don't quite know, we're stepping through every 7 bytes in a
/// block and doing some fancy shifting of the byte at that offset
/// masked into a magic number...
pub fn get_varint32_ptr_fallback(p: Slice) -> RubbleResult<FallbackResult>
{
    let mut result = 0;
    let mut p = p;
    for shift in (0..5).map(|s| s * 7) {
        let byte = p[shift];
        p = &p[1..];
        if byte & 128 != 0 {
            // More bytes are present
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            return Ok(FallbackResult{ slice: p, value: result as u32})
        }
    }
    Err("missed the ptr fallback or something".into())
}

#[inline(always)]
pub fn decode_fixed32(slice: Slice) -> u32
{
    match port::ENDIANNESS {
        port::Endian::Little =>
            Cursor::new(slice[..4].to_vec()).read_u32::<LittleEndian>().unwrap(),
        port::Endian::Big =>
            Cursor::new(slice[..4].to_vec()).read_u32::<BigEndian>().unwrap(),
    }
}

#[inline(always)]
pub fn decode_fixed64(slice: Slice) -> u64
{
    match port::ENDIANNESS {
        port::Endian::Little =>
            Cursor::new(slice[..4].to_vec()).read_u64::<LittleEndian>().unwrap(),
        port::Endian::Big =>
            Cursor::new(slice[..4].to_vec()).read_u64::<BigEndian>().unwrap(),
    }
}

#[inline(always)]
pub fn put_fixed32(buff: &mut Vec<u8>, value: u32)
{
    match port::ENDIANNESS {
        port::Endian::Little => buff.write_u32::<LittleEndian>(value).unwrap(),
        port::Endian::Big => buff.write_u32::<BigEndian>(value).unwrap(),
    };
}

#[inline(always)]
pub fn put_fixed64(buff: &mut Vec<u8>, value: u64)
{
    match port::ENDIANNESS {
        port::Endian::Little => buff.write_u64::<LittleEndian>(value).unwrap(),
        port::Endian::Big => buff.write_u64::<BigEndian>(value).unwrap(),
    };
}

#[inline(always)]
pub fn put_varint32(buff: &mut Vec<u8>, v: u32) -> usize
{
    let b = 128;
    if v < 1 << 7 {
        buff.push(v as u8);
        1
    } else if v < 1 << 14 {
        buff.reserve(2);
        buff.push((v | b) as u8);
        buff.push((v >> 7) as u8);
        2
    } else if v < 1 << 21 {
        buff.reserve(3);
        buff.push((v | b) as u8);
        buff.push((v >> 7) as u8);
        buff.push((v >> 14) as u8);
        3
    } else if v < 1 << 28 {
        buff.reserve(4);
        buff.push((v | b) as u8);
        buff.push((v >> 7) as u8);
        buff.push((v >> 14) as u8);
        buff.push((v >> 21) as u8);
        4
    } else {
        buff.reserve(5);
        buff.push((v | b) as u8);
        buff.push((v >> 7) as u8);
        buff.push((v >> 14) as u8);
        buff.push((v >> 21) as u8);
        buff.push((v >> 28) as u8);
        5
    }
}

#[inline(always)]
pub fn put_varint64(buff: &mut Vec<u8>, mut v: u64) -> usize
{
    let b = 128;
    let mut bytes = 0;
    buff.reserve(10);
    while v >= b {
        buff.push(((v & (b-1)) | b) as u8);
        v >>= 7;
        bytes += 1;
    }
    bytes
}

// void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
//   PutVarint32(dst, value.size());
//   dst->append(value.data(), value.size());
// }

// int VarintLength(uint64_t v) {
//   int len = 1;
//   while (v >= 128) {
//     v >>= 7;
//     len++;
//   }
//   return len;
// }

// bool GetVarint32(Slice* input, uint32_t* value) {
//   const char* p = input->data();
//   const char* limit = p + input->size();
//   const char* q = GetVarint32Ptr(p, limit, value);
//   if (q == NULL) {
//     return false;
//   } else {
//     *input = Slice(q, limit - q);
//     return true;
//   }
// }

/// Returns (remaining slice, u64 result)
pub fn get_varint64(slice: Slice) -> RubbleResult<(Slice, u64)>
{
    let mut result: u64 = 0;
    let mut p = 0;
    for shift in (0..10).map(|n| n*7) {
        if p >= slice.len() { break }
        let byte = slice[p] as u64;
        p += 1;
        if byte & 128 != 0 {
            // More bytes are present
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            return Ok((&slice[p..], result));
        }
    }
    Err(Status::IOError("Unable to read varin64".into()).into())
}


// const char* GetLengthPrefixedSlice(const char* p, const char* limit,
//                                    Slice* result) {
//   uint32_t len;
//   p = GetVarint32Ptr(p, limit, &len);
//   if (p == NULL) return NULL;
//   if (p + len > limit) return NULL;
//   *result = Slice(p, len);
//   return p + len;
// }

// bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
//   uint32_t len;
//   if (GetVarint32(input, &len) &&
//       input->size() >= len) {
//     *result = Slice(input->data(), len);
//     input->remove_prefix(len);
//     return true;
//   } else {
//     return false;
//   }
// }
