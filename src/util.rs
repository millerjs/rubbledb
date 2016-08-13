use ::errors::RubbleResult;
use regex::Regex;

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
    pub slice: &'a [u8],
    pub value: u32,
}

/// I don't quite know, we're stepping through every 7 bytes in a
/// block and doing some fancy shifting of the byte at that offset
/// masked into a magic number...
pub fn get_varint32_ptr_fallback(p: &[u8]) -> RubbleResult<FallbackResult>
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
