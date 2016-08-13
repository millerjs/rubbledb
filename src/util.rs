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
