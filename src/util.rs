use ::errors::RubbleResult;
use regex::Regex;


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
    let regex = Regex::new(r"(\d+)(.*)").unwrap();
    match regex.captures(text).and_then(|c| c.at(0)) {
        None => Err("No int found".into()),
        Some(substring) => {
            Ok(ParseU64Result{
                number: try!(substring.parse()),
                offset: substring.len(),
            })
        },
    }
}
