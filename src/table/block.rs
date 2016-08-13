use ::slice::Slice;
use ::errors::RubbleResult;
use ::util::coding;
use ::status::Status;
use std::mem;
use std::str;

pub struct OwnedBlock {
    data: Vec<u8>,
    restart_offset: usize,
}

pub struct SliceBlock<'a> {
    data: Slice<'a>,
    restart_offset: usize,
}

pub trait Block {
    fn get_size(&self) -> usize;
    fn data(&self) -> Slice;
    fn restart_offset(&self) -> usize;

    fn iter<'a, T: SliceComparator>(&'a self, comparator: T) -> BlockIterator<'a, T>;

    fn num_restarts(data: Slice) -> usize
    {
        assert!(data.len() >= mem::size_of::<u32>());
        let offset = data.len() - mem::size_of::<u32>();
        coding::decode_fixed32(&data[offset..]) as usize
    }

    fn iter_slice<'a, T: SliceComparator>(&'a self, comparator: T, slice: Slice<'a>) -> BlockIterator<'a, T>
    {
        if self.get_size() < mem::size_of::<u32>() {
            BlockIterator::new(comparator, &[], 0, 0)
                .with_status(Status::Corruption("bad block contents".into()))
        } else {
            let num_restarts = Self::num_restarts(slice);
            if num_restarts == 0 {
                BlockIterator::new(comparator, &[], 0, 0)
            } else {
                let restart_offset = self.restart_offset();
                BlockIterator::new(comparator, slice, restart_offset, num_restarts)
            }
        }
    }

}

impl Block for OwnedBlock {
    fn get_size(&self) -> usize { self.data.len() }
    fn data(&self) -> Slice { &self.data }
    fn restart_offset(&self) -> usize { self.restart_offset }

    fn iter<'a, T: SliceComparator>(&'a self, comparator: T) -> BlockIterator<'a, T>
    {
        self.iter_slice(comparator, self.data.as_slice())
    }
}


impl<'a> Block for SliceBlock<'a> {
    fn get_size(&self) -> usize { self.data.len() }
    fn data(&self) -> Slice { self.data }
    fn restart_offset(&self) -> usize { self.restart_offset }

    fn iter<'i, T: SliceComparator>(&'i self, comparator: T) -> BlockIterator<'i, T>
    {
        self.iter_slice(comparator, self.data)
    }
}


impl OwnedBlock {
    fn new(contents: Slice) -> RubbleResult<OwnedBlock>
    {
        let sizeof_u32 = mem::size_of::<u32>();
        let max_restarts_allowed = (contents.len() - sizeof_u32) / sizeof_u32;
        let num_restarts = Self::num_restarts(contents);

        if num_restarts > max_restarts_allowed {
            return Err("The size is too small for num_restarts()".into())
        }

        Ok(OwnedBlock {
            data: contents.to_vec(),
            restart_offset: contents.len() - (1 + num_restarts) * sizeof_u32,
        })
    }
}

impl<'a> SliceBlock<'a> {
    fn get_size(&self) -> usize { self.data.len() }
}

struct DecodedEntry<'a> {
    new_slice: Slice<'a>,
    shared: u32,
    non_shared: u32,
    value_length: u32,
}

/// Helper routine: decode the next block entry starting at "p",
/// storing the number of shared key bytes, non_shared key bytes,
/// and the length of the value in "*shared", "*non_shared", and
/// "*value_length", respectively.  Will not dereference past "limit".
///
/// If any errors are detected, returns NULL.  Otherwise, returns a
/// pointer to the key delta (just past the three decoded values).
fn decode_entry(mut p: &[u8]) -> RubbleResult<DecodedEntry>
{
    if p.len() < 3 {
        return Err("Entry missing header!".into())
    };

    let mut cur = 0;
    let mut shared = p[0] as u32;
    let mut non_shared = p[1] as u32;
    let mut value_length = p[2] as u32;

    if (shared | non_shared | value_length) < 128 {
        // Fast path: all three values are encoded in one byte each
        cur += 3;

    } else {
        let fallback = try!(coding::get_varint32_ptr_fallback(p));
        p = fallback.slice;
        shared = fallback.value;
        let fallback = try!(coding::get_varint32_ptr_fallback(p));
        p = fallback.slice;
        non_shared = fallback.value;
        let fallback = try!(coding::get_varint32_ptr_fallback(p));
        p = fallback.slice;
        value_length = fallback.value;
    }

    let new_slice = &p[cur..];

    if new_slice.len() < (non_shared + value_length) as usize {
        return Err("bad block?".into());
    }

    return Ok(DecodedEntry {
        new_slice: new_slice,
        shared: shared,
        non_shared: non_shared,
        value_length: value_length,
    });
}

pub trait SliceComparator {
    fn compare(&self, a: Slice, b: Slice) -> i32;
}

pub struct BlockIterator<'a, T: SliceComparator> {
    comparator: T,
    data: Slice<'a>,
    value_offset: usize,
    value_len: usize,
    restarts: usize,
    num_restarts: usize,
    current: usize,
    restart_index: usize,
    key: String,
    status: Status,
}

impl<'a, T: SliceComparator> BlockIterator<'a, T> {
    pub fn new(comparator: T, data: Slice<'a>, restarts: usize, num_restarts: usize)
               -> BlockIterator<'a, T>
    {
        assert!(num_restarts > 0);
        BlockIterator::<'a, T> {
            key: String::new(),
            status: Status::Ok,
            value_offset: 0,
            value_len: 0,
            comparator: comparator,
            data: data,
            restarts: restarts,
            num_restarts: num_restarts,
            current: restarts,
            restart_index: num_restarts,
        }
    }

    fn with_status(mut self, status: Status) -> BlockIterator<'a, T>
    {
        self.status = status;
        self
    }

    fn compare(&self, a: Slice, b: Slice) -> i32
    {
        self.comparator.compare(a, b)
    }

    /// Return the offset in data_ just past the end of the current entry.
    fn next_entry_offset(&self) -> usize
    {
        self.value_offset + self.value_len
    }

    fn get_restart_point(&self, index: usize) -> usize
    {
        assert!(index < self.num_restarts);
        let offset = self.restarts + index * mem::size_of::<u32>();
        coding::decode_fixed32(&self.data[offset..]) as usize
    }

    pub fn seek_to_restart_point(&mut self, index: usize)
    {
        self.key = String::new();
        self.restart_index = index;
        // current_ will be fixed by ParseNextKey();

        // ParseNextKey() starts at the end of value_, so set value_ accordingly
        self.value_offset = self.get_restart_point(index);
    }

    pub fn is_valid(&self) -> bool
    {
        self.current < self.restarts
    }

    pub fn status(&self) -> &Status {
        &self.status
    }

    pub fn key(&self) -> String {
        assert!(self.is_valid());
        self.key.clone()
    }

    pub fn value(&self) -> Slice {
        assert!(self.is_valid());
        &self.data[self.value_offset..self.value_offset+self.value_len]
    }

    pub fn step(&mut self) {
        assert!(self.is_valid());
        self.parse_next_key();
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());

        // Scan backwards to a restart point before current_
        let original = self.current;

        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entries
                self.current = self.restarts;
                self.restart_index = self.num_restarts;
                return;
            }
            self.restart_index -= 1;
        }
    }

    pub fn seek(&mut self, target: Slice)
    {
        // Binary search in restart array to find the last restart point
        // with a key < target
        let mut left = 0;
        let mut right = self.num_restarts - 1;

        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);

            // let shared, non_shared, value_length;

            let entry = match decode_entry(&self.data[region_offset as usize..]) {
                Err(_) => return self.corruption_error(),
                Ok(key) => key,
            };

            if entry.shared != 0 {
                return self.corruption_error()
            }

            let mid_key = entry.new_slice;

            if self.compare(mid_key, target) < 0 {
                // Key at "mid" is smaller than "target".  Therefore all
                // blocks before "mid" are uninteresting.
                left = mid;
            } else {
                // Key at "mid" is >= "target".  Therefore all blocks at or
                // after "mid" are uninteresting.
                right = mid - 1;
            }

        }

        // Linear search (within restart block) for first key >= target
        self.seek_to_restart_point(left);

        loop {
            if !self.parse_next_key() {
                return;
            }
            if self.compare(self.key.as_bytes(), target) >= 0 {
                return;
            }
        }

    }

    pub fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    pub fn seek_to_last(&mut self) {
        let n_restarts = self.num_restarts - 1;
        self.seek_to_restart_point(n_restarts);
        while self.parse_next_key() && self.next_entry_offset() < self.restarts {
            // Keep skipping
        }
    }

    fn corruption_error(&mut self) {
        self.current = self.restarts;
        self.restart_index = self.num_restarts;
        self.status = Status::Corruption("bad entry in block".into());
        self.key = String::new();
    }

    fn parse_next_key(&mut self) -> bool {
        self.current = self.next_entry_offset();
        let p = &self.data[self.current..];

        if p.len() == 0 {
            // No more entries to return.  Mark as invalid.
            self.current = self.restarts;
            self.restart_index = self.num_restarts;
            return false;
        }

        let entry = match decode_entry(p) {
            Ok(p) => p,
            _ => {
                self.corruption_error();
                return false;
            }
        };

        if self.key.len() < entry.shared as usize {
            self.corruption_error();
            return false;
        }

        self.key = str::from_utf8(&entry.new_slice[..entry.non_shared as usize])
            .expect("Invalid UTF-8 key")
            .to_owned();

        self.value_offset = entry.non_shared as usize;
        self.value_len = entry.value_length as usize;

        while self.restart_index + 1 < self.num_restarts
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1;
        }

        true
    }
}

pub struct KVEntry {
    key: String,
    value: Vec<u8>,
}

impl<'a, T: SliceComparator> Iterator for BlockIterator<'a, T> {
    // we will be counting with usize
    type Item = KVEntry;

    fn next(&mut self) -> Option<KVEntry> {
        self.step();
        match self.num_restarts {
            0 => None,
            _ => Some(KVEntry {
                key: self.key(),
                value: self.value().to_vec(),
            })
        }
    }
}
