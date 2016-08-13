/// BlockBuilder generates blocks where keys are prefix-compressed:
///
/// When we store a key, we drop the prefix shared with the previous
/// string.  This helps reduce the space requirement significantly.
/// Furthermore, once every K keys, we do not apply the prefix
/// compression and store the entire key.  We call this a "restart
/// point".  The tail end of the block stores the offsets of all of the
/// restart points, and can be used to do a binary search when looking
/// for a particular key.  Values are stored as-is (without compression)
/// immediately following the corresponding key.
///
/// An entry for a particular key-value pair has the form:
///     shared_bytes: varint32
///     unshared_bytes: varint32
///     value_length: varint32
///     key_delta: char[unshared_bytes]
///     value: char[value_length]
/// shared_bytes == 0 for restart points.
///
/// The trailer of the block has the form:
///     restarts: uint32[num_restarts]
///     num_restarts: uint32
/// restarts[i] contains the offset within the block of the ith restart point.

// #include "table/block_builder.h"

// #include <algorithm>
// #include <assert.h>
// #include "leveldb/comparator.h"
// #include "leveldb/table_builder.h"
// #include "util/coding.h"

use ::comparator::SliceComparator;
use ::slice::Slice;
use ::util::coding;
use std::mem;
use std::cmp;

pub struct Options {
    pub block_restart_interval: usize,
    pub comparator: Box<SliceComparator>,
}

pub struct BlockBuilder<'a> {
    options: &'a Options,

    buffer: Vec<u8>,

    /// Restart points
    restarts: Vec<u32>,

    /// Number of entries emitted since restart
    counter: usize,

    /// Has Finish() been called?
    finished: bool,

    last_key: Vec<u8>,
}

impl<'a> BlockBuilder<'a> {
    /// Reset the contents as if the BlockBuilder was just constructed.
    pub fn reset(&mut self)
    {
        self.buffer = vec![];
        self.restarts = vec![0];
        self.counter = 0;
        self.finished = false;
        self.last_key = vec![];
    }

    /// Return true iff no entries have been added since the last Reset()
    pub fn empty(&self) -> bool {
        self.buffer.len() == 0
    }

    pub fn new(options: &'a Options) -> BlockBuilder
    {
        assert!(options.block_restart_interval > =1);
        BlockBuilder {
            buffer: vec![],
            options: options,
            counter: 0,
            finished: false,
            restarts: vec![0],
            last_key: vec![],
        }
    }

    pub fn current_size_estimate(&self) -> usize
    {
        // Raw data buffer
        self.buffer.len()
        // Restart array
            + self.restarts.len() * mem::size_of::<u32>()
        // Restart array length
            + mem::size_of::<u32>()
    }

    pub fn finish(&mut self) -> Slice
    {
        // Append restart array
        for restart in &self.restarts {
            coding::put_fixed32(&mut self.buffer, *restart as u32);
        }
        coding::put_fixed32(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        self.buffer.as_slice()
    }

    /// REQUIRES: finish() has not been called since the last call to Reset().
    /// REQUIRES: key is larger than any previously added key
    fn add(&mut self, key: Slice, value: Slice)
    {
        let last_key_piece = &*self.last_key.clone();
        assert!(!self.finished);
        assert!(self.counter <= self.options.block_restart_interval);
        assert!(self.buffer.len() == 0 // No values yet?
               || self.options.comparator.compare(key, last_key_piece) > 0);

        let mut shared = 0;
        if self.counter < self.options.block_restart_interval {
            // See how much sharing to do with previous string
            let min_length = cmp::min(last_key_piece.len(), key.len());
            while (shared < min_length) && (last_key_piece[shared] == key[shared]) {
                shared += 1;
            }
        } else {
            // Restart compression
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // Add "<shared><non_shared><value_size>" to buffer_
        coding::put_fixed32(&mut self.buffer, shared as u32);
        coding::put_fixed32(&mut self.buffer, non_shared as u32);
        coding::put_fixed32(&mut self.buffer, value.len() as u32);

        // Add string delta to buffer_ followed by value
        self.buffer.extend_from_slice(&key[shared..non_shared].to_vec());
        self.buffer.extend_from_slice(&value);

        // Update state
        self.last_key = key.to_vec();
        self.counter += 1;
    }
}
