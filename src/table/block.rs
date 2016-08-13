use ::slice::Slice;
use ::errors::RubbleResult;
use ::util;
use ::status::Status;
use std::mem;

pub struct OwnedBlock {
    data: Vec<u8>,
    restart_offset: u32,
}

pub struct SliceBlock<'a> {
    data: Slice<'a>,
    restart_offset: u32,
}

pub trait Block {
    fn get_size(&self) -> usize;

    fn num_restarts(size: usize) -> u32
    {
        assert!(size >= mem::size_of::<u32>());
        10
        // TODO: DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    }
}

impl Block for OwnedBlock {
    fn get_size(&self) -> usize { self.data.len() }
}

impl<'a> Block for SliceBlock<'a> {
    fn get_size(&self) -> usize { self.data.len() }
}

impl OwnedBlock {
    fn new(contents: Slice) -> RubbleResult<OwnedBlock>
    {
        let sizeof_u32 = mem::size_of::<u32>() as u32;
        let max_restarts_allowed = (contents.len() as u32 -sizeof_u32) / sizeof_u32;
        let num_restarts = Self::num_restarts(contents.len());

        if num_restarts > max_restarts_allowed {
            return Err("The size is too small for num_restarts()".into())
        }

        Ok(OwnedBlock {
            data: contents.to_vec(),
            restart_offset: (
                contents.len() - (1 + num_restarts as usize) * sizeof_u32 as usize
            ) as u32,
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
        let fallback = try!(util::coding::get_varint32_ptr_fallback(p));
        p = fallback.slice;
        shared = fallback.value;
        let fallback = try!(util::coding::get_varint32_ptr_fallback(p));
        p = fallback.slice;
        non_shared = fallback.value;
        let fallback = try!(util::coding::get_varint32_ptr_fallback(p));
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
    fn compare(&self, a: Slice, b: Slice) -> usize;
}

pub struct BlockIterator<'a, T: SliceComparator> {
    comparator: T,
    data: Slice<'a>,
    restarts: u32,
    num_restarts: u32,
    current: usize,
    restart_index: usize,
    key: String,
    status: Status,
}

impl<'a, T: SliceComparator> BlockIterator<'a, T> {
    fn compare(&self, a: Slice, b: Slice) -> usize {
        self.comparator.compare(a, b)
    }

    // /// Return the slice in data_ just past the end of the current entry.
    // fn next_entry(&self) -> Slice {

    // }

    // fn get_restart_point(index: u32) ->  {
    //     assert!(index < self.num_restarts);
    //     // return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
    // }

    // void SeekToRestartPoint(uint32_t index) {
    //     key_.clear();
    //     restart_index_ = index;
    //     // current_ will be fixed by ParseNextKey();

    //     // ParseNextKey() starts at the end of value_, so set value_ accordingly
    //     uint32_t offset = GetRestartPoint(index);
    //     value_ = Slice(data_ + offset, 0);
    // }

}

// class Block::Iter : public Iterator {

//   inline int Compare(const Slice& a, const Slice& b) const {
//     return comparator_->Compare(a, b);
//   }

//   // Return the offset in data_ just past the end of the current entry.
//   inline uint32_t NextEntryOffset() const {
//     return (value_.data() + value_.size()) - data_;
//   }

//   uint32_t GetRestartPoint(uint32_t index) {
//     assert(index < num_restarts_);
//     return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
//   }

//   void SeekToRestartPoint(uint32_t index) {
//     key_.clear();
//     restart_index_ = index;
//     // current_ will be fixed by ParseNextKey();

//     // ParseNextKey() starts at the end of value_, so set value_ accordingly
//     uint32_t offset = GetRestartPoint(index);
//     value_ = Slice(data_ + offset, 0);
//   }

//  public:
//   Iter(const Comparator* comparator,
//        const char* data,
//        uint32_t restarts,
//        uint32_t num_restarts)
//       : comparator_(comparator),
//         data_(data),
//         restarts_(restarts),
//         num_restarts_(num_restarts),
//         current_(restarts_),
//         restart_index_(num_restarts_) {
//     assert(num_restarts_ > 0);
//   }

//   virtual bool Valid() const { return current_ < restarts_; }
//   virtual Status status() const { return status_; }
//   virtual Slice key() const {
//     assert(Valid());
//     return key_;
//   }
//   virtual Slice value() const {
//     assert(Valid());
//     return value_;
//   }

//   virtual void Next() {
//     assert(Valid());
//     ParseNextKey();
//   }

//   virtual void Prev() {
//     assert(Valid());

//     // Scan backwards to a restart point before current_
//     const uint32_t original = current_;
//     while (GetRestartPoint(restart_index_) >= original) {
//       if (restart_index_ == 0) {
//         // No more entries
//         current_ = restarts_;
//         restart_index_ = num_restarts_;
//         return;
//       }
//       restart_index_--;
//     }

//     SeekToRestartPoint(restart_index_);
//     do {
//       // Loop until end of current entry hits the start of original entry
//     } while (ParseNextKey() && NextEntryOffset() < original);
//   }

//   virtual void Seek(const Slice& target) {
//     // Binary search in restart array to find the last restart point
//     // with a key < target
//     uint32_t left = 0;
//     uint32_t right = num_restarts_ - 1;
//     while (left < right) {
//       uint32_t mid = (left + right + 1) / 2;
//       uint32_t region_offset = GetRestartPoint(mid);
//       uint32_t shared, non_shared, value_length;
//       const char* key_ptr = DecodeEntry(data_ + region_offset,
//                                         data_ + restarts_,
//                                         &shared, &non_shared, &value_length);
//       if (key_ptr == NULL || (shared != 0)) {
//         CorruptionError();
//         return;
//       }
//       Slice mid_key(key_ptr, non_shared);
//       if (Compare(mid_key, target) < 0) {
//         // Key at "mid" is smaller than "target".  Therefore all
//         // blocks before "mid" are uninteresting.
//         left = mid;
//       } else {
//         // Key at "mid" is >= "target".  Therefore all blocks at or
//         // after "mid" are uninteresting.
//         right = mid - 1;
//       }
//     }

//     // Linear search (within restart block) for first key >= target
//     SeekToRestartPoint(left);
//     while (true) {
//       if (!ParseNextKey()) {
//         return;
//       }
//       if (Compare(key_, target) >= 0) {
//         return;
//       }
//     }
//   }

//   virtual void SeekToFirst() {
//     SeekToRestartPoint(0);
//     ParseNextKey();
//   }

//   virtual void SeekToLast() {
//     SeekToRestartPoint(num_restarts_ - 1);
//     while (ParseNextKey() && NextEntryOffset() < restarts_) {
//       // Keep skipping
//     }
//   }

//  private:
//   void CorruptionError() {
//     current_ = restarts_;
//     restart_index_ = num_restarts_;
//     status_ = Status::Corruption("bad entry in block");
//     key_.clear();
//     value_.clear();
//   }

//   bool ParseNextKey() {
//     current_ = NextEntryOffset();
//     const char* p = data_ + current_;
//     const char* limit = data_ + restarts_;  // Restarts come right after data
//     if (p >= limit) {
//       // No more entries to return.  Mark as invalid.
//       current_ = restarts_;
//       restart_index_ = num_restarts_;
//       return false;
//     }

//     // Decode next entry
//     uint32_t shared, non_shared, value_length;
//     p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
//     if (p == NULL || key_.size() < shared) {
//       CorruptionError();
//       return false;
//     } else {
//       key_.resize(shared);
//       key_.append(p, non_shared);
//       value_ = Slice(p + non_shared, value_length);
//       while (restart_index_ + 1 < num_restarts_ &&
//              GetRestartPoint(restart_index_ + 1) < current_) {
//         ++restart_index_;
//       }
//       return true;
//     }
//   }
// };

// Iterator* Block::NewIterator(const Comparator* cmp) {
//   if (size_ < sizeof(uint32_t)) {
//     return NewErrorIterator(Status::Corruption("bad block contents"));
//   }
//   const uint32_t num_restarts = NumRestarts();
//   if (num_restarts == 0) {
//     return NewEmptyIterator();
//   } else {
//     return new Iter(cmp, data_, restart_offset_, num_restarts);
//   }
// }

// }  // namespace leveldb
