use ::table::block::Block;
use ::slice::Slice;
use ::status::Status;
use ::util::coding;
use ::errors::RubbleResult;
use ::options::CompressionType;
use snappy;
use std::fs::File;
use std::io::SeekFrom;
use std::io::prelude::*;

pub const MAX_ENCODED_LENGTH: usize = 10 + 10;

/// TABLE_MAGIC_NUMBER was picked by running
///    echo http://code.google.com/p/leveldb/ | sha1sum
/// and taking the leading 64 bits.
const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

/// 1-byte type + 32-bit crc
const BLOCK_TRAILER_SIZE: usize = 5;

/// Encoded length of a Footer.  Note that the serialization of a
/// Footer will always occupy exactly this many bytes.  It consists
/// of two block handles and a magic number.
pub const ENCODED_LENGTH: usize = 2 * MAX_ENCODED_LENGTH + 8;

pub struct ReadOptions;

pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new() -> BlockHandle
    {
        BlockHandle {
            offset: 0,
            size: 0,
        }
    }

    /// The offset of the block in the file.
    pub fn offset(&self) -> u64
    {
        self.offset
    }

    pub fn set_offset(&mut self, offset: u64)
    {
        self.offset = offset
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>)
    {
        // Sanity check that all fields have been set
        assert!(self.offset != !0);
        assert!(self.size != !0);
        coding::put_varint64(dst, self.offset);
        coding::put_varint64(dst, self.size);
    }

    pub fn decode_from<'a>(&mut self, input: Slice<'a>) -> RubbleResult<Slice<'a>>
    {

        let (temp, offset) = try!(coding::get_varint64(input));
        let (leftover, size) = try!(coding::get_varint64(temp));
        self.offset = offset;
        self.size = size;
        Ok(leftover)
    }

}

pub struct Footer {
    metaindex_handle: BlockHandle,
    index_handle: BlockHandle,
}


impl Footer {
    pub fn new() -> Footer
    {
        Footer {
            metaindex_handle: BlockHandle::new(),
            index_handle: BlockHandle::new(),
        }
    }

    pub fn metaindex_handle<'a>(&'a self) -> &'a BlockHandle
    {
        &self.metaindex_handle
    }

    pub fn set_metaindex_handle(&mut self, handle: BlockHandle)
    {
        self.metaindex_handle = handle
    }

    pub fn index_handle<'a>(&'a self) -> &'a BlockHandle
    {
        &self.index_handle
    }

    pub fn set_index_handle(&mut self, handle: BlockHandle)
    {
        self.index_handle = handle
    }

    pub fn decode_from<'a>(&mut self, input: Slice<'a>) -> RubbleResult<Slice<'a>>
    {
        let magic_slice = &input[(ENCODED_LENGTH - 8)..];
        let magic_lo = coding::decode_fixed32(magic_slice);
        let magic_hi = coding::decode_fixed32(&magic_slice[4..]);
        let magic = (magic_hi as u64) << 32 | magic_lo as u64;

        if magic != TABLE_MAGIC_NUMBER {
            return Err(Status::Corruption("not an sstable (bad magic number)".into()).into())
        }

        try!(self.metaindex_handle.decode_from(input));
        try!(self.index_handle.decode_from(input));
        // We skip over any leftover data (just padding for now) in "input"
        Ok(&magic_slice[8..])

    }

    pub fn encode_to(&self, dst: &mut Vec<u8>)
    {
        let original_size = dst.len();
        self.metaindex_handle.encode_to(dst);
        self.index_handle.encode_to(dst);
        dst.resize(2 * MAX_ENCODED_LENGTH, 0);  // Padding
        coding::put_fixed32(dst, (TABLE_MAGIC_NUMBER & 0xffffffff) as u32);
        coding::put_fixed32(dst, (TABLE_MAGIC_NUMBER >> 32) as u32);
        assert!(dst.len() == original_size + ENCODED_LENGTH);
    }

}

pub struct BlockContents {
    /// Actual contents of data
    data: Vec<u8>,
    /// True iff data can be cached
    cachable: bool,
}

/// TODO allow for stack allocation?
pub fn read_block(file: &mut File, options: &ReadOptions, handle: &BlockHandle)
                  -> RubbleResult<BlockContents>
{
    let mut result = BlockContents { data: vec![], cachable: false };

    // Read the block contents as well as the type/crc footer.
    // See table_builder.cc for the code that built this structure.
    let n = handle.size as usize;
    let mut buff = Vec::<u8>::with_capacity(n + BLOCK_TRAILER_SIZE);

    // Slice contents;
    try!(file.seek(SeekFrom::Start(handle.offset)));
    try!(file.read_exact(&mut buff.as_mut_slice()));

    // TODO CHECK CHECKSUMS
    //
    //   // Check the crc of the type and the block contents
    //   const char* data = contents.data();    // Pointer to where Read put the data
    //   if (options.verify_checksums) {
    //     const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    //     const uint32_t actual = crc32c::Value(data, n + 1);
    //     if (actual != crc) {
    //       delete[] buf;
    //       s = Status::Corruption("block checksum mismatch");
    //       return s;
    //     }
    //   }

    match buff[n] {
        x if x == CompressionType::NoCompression as u8 => {
            // TODO stack allocated implementation?
            // if (data != buf) {
            // File implementation gave us pointer to some other data.
            // Use it directly under the assumption that it will be live
            // while the file is open.
            // delete[] buf;
            // result->data = Slice(data, n);
            // result->heap_allocated = false;
            // result->cachable = false;  // Do not double-cache
            // } else {
            // result.heap_allocated = true;
            result.data = buff;
            result.cachable = true;
        },
        x if x == CompressionType::SnappyCompression as u8 => {
            let uncompressed = snappy::uncompress(buff.as_slice())
                .or(Err(Status::Corruption("corrupted compressed block contents".into())));
            let uncompressed = try!(uncompressed);

            result.data = uncompressed;
            result.cachable = true;
        }
        _ => return Err(Status::Corruption("Bad block type".into()).into())
    }

    Ok(result)
}
