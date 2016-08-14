use ::errors::RubbleResult;
use ::table::block::{Block, OwnedBlock, BlockIterator};
use ::comparator::SliceComparator;
use ::slice::Slice;
use ::status::Status;
use ::table::format;
use std::fs::File;
use std::io::SeekFrom;
use std::io::prelude::*;
use itertools::Zip;
use ::table::format::{
    MAX_ENCODED_LENGTH,
    ENCODED_LENGTH,
    BlockHandle,
    BlockContents,
    Footer,
    read_block,
};
use ::options::{
    Options,
    ReadOptions,
};

struct TableCache;
struct FilterBlockReader;



struct TableRep<'a, F: Read + Seek> {
    options: &'a Options,
    status: Status,
    file: F,
    cache_id: u64,
    filter: FilterBlockReader,
    filter_data: Vec<u8>,
    index_block: BlockContents,
    // metaindex_handle: &'a BlockHandle,
}


/// A Table is a sorted map from strings to strings.  Tables are
/// immutable and persistent.  A Table may be safely accessed from
/// multiple threads without external synchronization.
pub struct Table<'a, F: Read + Seek> {
    rep: TableRep<'a, F>,
}


impl<'a, F> Table<'a, F>
    where F: Read + Seek
{

    /// Attempt to open the table that is stored in bytes [0..file_size)
    /// of "file", and read the metadata entries necessary to allow
    /// retrieving data from the table.
    ///
    /// If successful, returns ok and sets "*table" to the newly opened
    /// table.  The client should delete "*table" when no longer needed.
    /// If there was an error while initializing the table, sets "*table"
    /// to NULL and returns a non-ok status.  Does not take ownership of
    /// "*source", but the client must ensure that "source" remains live
    /// for the duration of the returned table's lifetime.
    ///
    /// *file must remain live while this Table is in use.
    fn open(options: &'a Options, mut file: F, size: usize) -> RubbleResult<Table<'a, F>>
        where F: Read + Seek
    {
        if size < ENCODED_LENGTH as usize {
            return Err(Status::Corruption("file is too short to be an sstable".into()).into());
        }

        let mut footer_input = [0; ENCODED_LENGTH];

        let footer_offset = -(ENCODED_LENGTH as i64);
        try!(file.seek(SeekFrom::End(footer_offset)));
        try!(file.read_exact(&mut footer_input));

        let mut footer = Footer::new();
        try!(footer.decode_from(&footer_input));

        let mut opt = ReadOptions::new();
        let index_block = try!(read_block(&mut file, &opt, footer.index_handle()));
        opt.verify_checksums = options.paranoid_checks;

        let cache_id = match options.block_cache.is_some() {
            true => 0, //options.block_cache->NewId(),
            false => 0,
        };

        // We've successfully read the footer and the index block: we're
        // ready to serve requests.
        let rep = TableRep {
            status: Status::Ok,
            options: options,
            file: file,
            index_block: index_block,
            cache_id: cache_id,
            filter_data: vec![],
            // TODO
            filter: FilterBlockReader{},
        };

        let mut table = Table {
            rep: rep,
        };

        try!(table.read_meta(&footer));
        Ok(table)
    }

    /// Returns a new iterator over the table contents.
    /// The result of new_iterator() is initially invalid (caller must
    /// call one of the Seek methods on the iterator before using it).
    fn iter<'b, T>(&'b self, read_options: &ReadOptions) -> BlockIterator<'b, T>
        where T: SliceComparator
    {
        // return NewTwoLevelIterator(
        //     rep_->index_block->NewIterator(rep_->options.comparator),
        //     &Table::BlockReader, const_cast<Table*>(this), options);
        unimplemented!()
    }


    /// Given a key, return an approximate byte offset in the file where
    /// the data for that key begins (or would begin if the key were
    /// present in the file).  The returned value is in terms of file
    /// bytes, and so includes effects like compression of the underlying data.
    /// E.g., the approximate offset of the last key in the table will
    /// be close to the file length.
    fn approximate_offset_of(key: Slice) -> usize
    {

        // let index_iter = self.rep.index_block.iter(self.rep.op);
        // Iterator* index_iter =
        //         rep_->index_block->NewIterator(rep_->options.comparator);
        //     index_iter->Seek(key);
        //     uint64_t result;
        //     if (index_iter->Valid()) {
        //         BlockHandle handle;
        //         Slice input = index_iter->value();
        //         Status s = handle.DecodeFrom(&input);
        //         if (s.ok()) {
        //             result = handle.offset();
        //         } else {
        //             // Strange: we can't decode the block handle in the index block.
        //             // We'll just return the offset of the metaindex block, which is
        //             // close to the whole file size for this case.
        //             result = rep_->metaindex_handle.offset();
        //         }
        //     } else {
        //         // key is past the last key in the file.  Approximate the offset
        //         // by returning the offset of the metaindex block (which is
        //         // right near the end of the file).
        //         result = rep_->metaindex_handle.offset();
        //     }
        //     delete index_iter;
        //     return result;
        // }
        unimplemented!()
    }

    // /// Calls (*handle_result)(arg, ...) with the entry found after a call
    // /// to Seek(key).  May not make such a call if filter policy says
    // /// that key is not present.
    // fn internal_get(&self, options: &ReadOptions, key: Slice, void* arg,
    //                 void (*handle_result)(void* arg, const Slice& k, const Slice& v));

    fn read_meta(&mut self, footer: &Footer) -> RubbleResult<()>
    {
        // TODO: impl  self.rep.options.filter_policy.is_some()
        if self.rep.options.filter_policy.is_none() {
            return Ok(())
        }

        //   // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
        //   // it is an empty block.
        //   ReadOptions opt;
        //   if (rep_->options.paranoid_checks) {
        //     opt.verify_checksums = true;
        //   }
        //   BlockContents contents;
        //   if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
        //     // Do not propagate errors since meta info is not needed for operation
        //     return;
        //   }
        //   Block* meta = new Block(contents);

        //   Iterator* iter = meta->NewIterator(BytewiseComparator());
        //   std::string key = "filter.";
        //   key.append(rep_->options.filter_policy->Name());
        //   iter->Seek(key);
        //   if (iter->Valid() && iter->key() == Slice(key)) {
        //     ReadFilter(iter->value());
        //   }
        //   delete iter;
        //   delete meta;
        // }
        Ok(())
    }

    fn read_filter(&mut self, filter_handle_value: Slice) -> RubbleResult<()>
    {
        let mut filter_handle = BlockHandle::new();

        let next = try!(filter_handle.decode_from(filter_handle_value));

        // We might want to unify with ReadBlock() if we start
        // requiring checksum verification in Table::Open.
        let mut opt = ReadOptions::new();
        if self.rep.options.paranoid_checks {
            opt.verify_checksums = true;
        }

        // TODO!
        // let mut file = &mut self.rep.file;
        // let block = try!(read_block(file, &opt, &filter_handle));
        // if (block.heap_allocated) {
        // self.rep.filter_data = block.data;     // Will need to delete later??
        // }
        // self.rep.filter = FilterBlockReader::new(self.rep.options.filter_policy, block.data);
        Ok(())
    }

}

struct LessThanComparator;

impl SliceComparator for LessThanComparator {
    fn compare(&self, a: Slice, b: Slice) -> i32
    {
        for (bytea, byteb) in Zip::new((a, b)) {
            if bytea < byteb {
                return 1
            } else if bytea < byteb {
                return -1
            }
        }
        return 0
    }
}


// // Convert an index iterator value (i.e., an encoded BlockHandle)
// // into an iterator over the contents of the corresponding block.
// Iterator* Table::BlockReader(void* arg,
//                              const ReadOptions& options,
//                              const Slice& index_value) {
//   Table* table = reinterpret_cast<Table*>(arg);
//   Cache* block_cache = table->rep_->options.block_cache;
//   Block* block = NULL;
//   Cache::Handle* cache_handle = NULL;

//   BlockHandle handle;
//   Slice input = index_value;
//   Status s = handle.DecodeFrom(&input);
//   // We intentionally allow extra stuff in index_value so that we
//   // can add more features in the future.

//   if (s.ok()) {
//     BlockContents contents;
//     if (block_cache != NULL) {
//       char cache_key_buffer[16];
//       EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
//       EncodeFixed64(cache_key_buffer+8, handle.offset());
//       Slice key(cache_key_buffer, sizeof(cache_key_buffer));
//       cache_handle = block_cache->Lookup(key);
//       if (cache_handle != NULL) {
//         block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
//       } else {
//         s = ReadBlock(table->rep_->file, options, handle, &contents);
//         if (s.ok()) {
//           block = new Block(contents);
//           if (contents.cachable && options.fill_cache) {
//             cache_handle = block_cache->Insert(
//                 key, block, block->size(), &DeleteCachedBlock);
//           }
//         }
//       }
//     } else {
//       s = ReadBlock(table->rep_->file, options, handle, &contents);
//       if (s.ok()) {
//         block = new Block(contents);
//       }
//     }
//   }

//   Iterator* iter;
//   if (block != NULL) {
//     iter = block->NewIterator(table->rep_->options.comparator);
//     if (cache_handle == NULL) {
//       iter->RegisterCleanup(&DeleteBlock, block, NULL);
//     } else {
//       iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
//     }
//   } else {
//     iter = NewErrorIterator(s);
//   }
//   return iter;
// }

// Status Table::InternalGet(const ReadOptions& options, const Slice& k,
//                           void* arg,
//                           void (*saver)(void*, const Slice&, const Slice&)) {
//   Status s;
//   Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
//   iiter->Seek(k);
//   if (iiter->Valid()) {
//     Slice handle_value = iiter->value();
//     FilterBlockReader* filter = rep_->filter;
//     BlockHandle handle;
//     if (filter != NULL &&
//         handle.DecodeFrom(&handle_value).ok() &&
//         !filter->KeyMayMatch(handle.offset(), k)) {
//       // Not found
//     } else {
//       Iterator* block_iter = BlockReader(this, options, iiter->value());
//       block_iter->Seek(k);
//       if (block_iter->Valid()) {
//         (*saver)(arg, block_iter->key(), block_iter->value());
//       }
//       s = block_iter->status();
//       delete block_iter;
//     }
//   }
//   if (s.ok()) {
//     s = iiter->status();
//   }
//   delete iiter;
//   return s;
// }


// }  // namespace leveldb
