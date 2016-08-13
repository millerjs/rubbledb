use ::slice::Slice;

pub trait SliceComparator {
    fn compare(&self, a: Slice, b: Slice) -> i32;
}
