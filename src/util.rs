use crate::error::Result;

pub const HEX_NUM_OF_BITS_PER_DIGIT: u32 = 4;
pub const U64_HEX_NUM_OF_DIGITS: u32 = u64::BITS / HEX_NUM_OF_BITS_PER_DIGIT as u32;
pub const HEX_DIGIT_MASK: u32 = 0xF;

/// A general trait for having a get_id function
pub trait HasID<T> {
    fn get_id(&self) -> T;
}

/// Gets the nth hexadecimal digit from a u64.
pub fn get_nth_digit_in_u64_hex(num: u64, n: u32) -> Result<u32> {
    if n >= 16 {
        return Err(crate::error::Error::Internal(
            "There are only 16 digits in a hexadecimal number.".into(),
        ));
    }

    Ok((num >> (u64::BITS - (n + 1) * HEX_NUM_OF_BITS_PER_DIGIT)) as u32 & HEX_DIGIT_MASK)
}

pub fn get_distance_between_usigned<T>(a: T, b: T) -> T
where
    T: PartialOrd + std::ops::Sub<Output = T>,
{
    if a > b {
        a - b
    } else {
        b - a
    }
}
