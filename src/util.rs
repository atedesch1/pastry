use crate::error::Result;

pub const HEX_NUM_OF_BITS_PER_DIGIT: u32 = 4;
pub const U64_HEX_NUM_OF_DIGITS: u32 = u64::BITS / HEX_NUM_OF_BITS_PER_DIGIT as u32;
pub const HEX_DIGIT_MASK: u32 = 0xF;

/// Gets the nth hexadecimal digit from a u64.
pub fn get_nth_digit_in_u64_hex(num: u64, n: usize) -> Result<u32> {
    if n >= 16 {
        return Err(crate::error::Error::Internal(
            "There are only 16 digits in a hexadecimal number.".into(),
        ));
    }

    Ok((num >> (u64::BITS - (n + 1) as u32 * HEX_NUM_OF_BITS_PER_DIGIT)) as u32 & HEX_DIGIT_MASK)
}

pub fn get_distance_between_unsigned<T>(a: T, b: T) -> T
where
    T: PartialOrd + std::ops::Sub<Output = T>,
{
    if a > b {
        a - b
    } else {
        b - a
    }
}

mod tests {
    use super::get_nth_digit_in_u64_hex;
    use super::Result;

    #[test]
    fn test_get_nth_digit_in_u64_hex() -> Result<()> {
        assert_eq!(get_nth_digit_in_u64_hex(0xFEDCBA9876543210, 0)?, 0xF);
        assert_eq!(get_nth_digit_in_u64_hex(0xFEDCBA9876543210, 9)?, 0x6);
        assert_eq!(get_nth_digit_in_u64_hex(0xFEDCBA9876543210, 15)?, 0x0);
        assert_eq!(get_nth_digit_in_u64_hex(0xEDCBA9876543210, 0)?, 0x0);
        assert_eq!(get_nth_digit_in_u64_hex(0xDCBA9876543210, 0)?, 0x0);
        assert_eq!(get_nth_digit_in_u64_hex(0xDCBA9876543210, 1)?, 0x0);
        assert_eq!(get_nth_digit_in_u64_hex(0xDCBA9876543210, 2)?, 0xD);

        Ok(())
    }
}
