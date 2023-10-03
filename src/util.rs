use crate::error::Result;

pub const HEX_NUM_OF_BITS_PER_DIGIT: u32 = 4;
pub const U64_HEX_NUM_OF_DIGITS: u32 = u64::BITS / HEX_NUM_OF_BITS_PER_DIGIT as u32;
pub const HEX_DIGIT_MASK: u32 = 0xF;

/// Gets the first n hexadecimal digits from a u64.
pub fn get_first_digits_in_u64_hex(num: u64, n: usize) -> Result<u64> {
    if n > 16 {
        return Err(crate::error::Error::Internal(
            "There are only 16 digits in a hexadecimal number.".into(),
        ));
    }

    if n == 0 {
        return Err(crate::error::Error::Internal(
            "Cannot get first 0 digits from a hexadecimal number.".into(),
        ));
    }

    let mut mask: u64 = 0x0;
    for _ in 0..n {
        mask = mask << HEX_NUM_OF_BITS_PER_DIGIT;
        mask += HEX_DIGIT_MASK as u64;
    }

    Ok((num >> (u64::BITS - n as u32 * HEX_NUM_OF_BITS_PER_DIGIT)) & mask)
}

/// Gets the nth hexadecimal digit from a u64.
pub fn get_nth_digit_in_u64_hex(num: u64, n: usize) -> Result<u32> {
    if n >= 16 {
        return Err(crate::error::Error::Internal(
            "There are only 16 digits in a hexadecimal number.".into(),
        ));
    }

    Ok((num >> (u64::BITS - (n + 1) as u32 * HEX_NUM_OF_BITS_PER_DIGIT)) as u32 & HEX_DIGIT_MASK)
}

/// Gets the number of matched digits of two u64 numbers.
pub fn get_num_matched_digits(a: u64, b: u64) -> Result<u32> {
    for i in 0..U64_HEX_NUM_OF_DIGITS as usize {
        if get_nth_digit_in_u64_hex(a, i)? != get_nth_digit_in_u64_hex(b, i)? {
            return Ok(i as u32);
        }
    }

    Ok(U64_HEX_NUM_OF_DIGITS - 1)
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
    use super::*;

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

    #[test]
    fn test_get_first_n_digits_in_u64_hex() -> Result<()> {
        assert_eq!(get_first_digits_in_u64_hex(0xFEDCBA9876543210, 1)?, 0xF);
        assert_eq!(get_first_digits_in_u64_hex(0xFEDCBA9876543210, 2)?, 0xFE);
        assert_eq!(get_first_digits_in_u64_hex(0xFEDCBA9876543210, 3)?, 0xFED);
        assert_eq!(
            get_first_digits_in_u64_hex(0xFEDCBA9876543210, 16)?,
            0xFEDCBA9876543210
        );

        Ok(())
    }
}
