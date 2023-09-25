use crate::error::Result;
use std::ops::{Add, Sub};

/// A trait that defines a Ring data structure.
///
/// A Ring is a circular structure that usually maps nodes to positions on the ring in
/// order to distribute responsability for keys in a distributed network of nodes.
pub trait Ring<T>
where
    T: Ord + Add + Sub<Output = T>,
{
    /// Computes the counter clockwise distance from 'a' to 'b'.
    fn counter_clockwise_distance(a: T, b: T) -> T;

    /// Computes the smallest distance between 'a' and 'b'.
    fn distance(a: T, b: T) -> T;

    /// Returns if 'value' is in the range of from 'from' to 'to'.
    fn is_in_range(from: T, to: T, value: T) -> bool;
}

pub struct Ring64;
impl Ring<u64> for Ring64 {
    fn counter_clockwise_distance(a: u64, b: u64) -> u64 {
        if a > b {
            a - b
        } else {
            (u64::max_value() - b) + a
        }
    }

    fn distance(a: u64, b: u64) -> u64 {
        std::cmp::min(
            Self::counter_clockwise_distance(a, b),
            Self::counter_clockwise_distance(b, a),
        )
    }

    fn is_in_range(from: u64, to: u64, value: u64) -> bool {
        (from < to && (from <= value && value < to)) || (from > to && (from <= value || value < to))
    }
}

#[test]
fn test_counter_clockwise_distance() -> Result<()> {
    let counter_clockwise_distance = Ring64::counter_clockwise_distance(200, 100);
    assert_eq!(counter_clockwise_distance, 100);

    let counter_clockwise_distance = Ring64::counter_clockwise_distance(100, 200);
    assert_eq!(counter_clockwise_distance, u64::MAX - 100);

    let counter_clockwise_distance = Ring64::counter_clockwise_distance(100, u64::MAX - 100);
    assert_eq!(counter_clockwise_distance, 200);

    let counter_clockwise_distance = Ring64::counter_clockwise_distance(u64::MAX - 100, 100);
    assert_eq!(counter_clockwise_distance, u64::MAX - 200);

    Ok(())
}

#[test]
fn test_distance() -> Result<()> {
    let distance = Ring64::distance(200, 100);
    assert_eq!(distance, 100);

    let distance = Ring64::distance(100, 200);
    assert_eq!(distance, 100);

    let distance = Ring64::distance(100, u64::MAX - 100);
    assert_eq!(distance, 200);

    let distance = Ring64::distance(u64::MAX - 100, 100);
    assert_eq!(distance, 200);

    Ok(())
}

#[test]
fn test_is_in_range() -> Result<()> {
    let from = 100;
    let to = 200;

    let value = 50;
    assert_eq!(Ring64::is_in_range(from, to, value), false);
    assert_eq!(Ring64::is_in_range(to, from, value), true);

    let value = 150;
    assert_eq!(Ring64::is_in_range(from, to, value), true);
    assert_eq!(Ring64::is_in_range(to, from, value), false);

    let value = 250;
    assert_eq!(Ring64::is_in_range(from, to, value), false);
    assert_eq!(Ring64::is_in_range(to, from, value), true);

    Ok(())
}
