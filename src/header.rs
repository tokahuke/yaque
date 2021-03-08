//! Hamming-encoded headers for the preppers. Reading a corrupted header can
//! have devastating consequences. It is better to panic. And yes, this _has_
//! happened in a real world scenario (SIGKILL).
//!
//! Here is how to generate the parity masks, in Python:
//! ```python
//! pow2 = {1, 2, 4, 8, 16, 32}
//!
//! for p in ( 2 ** k for k in range(6) ):
//!     for i in list(range(1, 27))[::-1]: # arabic numerals...
//!         if i not in pow2:
//!             print(1 if i & p else 0, end="")
//!     print()
//! ```
//! See [this section in Wikipedia](https://en.wikipedia.org/wiki/Hamming_code#General_algorithm).

/// Parity check mask.
const P0: u32 = 0b11_1111_1111_1111_1111_1111_1111; // just a regular parity chech (not Hamming!)
/// Hamming first parity bit mask.
const P1: u32 = 0b10_1010_1010_1010_1101_0101_1011;
/// Hamming second parity bit mask.
const P2: u32 = 0b11_0011_0011_0011_0110_0110_1101;
/// Hamming third parity bit mask.
const P3: u32 = 0b11_1100_0011_1100_0111_1000_1110;
/// Hamming fourth parity bit mask.
const P4: u32 = 0b11_1111_1100_0000_0111_1111_0000;
/// Hamming fifth parity bit mask.
const P5: u32 = 0b11_1111_1111_1111_1000_0000_0000;

/// A structure holding all the header info.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The length of the payload.
    len: u32,
}

impl Header {
    /// Creates a new [`Header`] from header info (just the length by now).
    pub fn new(len: u32) -> Header {
        // last 6bits clean or 26bit available ~= 67MB:
        assert!(
            len == len & 0x03_FF_FF_FFu32,
            "length too big: {} > 2 ^ 26",
            len
        );

        Header { len }
    }

    /// Gets the length of the payload.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Encodes this header into the actual bits.
    pub fn encode(&self) -> [u8; 4] {
        let mut encoded = self.len;

        // parities:
        encoded |= ((self.len & P0).count_ones() & 0b1) << 26;
        encoded |= ((self.len & P1).count_ones() & 0b1) << 27;
        encoded |= ((self.len & P2).count_ones() & 0b1) << 28;
        encoded |= ((self.len & P3).count_ones() & 0b1) << 29;
        encoded |= ((self.len & P4).count_ones() & 0b1) << 30;
        encoded |= ((self.len & P5).count_ones() & 0b1) << 31;

        u32::to_be_bytes(encoded)
    }

    /// Decodes a header from the actual bits.
    pub fn decode(header: [u8; 4]) -> Header {
        let encoded = u32::from_be_bytes(header);

        // check parities...

        fn xor(a: bool, b: bool) -> bool {
            a && b || !a && !b
        }

        assert!(
            xor(
                (encoded & P0).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 26) != 0
            ),
            "parity 0 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (encoded & P1).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 27) != 0
            ),
            "parity 1 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (encoded & P2).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 28) != 0
            ),
            "parity 2 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (encoded & P3).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 29) != 0
            ),
            "parity 3 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (encoded & P4).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 30) != 0
            ),
            "parity 4 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (encoded & P5).count_ones() as u8 & 0b1 != 0,
                encoded & (1 << 31) != 0
            ),
            "parity 5 wrong: {:?}",
            header
        );

        // clear flags
        Header::new(encoded & 0x03_FF_FF_FFu32)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn lots_of_lengths() -> impl Iterator<Item = Header> {
        (0..)
            .map(|_| Header::new(rand::random::<u32>() & 0x03_FF_FF_FFu32))
            .take(1_000_000)
    }

    #[test]
    fn encode_length_ok() {
        let lengths = lots_of_lengths();

        for (_i, header) in lengths.enumerate() {
            // println!("{}", i);
            assert_eq!(header, Header::decode(header.encode()));
        }
    }

    #[test]
    #[should_panic]
    fn encode_gibberish() {
        let bad = *b"Asbt";
        println!("{:?}", bad);
        println!("{:?}", Header::decode(bad));
    }
}
