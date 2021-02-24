/// Hamming-encoded headers for the preppers. Reading a corrupted header can 
/// have devastating consequences. It is better to panic. And yes, this _has_
/// happened in a real world scenario (SIGKILL).
 
// generate the parity masks:
// ```python
// pow2 = {1, 2, 4, 8, 16, 32}
//
// for p in ( 2 ** k for k in range(6) ):
//     for i in list(range(1, 27))[::-1]: # arabic numerals...
//         if i not in pow2:
//             print(1 if i & p else 0, end="")
//     print()
// ```
// See: https://en.wikipedia.org/wiki/Hamming_code#General_algorithm
const P1: u32 = 0b10_1010_1010_1010_1101_0101_1011;
const P2: u32 = 0b11_0011_0011_0011_0110_0110_1101;
const P3: u32 = 0b11_1100_0011_1100_0111_1000_1110;
const P4: u32 = 0b11_1111_1100_0000_0111_1111_0000;
const P5: u32 = 0b11_1111_1111_1111_1000_0000_0000;

pub fn encode_len(mut len: u32) -> [u8; 4] {
    // last 6bits clean or 26bit available ~= 67MB:
    assert!(
        len == len & 0x03_FF_FF_FFu32,
        "length too big: {} > 2 ^ 26",
        len
    );

    // set last bit (new stuff):
    len |= 1 << 31;

    // parities:
    len |= ((len & P1).count_ones() & 0b1) << 26;
    len |= ((len & P2).count_ones() & 0b1) << 27;
    len |= ((len & P3).count_ones() & 0b1) << 28;
    len |= ((len & P4).count_ones() & 0b1) << 29;
    len |= ((len & P5).count_ones() & 0b1) << 30;

    u32::to_be_bytes(len)
}

pub fn decode_len(header: [u8; 4]) -> u32 {
    let len = u32::from_be_bytes(header);

    if len & (1 << 31) != 0 {
        // last bit set: new stuff
        // check parities...

        fn xor(a: bool, b: bool) -> bool {
            a && b || !a && !b
        }

        assert!(
            xor(
                (len & P1).count_ones() as u8 & 0b1 != 0,
                len & (1 << 26) != 0
            ),
            "parity 1 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (len & P2).count_ones() as u8 & 0b1 != 0,
                len & (1 << 27) != 0
            ),
            "parity 2 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (len & P3).count_ones() as u8 & 0b1 != 0,
                len & (1 << 28) != 0
            ),
            "parity 3 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (len & P4).count_ones() as u8 & 0b1 != 0,
                len & (1 << 29) != 0
            ),
            "parity 4 wrong: {:?}",
            header
        );
        assert!(
            xor(
                (len & P5).count_ones() as u8 & 0b1 != 0,
                len & (1 << 30) != 0
            ),
            "parity 5 wrong: {:?}",
            header
        );

        len & 0x03_FF_FF_FFu32
    } else {
        // last bit not set: old stuff
        len
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn lots_of_lengths() -> impl Iterator<Item = u32> {
        (0..).map(|_| rand::random::<u32>() & 0x03_FF_FF_FFu32).take(1_000_000)
    }

    #[test]
    fn encode_length_ok() {
        let lengths = lots_of_lengths();

        for (_i, len) in lengths.enumerate() {
            // println!("{}", i);
            assert_eq!(len, decode_len(encode_len(len)));
        }
    }

    #[test]
    fn encode_length_legacy_ok() {
        let lengths = lots_of_lengths();

        for (_i, len) in lengths.enumerate() {
            // println!("{}", i);
            assert_eq!(len, decode_len(u32::to_be_bytes(len)));
        }
    }

    #[test]
    #[should_panic]
    fn encode_gibberish() {
        decode_len(*b"\xA0cri");
    }
}
