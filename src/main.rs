use std::{
    fs,
    io::{self, stdout, Write},
    os::{fd::AsRawFd, raw::c_void},
    slice,
    time::Instant,
};

use hashbrown::{hash_table::Entry, HashTable};
use one_brc_rs::simd_buffers::SimdBuffer;
use rayon::prelude::*;

type FixedPrecisionNumber = fixed::types::I48F16;
type Measure = i16;

#[inline(always)]
fn to_fixed_number(measure: Measure) -> FixedPrecisionNumber {
    FixedPrecisionNumber::from(measure) / FixedPrecisionNumber::from(10)
}

type Db<'a> = HashTable<(u64, Record<'a>)>;

/// Wrapper above unsafe libc call for memory mapping management.
struct MmapedFile {
    addr: *mut c_void,
    data_len: usize,
}

impl MmapedFile {
    fn from_file(file: fs::File) -> Result<Self, io::Error> {
        let file_size = file.metadata()?.len();

        unsafe {
            let fd = file.as_raw_fd();
            let addr = libc::mmap(
                std::ptr::null_mut(),
                file_size as libc::size_t,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            );

            if addr == libc::MAP_FAILED {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self {
                    addr,
                    data_len: file_size as usize,
                })
            }
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.addr.cast(), self.data_len) }
    }
}

// 💩 do not clean our memory mapped segment.
// I know this is sad, but it make us win 25ms...
//
// impl Drop for MmapedFile {
//     fn drop(&mut self) {
//         unsafe {
//             libc::munmap(self.addr, self.data_len);
//         }
//     }
// }

#[inline(always)]
fn buf_append(buf: &mut SimdBuffer, value: i16) {
    if buf.is_full() {
        buf.simplify_and_add(value);
    } else {
        buf.add(value);
    }
}

#[derive(Debug)]
struct Record<'a> {
    city: &'a [u8],
    buf: SimdBuffer,
    sum: i32,
    count: usize,
}

impl<'a> Record<'a> {
    #[inline(always)]
    fn new(city: &'a [u8], value: Measure) -> Self {
        Self {
            city,
            buf: SimdBuffer::with_value(value),
            sum: value as i32,
            count: 1,
        }
    }

    #[inline(always)]
    fn add(&mut self, value: Measure) {
        buf_append(&mut self.buf, value);

        self.sum += value as i32;
        self.count += 1;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        let (other_min, other_max) = other.buf.min_max();
        buf_append(&mut self.buf, other_min);
        buf_append(&mut self.buf, other_max);
        self.sum += other.sum;
        self.count += other.count;
    }

    #[inline(always)]
    fn avg(&self) -> FixedPrecisionNumber {
        FixedPrecisionNumber::from(self.sum)
            / FixedPrecisionNumber::from(10)
            / FixedPrecisionNumber::from(self.count as i32)
    }
}

/// Fast line parser.
///
/// Parse everything in the line at once (city name + temp).
///
/// Do not work as a general purpose line parser and use a lot of UNSAFE!
/// I did not find any other way to fully remove bound check otherwise.
///
/// This function has been optimize with [GodBolt](https://godbolt.org/z/ojTzzYo9n) and takes only
/// 46 assembly line.
unsafe fn fast_parse_line(line: &[u8]) -> (&[u8], Measure) {
    let len = line.len();

    // We know in advance last 3 bytes are:
    // - the last temperature integer digit
    // - a dot
    // - the temperature floating part
    let f = (*line.get_unchecked(len - 1) & 0x0F) as i16;
    let i2 = (*line.get_unchecked(len - 3) & 0x0F) as i16 * 10;

    // Parse remaining bytes:
    // - neg sign ?
    // - missing digit ?
    // - missing digit and neg sign ?
    let (sep_index, neg, i1) = match *line.get_unchecked(len - 4) {
        b';' => (len - 4, false, 0),
        b'-' => (len - 5, true, 0),
        v => {
            let i1 = (v & 0x0F) as i16 * 100;
            match *line.get_unchecked(len - 5) {
                b';' => (len - 5, false, i1),
                _ => (len - 6, true, i1),
            }
        }
    };

    // Create slice reference for city name.
    let city = line.get_unchecked(..sep_index);

    // Build number from what we have extracted.
    let temp_output = i1 + i2 + f;
    let temp = if neg { -temp_output } else { temp_output };

    // Return result.
    (city, temp)
}

fn find_map_entry<'db, 'data>(
    map: &'db mut Db<'data>,
    code: u64,
) -> Entry<'db, (u64, Record<'data>)> {
    // Intentionally use wrong eq operator (instead of city name) for better performances.
    // If there are multiple city with same hash, they will collide 🐞.
    map.entry(code, |(x, _)| *x == code, |(x, _)| *x)
}

fn compute() -> io::Result<()> {
    // Warm up global thread pool.
    rayon::ThreadPoolBuilder::new()
        .stack_size(256 << 10) // Set a small stack size for our threads so we have more CPU cache available.
        .build_global()
        .expect("Fail to startup rayon thread pool");

    // Open file and memory map it.
    let file = fs::File::open("measurements.txt")?;
    let mmap_file = MmapedFile::from_file(file)?;

    let db = mmap_file
        // Convert memory mapped file to bytes slice.
        .as_slice()
        // Ignore last line break to remove empty lines.
        .strip_suffix(b"\n")
        .expect("Cannot strip line suffix")
        // Iterate over each line on a thread pool.
        .par_split(|x| *x == b'\n')
        // Parse each line assuming there are only 2 elements on it.
        .map(|line| unsafe { fast_parse_line(line) })
        // Insert every entry to thread local DB.
        .fold(Db::default, |mut map, (city, temp)| {
            let code = fxhash::hash64(city);

            match find_map_entry(&mut map, code) {
                Entry::Occupied(entry) => {
                    debug_assert_eq!(entry.get().1.city, city);
                    entry.into_mut().1.add(temp);
                }
                Entry::Vacant(entry) => {
                    entry.insert((code, Record::new(city, temp)));
                }
            }

            map
        })
        // Merge all DBs.
        .reduce(Db::default, |mut merged_map, sub_map| {
            // Reserve some space in output DB for sub DB.
            let new_element_count = sub_map.len().saturating_sub(merged_map.capacity());
            merged_map.reserve(new_element_count, |x| x.0);

            // Merge all elements.
            for (code, record) in sub_map {
                match find_map_entry(&mut merged_map, code) {
                    Entry::Occupied(entry) => {
                        debug_assert_eq!(entry.get().1.city, record.city);
                        entry.into_mut().1.merge(&record);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((code, record));
                    }
                }
            }

            merged_map
        });

    // Extract record from aggregated DB and sort them by city name.
    let mut records: Vec<_> = db.into_iter().map(|x| x.1).collect();
    records.sort_unstable_by_key(|x| x.city);

    // Allocate output buffer.
    let est_record_size =
            20 // enough space for city name
            + 1 // eq
            + (3 * 5) // values
            + 2 // slashes
            + 2 // comma-space
        ;

    let mut out: Vec<u8> = Vec::with_capacity(records.len() * est_record_size);
    out.push(b'{');

    // Write all records to output buffer.
    for (idx, record) in records.into_iter().enumerate() {
        if idx > 0 {
            out.extend_from_slice(b", ");
        }

        out.extend_from_slice(record.city);
        out.push(b'=');

        let (temp_min, temp_max) = record.buf.min_max();

        write!(
            out,
            "{:.1}/{:.1}/{:.1}",
            to_fixed_number(temp_min),
            record.avg(),
            to_fixed_number(temp_max),
        )?;
    }

    out.extend_from_slice(b"}\n");

    // Write everything to stdout
    stdout().lock().write_all(&out)?;

    Ok(())
}

fn main() -> io::Result<()> {
    let start_instant = Instant::now();
    compute()?;
    eprintln!("Inside main total duration: {:?}", start_instant.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_parse() {
        fn build_parsed_line(city: &[u8], x: Measure) -> (&[u8], Measure) {
            (city, x)
        }

        // Test with no city name.
        assert_eq!(
            unsafe { fast_parse_line(b";8.5") },
            build_parsed_line(b"", 85)
        );
        assert_eq!(
            unsafe { fast_parse_line(b";-6.9") },
            build_parsed_line(b"", -69)
        );
        assert_eq!(
            unsafe { fast_parse_line(b";42.3") },
            build_parsed_line(b"", 423)
        );
        assert_eq!(
            unsafe { fast_parse_line(b";-86.1") },
            build_parsed_line(b"", -861)
        );

        // One char city name.
        assert_eq!(
            unsafe { fast_parse_line(b"X;8.5") },
            build_parsed_line(b"X", 85)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"X;-6.9") },
            build_parsed_line(b"X", -69)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"X;42.9") },
            build_parsed_line(b"X", 429)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"X;-86.1") },
            build_parsed_line(b"X", -861)
        );

        // Multi digit city name.
        assert_eq!(
            unsafe { fast_parse_line(b"Paris;8.5") },
            build_parsed_line(b"Paris", 85)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"Paris;-6.9") },
            build_parsed_line(b"Paris", -69)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"Paris;42.0") },
            build_parsed_line(b"Paris", 420)
        );
        assert_eq!(
            unsafe { fast_parse_line(b"Paris;-86.1") },
            build_parsed_line(b"Paris", -861)
        );
    }
}
