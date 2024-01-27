#![allow(dead_code)]

use std::{
    cmp,
    collections::hash_map::Entry,
    fs,
    io::{self},
    os::{fd::AsRawFd, raw::c_void},
    slice,
    time::Instant,
};

use fixed::types::I48F16;
use fxhash::FxHashMap;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};

type Number = I48F16;
type Db<'a> = FxHashMap<&'a [u8], Record>;

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
                libc::MAP_PRIVATE | libc::MAP_NONBLOCK,
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

impl Drop for MmapedFile {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr, self.data_len);
        }
    }
}

#[derive(Debug)]
struct Record {
    min: Number,
    max: Number,
    sum: Number,
    count: usize,
}

impl Record {
    fn new(value: Number) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    #[inline(always)]
    fn add(&mut self, value: Number) {
        self.min = cmp::min(self.min, value);
        self.max = cmp::max(self.max, value);
        self.sum += value;
        self.count += 1;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.min.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

#[inline(always)]
fn fast_parse(input: &[u8]) -> Number {
    let (neg, i1, i2, f) = unsafe {
        match (
            *input.get_unchecked(0),
            *input.get_unchecked(1),
            *input.get_unchecked(2),
        ) {
            (b'-', x, b'.') => (true, 0, (x & 0x0F) as i16 * 10, *input.get_unchecked(3)),
            (b'-', x, y) => (
                true,
                (x & 0x0F) as i16 * 100,
                (y & 0x0F) as i16 * 10,
                *input.get_unchecked(4),
            ),
            (x, y, b'.') => (
                false,
                (x & 0x0F) as i16 * 100,
                (y & 0x0F) as i16 * 10,
                *input.get_unchecked(3),
            ),
            (x, _, y) => (false, 0, (x & 0x0F) as i16 * 10, y),
        }
    };

    let output = i1 + i2 + (f & 0x0F) as i16;
    Number::from(if neg { -output } else { output }) / Number::from(10)
}

fn compute() -> io::Result<()> {
    // Open file and memory map it.
    let file = fs::File::open("measurements.txt")?;
    let mmap_file = MmapedFile::from_file(file)?;

    let _db = mmap_file
        // Convert memory mapped file to bytes slice.
        .as_slice()
        // Iterate over each line on a thread pool.
        .par_split(|x| *x == b'\n')
        // Parse each line assuming there are only 2 elements on it.
        .filter_map(|line| {
            let sep_index = line.iter().position(|x| *x == b';')?;
            let city = &line[..sep_index];
            let temp = fast_parse(&line[sep_index + 1..]);
            Some((city, temp))
        })
        // Insert every entry to the DB.
        .fold(
            || Db::default(),
            |mut map, (city, temp)| {
                match map.entry(city) {
                    Entry::Occupied(entry) => {
                        entry.into_mut().add(temp);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Record::new(temp));
                    }
                }

                map
            },
        )
        .reduce(
            || Db::default(),
            |mut map1, map2| {
                for (city, record) in map2 {
                    match map1.entry(city) {
                        Entry::Occupied(entry) => {
                            entry.into_mut().merge(&record);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(record);
                        }
                    }
                }

                map1
            },
        );

    Ok(())
}

fn main() -> io::Result<()> {
    let start_instant = Instant::now();
    compute()?;
    println!("duration {:?}", start_instant.elapsed());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_parse() {
        fn one_digit_fixed(x: i16) -> Number {
            Number::from(x) / Number::from(10)
        }

        assert_eq!(fast_parse(b"8.5"), one_digit_fixed(85));
        assert_eq!(fast_parse(b"-6.9"), one_digit_fixed(-69));
        assert_eq!(fast_parse(b"42.3"), one_digit_fixed(423));
        assert_eq!(fast_parse(b"-86.1"), one_digit_fixed(-861));
    }
}
