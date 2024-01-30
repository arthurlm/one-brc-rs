use std::arch::x86_64::*;

#[derive(Debug)]
pub struct SimdBuffer {
    data: [i16; Self::MAX_LEN],
    len: u8,
}

impl SimdBuffer {
    const MAX_LEN: usize = 16;

    #[inline(always)]
    pub fn with_value(value: i16) -> Self {
        Self {
            // It is cheeper to init the vector with the same value everywhere thanks
            // to SIMD instruction.
            data: [value; Self::MAX_LEN],
            len: 1,
        }
    }

    #[inline(always)]
    #[allow(clippy::len_without_is_empty)]
    pub const fn len(&self) -> usize {
        debug_assert!(self.len <= (Self::MAX_LEN as u8));
        self.len as usize
    }

    #[inline(always)]
    pub const fn is_full(&self) -> bool {
        self.len >= (Self::MAX_LEN as u8)
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[i16] {
        unsafe { self.data.get_unchecked(..self.len()) }
    }

    #[inline(always)]
    pub fn add(&mut self, value: i16) {
        if !self.is_full() {
            self.data[self.len()] = value;
            self.len += 1;
        }
    }

    #[inline(always)]
    pub fn min_max(&self) -> (i16, i16) {
        unsafe {
            let data_i16 = _mm256_loadu_epi16(self.data.as_ptr());
            let data_i32 = _mm512_cvtepi16_epi32(data_i16);
            let min_value = _mm512_reduce_min_epi32(data_i32);
            let max_value = _mm512_reduce_max_epi32(data_i32);
            (min_value as i16, max_value as i16)
        }
    }

    pub fn simplify_and_add(&mut self, value: i16) {
        let (min, max) = self.min_max();

        // It is cheeper to init the vector with the same value everywhere thanks
        // to SIMD instruction.
        *self = Self::with_value(value);
        self.data[1] = min;
        self.data[2] = max;
        self.len = 3;
    }
}
