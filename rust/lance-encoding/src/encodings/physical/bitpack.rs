use bytes::{Bytes, BytesMut};
use futures::future::{BoxFuture, FutureExt};
use log::trace;

use lance_core::Result;

use crate::decoder::{PhysicalPageDecoder, PhysicalPageScheduler};

// A physical scheduler for bitpacked buffers
#[derive(Debug, Clone, Copy)]
pub struct BitpackedScheduler {
    bits_per_value: u64,
    uncompressed_bits_per_value: u64,
    buffer_offset: u64,
}

impl BitpackedScheduler {
    pub fn new(bits_per_value: u64, uncompressed_bits_per_value: u64, buffer_offset: u64) -> Self {
        Self {
            bits_per_value,
            uncompressed_bits_per_value,
            buffer_offset,
        }
    }
}

impl PhysicalPageScheduler for BitpackedScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u32>],
        scheduler: &dyn crate::EncodingsIo,
    ) -> BoxFuture<'static, Result<Box<dyn PhysicalPageDecoder>>> {
        let mut min = u64::MAX;
        let mut max = 0;

        let byte_ranges = ranges
            .iter()
            .map(|range| {
                let start_byte_offset = range.start as u64 * self.bits_per_value / 8;
                let mut end_byte_offset = range.end as u64 * self.bits_per_value / 8;
                if range.end as u64 * self.bits_per_value % 8 != 0 {
                    // If the end of the range is not byte-aligned, we need to read one more byte
                    end_byte_offset += 1;
                }

                let start = self.buffer_offset + start_byte_offset;
                let end = self.buffer_offset + end_byte_offset;
                min = min.min(start);
                max = max.max(end);

                start..end
            })
            .collect::<Vec<_>>();

        trace!(
            "Scheduling I/O for {} ranges spread across byte range {}..{}",
            byte_ranges.len(),
            min,
            max
        );

        let bytes = scheduler.submit_request(byte_ranges);

        let bits_per_value = self.bits_per_value;
        let uncompressed_bits_per_value = self.uncompressed_bits_per_value;
        async move {
            let bytes = bytes.await?;
            Ok(Box::new(BitpackedPageDecoder {
                bits_per_value,
                uncompressed_bits_per_value,
                data: bytes,
            }) as Box<dyn PhysicalPageDecoder>)
        }
        .boxed()
    }
}

struct BitpackedPageDecoder {
    bits_per_value: u64,
    uncompressed_bits_per_value: u64,
    data: Vec<Bytes>,
}

impl PhysicalPageDecoder for BitpackedPageDecoder {
    fn update_capacity(
        &self,
        // TODO handle rows to skip
        _rows_to_skip: u32,
        num_rows: u32,
        buffers: &mut [(u64, bool)],
        // TODO handle all nulls
        _all_null: &mut bool,
    ) {
        // TODO -- not sure if this is correct
        buffers[0].0 = self.uncompressed_bits_per_value / 8 * num_rows as u64;
        buffers[0].1 = true;
    }

    fn decode_into(&self, rows_to_skip: u32, num_rows: u32, dest_buffers: &mut [BytesMut]) {
        let mut bytes_to_skip = rows_to_skip as u64 * self.bits_per_value / 8;
        let mut rows_taken = 0;
        let byte_len = self.uncompressed_bits_per_value / 8;
        let dst = &mut dest_buffers[0];
        let mut dst_idx = dst.len();

        // pre-add enough capacity to the buffer to hold all the values we're about to put in it
        let capacity_to_add = dst.capacity() as i64 - dst.len() as i64 + num_rows as i64;
        if capacity_to_add > 0 {
            let bytes_to_add = capacity_to_add as usize * self.uncompressed_bits_per_value as usize / 8;
            dst.extend((0..bytes_to_add).into_iter().map(|_| 0));
        }

        let mut mask = 0u64;
        for _ in 0..self.bits_per_value {
            mask = mask << 1 | 1;
        }

        for src in &self.data {
            let buf_len = src.len() as u64;
            if bytes_to_skip > buf_len {
                bytes_to_skip -= buf_len;
                continue;
            }

            // start at the first offset we're not skipping
            let mut src_idx = bytes_to_skip as usize;
            let mut src_offset = 0;
            while src_idx < src.len() && rows_taken < num_rows {
                rows_taken += 1;
                let mut curr_mask = mask;
                let mut curr_src = src[src_idx] >> src_offset & curr_mask as u8;
                let mut src_bits_written = 0;
                let mut dst_offset = 0;

                while src_bits_written < self.bits_per_value {
                    dst[dst_idx] += (curr_src) << dst_offset;
                    let bits_written = (self.bits_per_value - src_bits_written)
                        .min(8 - src_offset)
                        .min(8 - dst_offset);
                    src_bits_written += bits_written;
                    dst_offset += bits_written;
                    src_offset += bits_written;

                    if dst_offset == 8 {
                        dst_idx += 1;
                        dst_offset = 0;
                    }

                    if src_offset == 8 {
                        src_idx += 1;
                        src_offset = 0;
                        curr_mask >>= 8;
                        if src_idx == src.len() {
                            break;
                        }
                        curr_src = src[src_idx] & curr_mask as u8;
                    }
                }

                // advance source_offset to the next byte if we're not at the end..
                // note that we don't need to do this if we wrote the full number of bits
                // because source index would have been advanced by the inner loop above
                if self.uncompressed_bits_per_value != self.bits_per_value {
                    let mut partial_bytes_written = self.bits_per_value / 8;

                    // if we didn't write the full byte for the last byte, increment by one because
                    // we wrote a partial byte
                    if self.uncompressed_bits_per_value % self.bits_per_value != 0 {
                        partial_bytes_written += 1;
                    }
                    dst_idx += (byte_len as u64 - partial_bytes_written + 1) as usize;
                }
            }
        }
        
    }

    fn num_buffers(&self) -> u32 {
        // TODO ask weston what this is about
        1
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_decode_into() {
        let src_buffer = vec![ 0b11_010_001, 0b0_101_100_0, 0b111_11 ];
        let unit = BitpackedPageDecoder{
            bits_per_value: 3,
            uncompressed_bits_per_value: 32,
            data: vec![Bytes::copy_from_slice(&src_buffer)],
        };

        let mut dest = vec![BytesMut::new()];
        unit.decode_into(0, 7, &mut dest);

        println!("{:?}", dest);

    }
}
