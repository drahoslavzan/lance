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
    pub fn new(
        bits_per_value: u64,
        uncompressed_bits_per_value: u64,
        buffer_offset: u64,
    ) -> Self {
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
        _rows_to_skip: u32,
        _num_rows: u32,
        _buffers: &mut [(u64, bool)],
        _all_null: &mut bool,
    ) {
        todo!()
    }

    fn decode_into(&self, _rows_to_skip: u32, _num_rows: u32, _dest_buffers: &mut [BytesMut]) {
        todo!()
    }

    fn num_buffers(&self) -> u32 {
        // TODO ask weston what this is about
        1
    }
}


