// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow::{
    array::ArrayData,
    datatypes::{ArrowPrimitiveType, UInt16Type, UInt32Type, UInt64Type, UInt8Type},
};
use arrow_array::{cast::AsArray, Array, ArrayRef, PrimitiveArray};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType;
use num_traits::{AsPrimitive, PrimInt};
use snafu::{location, Location};

use lance_arrow::DataTypeExt;
use lance_core::{Error, Result};

use crate::{
    encoder::{BufferEncoder, EncodedBuffer},
    format::pb,
};

#[derive(Debug, Default)]
pub struct FlatBufferEncoder {}

impl BufferEncoder for FlatBufferEncoder {
    fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedBuffer> {
        let data_type = arrays[0].data_type();
        let bits_per_value = 8 * data_type.byte_width() as u64;
        let parts = arrays
            .iter()
            .map(|arr| arr.to_data().buffers()[0].clone())
            .collect::<Vec<_>>();
        Ok(EncodedBuffer {
            bits_per_value,
            parts,
            bitpack_meta: None,
        })
    }
}

// Encoder for writing boolean arrays as dense bitmaps
#[derive(Debug, Default)]
pub struct BitmapBufferEncoder {}

impl BufferEncoder for BitmapBufferEncoder {
    fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedBuffer> {
        debug_assert!(arrays
            .iter()
            .all(|arr| *arr.data_type() == DataType::Boolean));
        let num_rows: u32 = arrays.iter().map(|arr| arr.len() as u32).sum();
        // Empty pages don't make sense, this should be prevented before we
        // get here
        debug_assert_ne!(num_rows, 0);
        // We can't just write the inner value buffers one after the other because
        // bitmaps can have junk padding at the end (e.g. a boolean array with 12
        // values will be 2 bytes but the last four bits of the second byte are
        // garbage).  So we go ahead and pay the cost of a copy (we could avoid this
        // if we really needed to, at the expense of more complicated code and a slightly
        // larger encoded size but writer cost generally doesn't matter as much as reader cost)
        let mut builder = BooleanBufferBuilder::new(num_rows as usize);
        for arr in arrays {
            let bool_arr = arr.as_boolean();
            builder.append_buffer(bool_arr.values());
        }
        let buffer = builder.finish().into_inner();
        let parts = vec![buffer];
        let buffer = EncodedBuffer {
            bits_per_value: 1,
            parts,
            bitpack_meta: None,
        };
        Ok(buffer)
    }
}

#[derive(Debug, Default)]
pub struct BitpackingBufferEncoder {}

impl BufferEncoder for BitpackingBufferEncoder {
    fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedBuffer> {
        let mut num_bits = 0;
        for arr in arrays {
            let arr_max = compute_num_bits(arr.clone())?;
            num_bits = num_bits.max(arr_max);
        }

        // TODO handle case where there are no arrays or all the arrays are full of zeros
        if num_bits == 0 {
            panic!("TODO handle zero length bitpacking")
        }

        let mut packed_arrays = vec![];
        for arr in arrays {
            let packed = pack_array(arr.clone(), num_bits)?;
            packed_arrays.push(packed.into());
        }

        let data_type = arrays[0].data_type();
        let bits_per_value = 8 * data_type.byte_width() as u64;

        Ok(EncodedBuffer {
            bits_per_value: num_bits,
            parts: packed_arrays,
            bitpack_meta: Some(pb::BitpackMeta {
                uncompressed_bits_per_value: bits_per_value,
            }),
        })
    }
}

// TODO write some unit tests for this
fn compute_num_bits(arr: ArrayRef) -> Result<u64> {
    let max = match arr.data_type() {
        DataType::UInt8 => num_bits_for_type::<UInt8Type>(arr.as_primitive()),
        DataType::UInt16 => num_bits_for_type::<UInt16Type>(arr.as_primitive()),
        DataType::UInt32 => num_bits_for_type::<UInt32Type>(arr.as_primitive()),
        DataType::UInt64 => num_bits_for_type::<UInt64Type>(arr.as_primitive()),
        _ => None,
    };

    max.ok_or(Error::InvalidInput {
        source: format!(
            "Invalid type supplied to compute bits for bitpacking: {}",
            arr.data_type()
        )
        .into(),
        location: location!(),
    })
}

fn num_bits_for_type<T>(arr: &PrimitiveArray<T>) -> Option<u64>
where
    T: ArrowPrimitiveType,
    T::Native: PrimInt + AsPrimitive<u64>,
{
    let max = arrow::compute::bit_or(arr);
    max.map(|x| arr.data_type().byte_width() as u64 * 8 - x.leading_zeros() as u64)
}

fn pack_array(arr: ArrayRef, num_bits: u64) -> Result<Vec<u8>> {
    match arr.data_type() {
        DataType::UInt8 | DataType::UInt32 => Ok(pack_buffers(
            arr.to_data(),
            num_bits,
            arr.data_type().byte_width(),
        )),
        _ => Err(Error::InvalidInput {
            source: format!("Invalid data type for bitpacking: {}", arr.data_type()).into(),
            location: location!(),
        }),
    }
}

fn pack_buffers(data: ArrayData, num_bits: u64, byte_len: usize) -> Vec<u8> {
    let buffers = data.buffers();
    let mut packed_buffers = vec![];
    for buffer in buffers {
        let packed_buffer = pack_bits(&buffer, num_bits, byte_len);
        println!("PACKED BUFFER: {:?}", packed_buffer);
        packed_buffers.push(packed_buffer);
    }
    packed_buffers.concat()
}

fn pack_bits(src: &[u8], num_bits: u64, byte_len: usize) -> Vec<u8> {
    // calculate the total number of bytes we need to allocate for the destination.
    // this will be the number of items in the source array times the number of bits.
    let src_items = src.len() / byte_len as usize;
    let mut dst_bytes_total = src_items * num_bits as usize / 8;

    // if if there's a partial byte at the end, we need to allocate one more byte
    if (src_items * num_bits as usize) % 8 != 0 {
        dst_bytes_total += 1;
    }

    let mut dst = vec![0u8; dst_bytes_total];
    let mut dst_idx = 0;
    let mut dst_offset = 0;
    let bit_len = byte_len as u64 * 8;

    let mut mask = 0u64;
    for _ in 0..num_bits {
        mask = mask << 1 | 1;
    }

    let mut src_idx = 0;
    while src_idx < src.len() {
        let mut curr_mask = mask;
        let mut curr_src = src[src_idx] & curr_mask as u8;
        let mut src_offset = 0;
        let mut src_bits_written = 0;

        while src_bits_written < num_bits {
            dst[dst_idx] += (curr_src >> src_offset) << dst_offset;
            let bits_written = (num_bits - src_bits_written)
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
        if bit_len != num_bits {
            let mut partial_bytes_written = num_bits / 8;

            // if we didn't write the full byte for the last byte, increment by one because
            // we wrote a partial byte
            if bit_len % num_bits != 0 {
                partial_bytes_written += 1;
            }
            src_idx += (byte_len as u64 - partial_bytes_written + 1) as usize;
        }
    }

    dst
}

#[cfg(test)]
pub mod test {
    use super::*;

    use arrow_array::{Int32Array, UInt32Array};
    use std::sync::Arc;

    #[test]
    fn test_bitpacking_encoder() {
        // TODO -- this is kind of a useless test
        let arr1 = UInt32Array::from_iter(vec![1, 2, 3]);
        let encoder = BitpackingBufferEncoder {};
        let arrs = vec![Arc::new(arr1) as ArrayRef];
        let result = encoder.encode(&arrs);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_bitpacking_encoder1() {
        // TODO should refactor this into a test about an unsupported datatype
        let arr1 = Int32Array::from_iter(vec![1, 2, 3, -1, -2, -3]);
        let encoder = BitpackingBufferEncoder {};
        let arrs = vec![Arc::new(arr1) as ArrayRef];
        let result = encoder.encode(&arrs);
        println!("{:?}", result);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_pack_bits_less_than_one_byte() {
        let src = UInt32Array::from_iter(vec![1, 2, 3, 4, 5, 6, 7]);
        let data = src.to_data();
        let num_bits = 3;
        let buffer = &data.buffers()[0];
        let result = pack_bits(&buffer, num_bits, 4);

        let result_str: Vec<String> = result.iter().map(|x| format!("{:08b}", x)).collect();
        let expected_str = vec![0b11_010_001, 0b0_101_100_0, 0b111_11]
            .iter()
            .map(|x| format!("{:08b}", x))
            .collect::<Vec<String>>();

        assert_eq!(result_str, expected_str);
    }

    #[test]
    fn test_pack_where_aligned_with_datatype() {
        let src = UInt32Array::from_iter(vec![0x01020304, 0x05060708]);
        let data = src.to_data();
        let num_bits = 32;
        let buffer = &data.buffers()[0];
        let result = pack_bits(&buffer, num_bits, 4);
        let mut expected = vec![];
        for i in buffer.into_iter() {
            expected.push(*i);
        }
        assert_eq!(result, expected);
    }
}
