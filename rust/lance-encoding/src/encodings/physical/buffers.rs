// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{fmt::Debug, ops::{self, AddAssign, BitAnd, BitOr, Shl, ShrAssign}};

use arrow::datatypes::{ArrowPrimitiveType, UInt8Type, UInt32Type};
use arrow_array::{cast::AsArray, Array, ArrayRef, PrimitiveArray};

use num_traits::{One, Zero, FromPrimitive, PrimInt, AsPrimitive};

use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::DataType;
use lance_arrow::DataTypeExt;
use lance_core::Result;

use crate::encoder::{BufferEncoder, EncodedBuffer};

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
        Ok(EncodedBuffer { bits_per_value, parts })
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
        let buffer = EncodedBuffer { bits_per_value: 1, parts };
        Ok(buffer)
    }
}

#[derive(Debug, Default)]
pub struct BitpackingBufferEncoder {}

impl BufferEncoder for BitpackingBufferEncoder {
    fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedBuffer> {
        let mut num_bits = arrays.iter().filter_map(|arr| {
            // TODO -- here we maybe want to handle if we can't figure out the
            // number of bits -- e.g. it's a bad datatype or some other problem
            num_bits(arr.clone())
        })
        .max()
        .unwrap(); // TODO nounwrap
        
        // TODO handle case where there are no arrays or all the arrays are full of zeros
        if num_bits == 0 {
            panic!("TODO handle zero length bitpacking")
        }

        let mut packed_arrays = vec![];
        for arr in arrays {
            // let num_bits = num_bits(arr.clone()).unwrap();
            let packed = pack_bits_for_type(arr.clone(), num_bits);
            packed_arrays.push(packed.into());
        }

        Ok(EncodedBuffer{
            bits_per_value: num_bits,
            parts: packed_arrays,
        })
        // let mask = 

        // let bytes = arrays[0].to_data().buffers()[0].clone();
        // let mut val_byte_chunk = bytes.chunks(byte_width);
        // let chunk = val_byte_chunk.next().unwrap();
        // let mb = min_bits(chunk);

        // // arrays[0].as_primitive()
        // let byte2: &[u8] = bytes.typed_data();
        // println!("{:?}", byte2);
        // byte2[0].leading_zeros();
        // todo!()
    }
}

// TODO write some unit tests for this
fn num_bits(arr: ArrayRef) -> Option<u64> {
    match arr.data_type() {
        DataType::UInt8 => {
            let arr: &PrimitiveArray<UInt8Type> = arr.as_primitive();
            let max = arrow::compute::bit_or(arr);
            return max.map(|x| 8 - x.leading_zeros() as u64);
        },
        // TODO other datatypes incl signed
        DataType::UInt32 => {
            let arr: &PrimitiveArray<UInt32Type> = arr.as_primitive();
            let max = arrow::compute::bit_or(arr);
            return max.map(|x| 32 - x.leading_zeros() as u64);
        },
        _ => None,
    }
}

fn pack_bits_for_type(arr: ArrayRef, num_bits: u64) -> Vec<u8> {
    match arr.data_type() {
        DataType::UInt8 => {
            let arr: &PrimitiveArray<UInt8Type> = arr.as_primitive();
            // let buffers = arr.to_data().buffers();
            let arr_data = arr.to_data();
            let buffers = arr_data.buffers();
            let mut packed_buffers = vec![];
            for buffer in buffers {
                let packed_buffer = pack_arrow_bits(&buffer, num_bits);
                packed_buffers.push(packed_buffer);
            }
            let packed_buffers =packed_buffers.concat();
            return packed_buffers;
        },
        // TODO other signed datatypes
        DataType::UInt32 => {
            println!("arr {:?}, num_bits {:?}", arr, num_bits);
            // let arr: &PrimitiveArray<UInt32Type> = arr.as_primitive();
            let arr_data = arr.to_data();
            println!("arr_data type = {:?}", arr_data.data_type());
            let buffers = arr_data.buffers();
            let mut packed_buffers = vec![];
            for buffer in buffers {
                let packed_buffer = pack_bits_again_2(&buffer, num_bits, 4);
                packed_buffers.push(packed_buffer);
            }
            let packed_buffers =packed_buffers.concat();
            return packed_buffers;
        },
        _ => panic!("Unsupported datatype"),
    }
}

fn pack_bits(src: Vec<u32>, num_bits: u64) -> Vec<u8> {
    let mut dst = vec![0u8; (src.len() * num_bits as usize ) / 8 + 1];
    let dst_bit_len = 8;
    let mut dst_idx = 0;
    let mut dst_offset = 0;

    let mut mask = 0;
    for _ in 0..num_bits {
        mask = (mask << 1) | 1;
    }

    for src_idx in 0..src.len() {
        let mut curr_src = src[src_idx] & mask;
        let mut src_bits_written = 0;

        while src_bits_written < num_bits {
            dst[dst_idx] += (curr_src << dst_offset) as u8;
            let bits_written = (num_bits - src_bits_written).min(dst_bit_len - dst_offset);
            src_bits_written += bits_written;
            dst_offset += bits_written;
            curr_src >>= bits_written;

            if dst_offset == dst_bit_len {
                dst_idx += 1;
                dst_offset = 0;
            }
        }
    }

    dst
}

// fn pack_arrow_bits_2<T>(arr: PrimitiveArray<T>, num_bits: u64) -> Vec<u8> 
// where
//     T: ArrowPrimitiveType
// {
//     let buffers = arr.to_data().buffers();
//     let mut packed_buffers = vec![];
//     for buffer in buffers {
//         let packed_buffer = pack_arrow_bits(&buffer, num_bits);
//         packed_buffers.push(packed_buffer);
//     }
//     let packed_buffers =packed_buffers.concat();
//     return packed_buffers;
// }

fn pack_arrow_bits<T>(src: &[T], num_bits: u64) -> Vec<u8>
where
    // TODO, don't really need Debug here
    T: num_traits::PrimInt + FromPrimitive + AsPrimitive<u8> + ShrAssign<u64> + Debug,
{
    let mut dst = vec![0u8; (src.len() * num_bits as usize ) / 8 + 1];
    let dst_bit_len: u64 = 8;
    let mut dst_idx = 0;
    let mut dst_offset: u64 = 0;

    let mut mask: T = FromPrimitive::from_u8(0).unwrap();
    for _ in 0..num_bits {
        mask = mask << 1 | FromPrimitive::from_u8(1).unwrap();
    }

    for src_idx in 0..src.len() {
        let mut curr_src = src[src_idx] & mask;
        let mut src_bits_written = 0;

        while src_bits_written < num_bits {
            // let tmp = 
            // let tmp2:u8 = tmp.as_();
            dst[dst_idx] += (curr_src << dst_offset.as_()).as_() as u8;
            let bits_written = (num_bits - src_bits_written).min(dst_bit_len - dst_offset);
            src_bits_written += bits_written;
            dst_offset += bits_written;
            println!("{:?} {}", curr_src, bits_written);
            curr_src >>= bits_written;

            if dst_offset == dst_bit_len {
                dst_idx += 1;
                dst_offset = 0;
            }
        }
    }
    dst
}

fn pack_bits_again_2(src: &[u8], num_bits: u64, byte_len: u64) -> Vec<u8> {
    let len = src.len() * num_bits as usize / (byte_len as usize * 8) + 1;
    let mut dst = vec![0u8; len];
    let mut dst_idx = 0;
    let mut dst_offset = 0;

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
            let bits_written = (num_bits - src_bits_written).min(8 - src_offset).min(8 - dst_offset);
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
                    break
                }
                curr_src = src[src_idx] & curr_mask as u8;
            }
        }

        // TODO -- advance source_offset to the next byte if we're not at the end
        let mut bytes_written = num_bits / (byte_len * 8);
        if byte_len % num_bits != 0 {
            bytes_written += 1;
        }
        println!("{} - {}", byte_len, bytes_written);
        src_idx += (byte_len - bytes_written + 1) as usize;
    }

    dst
}


#[cfg(test)]
pub mod test {
    use super::*;
    
    use std::sync::Arc;
    use arrow_array::{Int32Array, UInt32Array};

    #[test]
    fn test_2() {
        let mut x: u32 = 97;
        x >>= 8u64;
        println!("{}", x);
    }

    #[test]
    fn test_bitpacking_encoder() {
        let arr1 = UInt32Array::from_iter(vec![1, 2, 3]);
        let encoder = BitpackingBufferEncoder{};
        let arrs = vec![Arc::new(arr1) as ArrayRef];
        let result = encoder.encode(&arrs);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_bitpacking_encoder1() {
        let arr1 = Int32Array::from_iter(vec![1, 2, 3, -1, -2, -3]);
        let encoder = BitpackingBufferEncoder{};
        let arrs = vec![Arc::new(arr1) as ArrayRef];
        let result = encoder.encode(&arrs);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_pack_bits() {
        // let src = vec![1, 2, 3, 4, 5, 6, 7];
        let src = UInt32Array::from_iter(vec![1, 2, 3, 4, 5, 6, 7]);
        let data = src.to_data();
        let num_bits = 3;
        // let result = pack_bits(src, num_bits);
        let buffer = &data.buffers()[0];
        let result = pack_bits_again_2(&buffer, num_bits, 4);
        
        let result_str: Vec<String> = result.iter().map(|x| format!("{:08b}", x)).collect();
        let expected_str = vec![
            0b11_010_001,
            0b0_101_100_0,
            0b111_11].iter().map(|x| format!("{:08b}", x)).collect::<Vec<String>>();

        assert_eq!(result_str, expected_str);
    }
}
