// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb
{
    /*************************************************************************
     Footer的结构：               // 大小：20+20+8=48字节
     ——————————————————————————————————————————
     metaindex_handle           // 结构：offset | size  注：即是metablock index的起始位置和大小 最大占20个字节
     ——————————————————————————————————————————
     index_handle               // 结构：offset | size  注：即是index Block的起始位置和大小 最大占20个字节
     ——————————————————————————————————————————
     padding                    // 填充部分，即20个字节的空间减去metaindex_handle所占空间 加上 20个字节的空间减去index_handle所占空间
     ——————————————————————————————————————————
     kTableMagicNumber          // 魔数 占8个字节
     ——————————————————————————————————————————
     
     
     *************************************************************************更多关注文件block_bulider.cc里指定的block结构
     Block的结构：
     ——————————————————————————————————————————
     record1          // 记录的数据：一个key和value键值对；记录的结构: key共享长度 | key非共享长度 | value长度 | key非共享内容 | value内容
     ——————————————————————————————————————————
     record2
     ——————————————————————————————————————————
     record3
     ——————————————————————————————————————————
     ......
     ——————————————————————————————————————————
     record17
     ——————————————————————————————————————————
     record18
     ——————————————————————————————————————————
     ......           // 更多记录
     ——————————————————————————————————————————
     restart[0]       // 重启点存储的是记录record1的偏移量  占4个字节
     ——————————————————————————————————————————
     restart[1]       // 重启点存储的是记录record17的偏移量 占4个字节
     ——————————————————————————————————————————
     ......           // 更多重启点
     ——————————————————————————————————————————
     num_restarts:2   // 重启点restart的数量 占4个字节
     ——————————————————————————————————————————
     
     
     **************************************************************************
     FilterBlock的结构：
     ——————————————————————————————————————————
     filter1             // 由n个key生产的过滤器1，即一段字符串
     ——————————————————————————————————————————
     filter2             // 由n个key生产的过滤器2，即一段字符串
     ——————————————————————————————————————————
     ......              // 更多过滤器
     ——————————————————————————————————————————
     filter1.offset      // 过滤器1距离首地址的大小，即偏移量 占四个字节
     ——————————————————————————————————————————
     filter2.offset      // 过滤器2距离首地址的大小，即偏移量 占四个字节
     ——————————————————————————————————————————
     ......              // 更多过滤器的偏移量
     ——————————————————————————————————————————
     filters.size        // 所有过滤器的总大小 占四个字节
     ——————————————————————————————————————————
     kFilterBaseLg       // 基值，值为11 占一个字节
     ——————————————————————————————————————————
     
     
     *************************************************************************
     IndexBlock的结构：
     ——————————————————————————————————————————
     index1          // 索引的结构：key | offset | size  注：索引index1是SSTable.sst文件结构里dataBlock1或metaBlock1的索引；
     ——————————————————————————————————————————
     index2                                            注：key：dataBlock1或metaBlock1给定的一段字符串
     ——————————————————————————————————————————
     index3                                            注：offset：dataBlock1或metaBlock1在文件SSTable.sst的偏移量；
     ——————————————————————————————————————————
     ......                                            注：size：dataBlock1或metaBlock1的大小。
     ——————————————————————————————————————————
     
     
     *************************************************************************
     SSTable.sst文件的结构
     ——————————————————————————————————————————
     dataBlock1               // 数据块的结构：block | type | crc 注：block：Block的结构，key在block里是有序存储的
     ——————————————————————————————————————————
     dataBlock2                                                 注：type：否压缩类型 占一个字节
     ——————————————————————————————————————————
     dataBlock3                                                 注：crc：数据校验码 占4个字节
     ——————————————————————————————————————————
     ......                  // 更多KV数据块
     ——————————————————————————————————————————
     metaBlock1              // 数据块的结构：filterBlock | type | crc 注：filterBlock：FilterBlock的结构
     ——————————————————————————————————————————
     metaBlock2
     ——————————————————————————————————————————
     ......                  // 更多metaBlock
     ——————————————————————————————————————————
     metaBlock index         // metaBlock的索引，其中的过滤器metaBlock1对应的索引，索引中的key是"filter."加上过滤器的name；基体结构是IndexBlock的结构
     ——————————————————————————————————————————
     index block             // dataBlock的索引，其中的数据块dataBlock1对应的索引，索引中的key是大于等于dataBlock1.lastKey的最小值，且小于dataBlock2.firstKey；基体结构是IndexBlock的结构
     ——————————————————————————————————————————
     footer                  // 文件尾部 即是Footer的结构
     ——————————————————————————————————————————
     
     *************************************************************************/
    
    void BlockHandle::EncodeTo(std::string* dst) const
    {
        // Sanity check that all fields have been set
        assert(offset_ != ~static_cast<uint64_t>(0));
        assert(size_ != ~static_cast<uint64_t>(0));
        PutVarint64(dst, offset_);
        PutVarint64(dst, size_);
    }
    
    Status BlockHandle::DecodeFrom(Slice* input)
    {
        if (GetVarint64(input, &offset_) && GetVarint64(input, &size_))
        {
            return Status::OK();
        } else {
            return Status::Corruption("bad block handle");
        }
    }
    
    void Footer::EncodeTo(std::string* dst) const
    {
#ifndef NDEBUG //关闭条件编译调试代码开关(即不编译assert函数)
        const size_t original_size = dst->size();
#endif
        metaindex_handle_.EncodeTo(dst);
        index_handle_.EncodeTo(dst);
        dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding 将剩下的空间填充
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu)); //添加低位：32位占4个字节
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32)); //添加高位：4个字节
        assert(dst->size() == original_size + kEncodedLength);
    }
    
    Status Footer::DecodeFrom(Slice* input)
    {
        const char* magic_ptr = input->data() + kEncodedLength - 8; // 魔数首地址
        const uint32_t magic_lo = DecodeFixed32(magic_ptr);     // 获取低位4个字节
        const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4); // 获取高位4个字节
        // 魔数占8个字节，所以需要使用uint64_t类型。 转换成8位字节的高位向右移动4个字节与转换成8位字节的低位进行或运算，得到8字节的魔数
        const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) | (static_cast<uint64_t>(magic_lo)));
        if (magic != kTableMagicNumber)
        {
            return Status::Corruption("not an sstable (bad magic number)");
        }
        
        Status result = metaindex_handle_.DecodeFrom(input);
        if (result.ok())
        {
            result = index_handle_.DecodeFrom(input);
        }
        if (result.ok())
        {
            // We skip over any leftover data (just padding for now) in "input"
            const char* end = magic_ptr + 8;
            *input = Slice(end, input->data() + input->size() - end);
        }
        return result;
    }
    
    Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result)
    {
        result->data = Slice();
        result->cachable = false;
        result->heap_allocated = false;
        
        // Read the block contents as well as the type/crc footer.
        // See table_builder.cc for the code that built this structure.
        size_t n = static_cast<size_t>(handle.size());
        char* buf = new char[n + kBlockTrailerSize];
        Slice contents;
        Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
        if (!s.ok())
        {
            delete[] buf;
            return s;
        }
        if (contents.size() != n + kBlockTrailerSize)
        {
            delete[] buf;
            return Status::Corruption("truncated block read");
        }
        
        // Check the crc of the type and the block contents
        const char* data = contents.data();    // Pointer to where Read put the data
        if (options.verify_checksums) // 是否需要验证校验
        {
            const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));//记录结构是block+type+crc;所以首地址+block长度+type长度得到crc起始地址
            const uint32_t actual = crc32c::Value(data, n + 1);
            if (actual != crc)
            {
                delete[] buf;
                s = Status::Corruption("block checksum mismatch");
                return s;
            }
        }
        
        switch (data[n]) // type
        {
            case kNoCompression:
                if (data != buf)
                {
                    // File implementation gave us pointer to some other data.
                    // Use it directly under the assumption that it will be live
                    // while the file is open.
                    delete[] buf;
                    result->data = Slice(data, n);
                    result->heap_allocated = false;
                    result->cachable = false;  // Do not double-cache
                } else
                {
                    result->data = Slice(buf, n);
                    result->heap_allocated = true;
                    result->cachable = true;
                }
                
                // Ok
                break;
            case kSnappyCompression:
            {
                size_t ulength = 0;
                if (!port::Snappy_GetUncompressedLength(data, n, &ulength))
                {
                    delete[] buf;
                    return Status::Corruption("corrupted compressed block contents");
                }
                char* ubuf = new char[ulength];
                if (!port::Snappy_Uncompress(data, n, ubuf))
                {
                    delete[] buf;
                    delete[] ubuf;
                    return Status::Corruption("corrupted compressed block contents");
                }
                delete[] buf;
                result->data = Slice(ubuf, ulength);
                result->heap_allocated = true;
                result->cachable = true;
                break;
            }
            default:
                delete[] buf;
                return Status::Corruption("bad block type");
        }
        
        return Status::OK();
    }
    
}  // namespace leveldb
