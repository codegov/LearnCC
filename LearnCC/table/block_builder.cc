// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb
{
    /******************************************************************
     block的结构：
     ——————————————————————————————————
     Record1          // 记录的数据：一个key和value键值对；记录的结构: key共享长度 | key非共享长度 | value长度 | key非共享内容 | value内容
     ——————————————————————————————————
     Record2
     ——————————————————————————————————
     Record3
     ——————————————————————————————————
     ......
     ——————————————————————————————————
     Record17
     ——————————————————————————————————
     Record18
     ——————————————————————————————————
     ......           // 更多记录
     ——————————————————————————————————
     Restart[0]       // 重启点存储的是记录Record1的偏移量  占4个字节
     ——————————————————————————————————
     Restart[1]       // 重启点存储的是记录Record17的偏移量 占4个字节
     ——————————————————————————————————
     ......           // 更多重启点
     ——————————————————————————————————
     num_restarts:2   // 重启点Restart的数量 占4个字节
     ——————————————————————————————————
     注：
     1、记录Record1、Record2、Record3、...、Record17、Record18、...
        在代码里表现是：字符串buffer_里存储一段段子字符串；
        Record1和Record17是重启点记录，则它们的key共享长度为0，key非共享长度为key的长度，key非共享内容为key的内容。
        Record2到Record16的key都共享了Record1的key。假设它们的key与Record1的key,前面3个字符相同，则它们的key共享长度为3，key非共享长度为key的长度减去3，key非共享内容为key第3个字符之后的内容
     2、重启点Restart[0]、Restart[1]、...
        在代码里表现是：整数容器restarts_里存储的元素；
        Restart[0]存储的是记录Record1的偏移量，Restart[1]存储的是记录Record17的偏移量。
     3、重启点Restart的数量num_restarts
        在代码里表现是：整数容器restarts_的元素个数。
     4、默认每隔16条记录生成一个重启点，当然也可以通过options_->block_restart_interval改变间隔多少条生成重启点。
     
     ******************************************************************/
    
    BlockBuilder::BlockBuilder(const Options* options) : options_(options), restarts_(), counter_(0), finished_(false)
    {
        assert(options->block_restart_interval >= 1); // block_restart_interval条记录一个重启点，默认16
        restarts_.push_back(0);       // First restart point is at offset 0
    }
    
    void BlockBuilder::Reset()
    {
        buffer_.clear();
        restarts_.clear();
        restarts_.push_back(0);       // First restart point is at offset 0
        counter_ = 0;
        finished_ = false;
        last_key_.clear();
    }
    
    size_t BlockBuilder::CurrentSizeEstimate() const
    {
        return (buffer_.size() +                        // Raw data buffer
                restarts_.size() * sizeof(uint32_t) +   // Restart array
                sizeof(uint32_t));                      // Restart array length
    }
    
    Slice BlockBuilder::Finish()
    {
        // Append restart array
        for (size_t i = 0; i < restarts_.size(); i++)
        {
            PutFixed32(&buffer_, restarts_[i]);
        }
        PutFixed32(&buffer_, restarts_.size());
        finished_ = true;
        return Slice(buffer_);
    }
    
    void BlockBuilder::Add(const Slice& key, const Slice& value)
    {
        Slice last_key_piece(last_key_);
        assert(!finished_);
        assert(counter_ <= options_->block_restart_interval);
        assert(buffer_.empty() // No values yet?
               || options_->comparator->Compare(key, last_key_piece) > 0); // buffer为空，则可立马添加记录；buffer不为空，则记录必须满足大小顺序添加
        size_t shared = 0;
        if (counter_ < options_->block_restart_interval)
        {
            // See how much sharing to do with previous string
            const size_t min_length = std::min(last_key_piece.size(), key.size());
            while ((shared < min_length) && (last_key_piece[shared] == key[shared]))
            {
                shared++;
            }
        } else
        {
            // Restart compression
            restarts_.push_back(buffer_.size());//16条记录一个重启点，重启点记录记录的偏移量
            counter_ = 0;
        }
        const size_t non_shared = key.size() - shared;
        
        // Add "<shared><non_shared><value_size>" to buffer_
        PutVarint32(&buffer_, shared);
        PutVarint32(&buffer_, non_shared);
        PutVarint32(&buffer_, value.size());
        
        // Add string delta to buffer_ followed by value
        buffer_.append(key.data() + shared, non_shared);
        buffer_.append(value.data(), value.size());
        
        // Update state
        last_key_.resize(shared);
        last_key_.append(key.data() + shared, non_shared);
        assert(Slice(last_key_) == key);
        counter_++;
    }
    
}  // namespace leveldb
