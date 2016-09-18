// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {
    
    // See doc/table_format.txt for an explanation of the filter block format.
    
    /********************************************************************************
     filterBlock的结构：
     ————————————————————————————————————————————————
     filter1             // 由n个key生产的过滤器1，即一段字符串
     ————————————————————————————————————————————————
     filter2             // 由n个key生产的过滤器2，即一段字符串
     ————————————————————————————————————————————————
     ......              // 更多过滤器
     ————————————————————————————————————————————————
     filter1.offset      // 过滤器1距离首地址的大小，即偏移量 占四个字节
     ————————————————————————————————————————————————
     filter2.offset      // 过滤器2距离首地址的大小，即偏移量 占四个字节
     ————————————————————————————————————————————————
     ......              // 更多过滤器的偏移量
     ————————————————————————————————————————————————
     filters.size        // 所有过滤器的总大小 占四个字节
     ————————————————————————————————————————————————
     kFilterBaseLg       // 基值，值为11 占一个字节
     ————————————————————————————————————————————————
     注：
     1、过滤器filter1、filter2、...
        在代码里表现是：字符串result_里存储的一段段子字符串；
     2、过滤器偏移量filter1.offset、filter2.offset、...
        在代码里表现是：整数容器filter_offsets_里存储的元素；
     3、所有过滤器的总大小filters.size
        在代码里表现是字符串：result_的长度；
     4、kFilterBaseLg的作用：
        通过每个dataBlock在sstable文件的偏移量block_offset,与基值kFilterBase做除法运算，得到dataBlock在filter偏移量数组的位置，
     也就是得到在filter偏移量数组中的数组下标，则可获取相应的filter偏移量，通过filter偏移量又可找到对应的filter，
     有了filter就可以去判断key是否在filter中，如果key在filter中，则说明也就在dataBlock中。
     ********************************************************************************/
    // Generate new filter every 2KB of data
    static const size_t kFilterBaseLg = 11;
    static const size_t kFilterBase = 1 << kFilterBaseLg; // 2的11方=2048
    
    /*
     FilterBlockBuilder的实现
     */
    
    FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy) : policy_(policy)
    {
    }
    
    void FilterBlockBuilder::StartBlock(uint64_t block_offset)
    {
        uint64_t filter_index = (block_offset / kFilterBase);
        assert(filter_index >= filter_offsets_.size());
        while (filter_index > filter_offsets_.size())
        {
            GenerateFilter();
        }
    }
    
    void FilterBlockBuilder::AddKey(const Slice& key)
    {
        Slice k = key;
        start_.push_back(keys_.size());
        keys_.append(k.data(), k.size());
    }
    
    Slice FilterBlockBuilder::Finish()
    {
        if (!start_.empty())
        {
            GenerateFilter();
        }
        
        // Append array of per-filter offsets
        const uint32_t array_offset = result_.size();
        for (size_t i = 0; i < filter_offsets_.size(); i++)
        {
            PutFixed32(&result_, filter_offsets_[i]);
        }
        
        PutFixed32(&result_, array_offset);
        result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
        return Slice(result_);
    }
    
    void FilterBlockBuilder::GenerateFilter()
    {
        const size_t num_keys = start_.size();
        if (num_keys == 0)
        {
            // Fast path if there are no keys for this filter
            filter_offsets_.push_back(result_.size());
            return;
        }
        
        // Make list of keys from flattened key structure
        start_.push_back(keys_.size());  // Simplify length computation
        tmp_keys_.resize(num_keys);
        for (size_t i = 0; i < num_keys; i++)
        {
            const char* base = keys_.data() + start_[i];
            size_t length = start_[i+1] - start_[i];
            tmp_keys_[i] = Slice(base, length);
        }
        
        // Generate filter for current set of keys and append to result_.
        filter_offsets_.push_back(result_.size());
        policy_->CreateFilter(&tmp_keys_[0], num_keys, &result_);
        
        tmp_keys_.clear();
        keys_.clear();
        start_.clear();
    }
    
    /*
     FilterBlockReader的实现
     */
    
    FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents): policy_(policy), data_(NULL), offset_(NULL), num_(0), base_lg_(0)
    {
        size_t n = contents.size();
        if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
        base_lg_ = contents[n-1];
        uint32_t last_word = DecodeFixed32(contents.data() + n - 5);// 所有filter的总长度
        if (last_word > n - 5) return;
        data_ = contents.data();
        offset_ = data_ + last_word;
        num_ = (n - 5 - last_word) / 4;
    }
    
    bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key)
    {
        uint64_t index = block_offset >> base_lg_; // 相当于block_offset/2的base_lg_次方；也就相当于block_offset/(1<<base_lg_)
        if (index < num_)
        {
            uint32_t start = DecodeFixed32(offset_ + index*4);    // 得到filer的偏移量
            uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);// 得到下一个filer的偏移量
            if (start <= limit && limit <= (offset_ - data_)) // 偏移量需要小于等于所有filter的总长度
            {
                Slice filter = Slice(data_ + start, limit - start);
                return policy_->KeyMayMatch(key, filter);
            } else if (start == limit)
            {
                // Empty filters do not match any keys
                return false;
            }
        }
        return true;  // Errors are treated as potential matches
    }
    
}
