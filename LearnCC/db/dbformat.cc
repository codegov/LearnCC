// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb
{
    static uint64_t PackSequenceAndType(uint64_t seq, ValueType t)
    {
        assert(seq <= kMaxSequenceNumber);
        assert(t <= kValueTypeForSeek);
        return (seq << 8) | t;
    }
    
    void AppendInternalKey(std::string* result, const ParsedInternalKey& key)
    {
        result->append(key.user_key.data(), key.user_key.size());
        PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
    }
    
    /***********************************************************************************
     类：ParsedInternalKey 解析内部key
     **********************************************************************************/
    
    std::string ParsedInternalKey::DebugString() const
    {
        char buf[50];
        snprintf(buf, sizeof(buf), "' @ %llu : %d", (unsigned long long) sequence, int(type));
        std::string result = "'";
        result += EscapeString(user_key.ToString());
        result += buf;
        return result;
    }
    
    /***********************************************************************************
     类：InternalKey 内部key
     **********************************************************************************/
    
    std::string InternalKey::DebugString() const
    {
        std::string result;
        ParsedInternalKey parsed;
        if (ParseInternalKey(rep_, &parsed))
        {
            result = parsed.DebugString();
        } else
        {
            result = "(bad)";
            result.append(EscapeString(rep_));
        }
        return result;
    }
    
    /***********************************************************************************
     类：InternalKeyComparator 内部key比较器
     **********************************************************************************/
    
    const char* InternalKeyComparator::Name() const
    {
        return "leveldb.InternalKeyComparator";
    }
    
    int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const
    {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
        if (r == 0)
        {
            const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
            const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
            if (anum > bnum)
            {
                r = -1;
            } else if (anum < bnum)
            {
                r = +1;
            }
        }
        return r;
    }
    
    void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const
    {
        // Attempt to shorten the user portion of the key
        Slice user_start = ExtractUserKey(*start);
        Slice user_limit = ExtractUserKey(limit);
        std::string tmp(user_start.data(), user_start.size());
        user_comparator_->FindShortestSeparator(&tmp, user_limit);
        if (tmp.size() < user_start.size() && user_comparator_->Compare(user_start, tmp) < 0)
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
            assert(this->Compare(*start, tmp) < 0);
            assert(this->Compare(tmp, limit) < 0);
            start->swap(tmp);
        }
    }
    
    void InternalKeyComparator::FindShortSuccessor(std::string* key) const
    {
        Slice user_key = ExtractUserKey(*key);
        std::string tmp(user_key.data(), user_key.size());
        user_comparator_->FindShortSuccessor(&tmp);
        if (tmp.size() < user_key.size() && user_comparator_->Compare(user_key, tmp) < 0)
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
            assert(this->Compare(*key, tmp) < 0);
            key->swap(tmp);
        }
    }
    
    /***********************************************************************************
     类：InternalFilterPolicy 内部过滤器
     **********************************************************************************/
    
    const char* InternalFilterPolicy::Name() const
    {
        return user_policy_->Name();
    }
    
    void InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const
    {
        // We rely on the fact that the code in table.cc does not mind us
        // adjusting keys[].
        Slice* mkey = const_cast<Slice*>(keys);
        for (int i = 0; i < n; i++)
        {
            mkey[i] = ExtractUserKey(keys[i]);
            // TODO(sanjay): Suppress dups?
        }
        user_policy_->CreateFilter(keys, n, dst);
    }
    
    bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const
    {
        return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
    }
    
    /***********************************************************************************
     类：LookupKey
     **********************************************************************************/
    
    LookupKey::LookupKey(const Slice& user_key, SequenceNumber s)
    {
        size_t usize = user_key.size();
        /**
         为什么加13？假设指针dst的初始位置为x；
         存储klength时，klength的类型为size_t，需要占4个字节的空间，而因为klength通过EncodeVarint32的编码，得到了实际占用的空间，肯定小于等于4个字节，不过这里我们需要按最大值来计算，所以存储klength最大需要占4个字节的空间，则使指针dst向右移动4，因为指针是char类型，所以移动4就等同于移动了4个字节。此时指针dst的初始位置为x+4；
         存储user_key时，需要占usize个的字节空间，则使指针dst向右移动usize。此时指针dst的初始位置为x+4+usize；
         存储sequence+type时，sequence的类型为uint64_t，需要占8个字节，由sequence的最大值kMaxSequenceNumber可以看出，其中sequence占7个字节，type占1个字节。则使指针dst向右移动8。此时指针dst的初始位置为x+4+usize+8；
         指针dst的位置从x到x+usize+12，所以估测需要占用usize+13的空间
         */
        size_t needed = usize + 13;  // A conservative estimate
        char* dst;
        if (needed <= sizeof(space_))
        {
            dst = space_;
        } else
        {
            dst = new char[needed];
        }
        start_ = dst;
        dst = EncodeVarint32(dst, usize + 8); // klength=usize + 8；根据klength长度将klength存储到dst中
        kstart_ = dst;
        memcpy(dst, user_key.data(), usize);  // 将user_key存储到dst中
        dst += usize; // 移动地址位置
        EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek)); // 将sequence+type存储到dst中
        dst += 8;     // 移动地址位置
        end_ = dst;
    }
    
}  // namespace leveldb
