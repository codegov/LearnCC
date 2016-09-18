// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "port/port.h"
#include "leveldb/status.h"

namespace leveldb
{
    /*
     // OK status has a NULL state_.  Otherwise, state_ is a new[] array
     // of the following form:
     //    state_[0..3] == length of message  // message.length是key.length + ": ".length + value.length
     //    state_[4]    == code
     //    state_[5..]  == message  // message的结构是key: value
     */
    
    const char* Status::CopyState(const char* state)
    {
        uint32_t size;//四个字节
        memcpy(&size, state, sizeof(size));//先把前四个字节拷贝出来，得到message的长度
        char* result = new char[size + 5]; //建立一个长度为（5 + message.length）的字节  5是指消息长度占的4个字节加上code占的一个字节
        memcpy(result, state, size + 5);   //把state里的值拷贝到result里
        return result;
    }
    
    Status::Status(Code code, const Slice& msg, const Slice& msg2)
    {
        assert(code != kOk);
        const uint32_t len1 = msg.size();
        const uint32_t len2 = msg2.size();
        const uint32_t size = len1 + (len2 ? (2 + len2) : 0); // 2是指下面赋值消息内容时，填充的':'和' '
        char* result = new char[size + 5];
        memcpy(result, &size, sizeof(size));//拷贝前四个字节，赋值message.length
        result[4] = static_cast<char>(code);//赋值第5个字节
        memcpy(result + 5, msg.data(), len1);//赋值消息实体
        if (len2)
        {
            result[5 + len1] = ':';
            result[6 + len1] = ' ';
            memcpy(result + 7 + len1, msg2.data(), len2);
        }
        state_ = result;
    }
    
    std::string Status::ToString() const
    {
        if (state_ == NULL)
        {
            return "OK";
        } else
        {
            char tmp[30];
            const char* type;
            switch (code())
            {
                case kOk:
                    type = "OK";
                    break;
                case kNotFound:
                    type = "NotFound: ";
                    break;
                case kCorruption:
                    type = "Corruption: ";
                    break;
                case kNotSupported:
                    type = "Not implemented: ";
                    break;
                case kInvalidArgument:
                    type = "Invalid argument: ";
                    break;
                case kIOError:
                    type = "IO error: ";
                    break;
                default:
                    snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
                    type = tmp;
                    break;
            }
            std::string result(type);
            uint32_t length;
            memcpy(&length, state_, sizeof(length));
            result.append(state_ + 5, length);
            return result;
        }
    }
    
}  // namespace leveldb
