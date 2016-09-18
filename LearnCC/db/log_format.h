// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb
{
    namespace log
    {
     
        /*************************************************************************
         日志文件的结构：
         ——————————————————————————————————————————
         block1           // Block的结构
         ——————————————————————————————————————————
         block2
         ——————————————————————————————————————————
         block3
         ——————————————————————————————————————————
         ......
         ——————————————————————————————————————————
         
         
         Block的结构：              // 大小：32K
         ——————————————————————————————————————————
         record1   // crc | size | type | data  // 注：crc是校验码，占4个字节；size是大小，占2个字节；type是类型，占1个字节；data是记录全部数据或者部分数据
         ——————————————————————————————————————————
         record1   // crc | size | type | data
         ——————————————————————————————————————————
         record2   // crc | size | type | data
         ——————————————————————————————————————————
         record2
         ——————————————————————————————————————————
         record2
         ——————————————————————————————————————————
         record2
         ——————————————————————————————————————————
         record2
         ——————————————————————————————————————————
         record3
         ——————————————————————————————————————————
         ......
         ——————————————————————————————————————————
         
         
         *************************************************************************/
        enum RecordType
        {
            // Zero is reserved for preallocated files
            kZeroType = 0,
            
            kFullType = 1,
            
            // For fragments
            kFirstType = 2,
            kMiddleType = 3,
            kLastType = 4
        };
        static const int kMaxRecordType = kLastType;
        
        static const int kBlockSize = 32768; // 32 * 1024 即32K
        
        // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
        static const int kHeaderSize = 4 + 2 + 1;
        
    }  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
