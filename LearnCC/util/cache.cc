// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb
{
    
    Cache::~Cache() { }
    
    namespace {
        
        // LRU cache implementation
        
        // An entry is a variable length heap-allocated structure.  Entries
        // are kept in a circular doubly linked list ordered by access time.
        struct LRUHandle
        {
            void* value;
            void (*deleter)(const Slice&, void* value);
            LRUHandle* next_hash;  // 二级指针数组的二级指针链表
            LRUHandle* next;       // 双向链表 指向比较旧的数据
            LRUHandle* prev;       // 双向链表 指向比较新的数据
            size_t charge;      // TODO(opt): Only allow uint32_t?
            size_t key_length;
            uint32_t refs;
            uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
            char key_data[1];   // Beginning of key key的首地址
            
            Slice key() const
            {
                // For cheaper lookups, we allow a temporary Handle object
                // to store a pointer to a key in "value".
                if (next == this)
                {
                    return *(reinterpret_cast<Slice*>(value));
                } else
                {
                    return Slice(key_data, key_length);
                }
            }
        };
        
        
        /*****************************************************************************
         类：HandleTable
         *****************************************************************************/
        
        // We provide our own simple hash table since it removes a whole bunch
        // of porting hacks and is also faster than some of the built-in hash
        // table implementations in some of the compiler/runtime combinations
        // we have tested.  E.g., readrandom speeds up by ~5% over the g++
        // 4.4.3's builtin hashtable.
        class HandleTable
        {
        public:
            HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
            ~HandleTable() { delete[] list_; }
            
            LRUHandle* Lookup(const Slice& key, uint32_t hash)
            {
                return *FindPointer(key, hash);
            }
            
            LRUHandle* Insert(LRUHandle* h)
            {
                LRUHandle** ptr = FindPointer(h->key(), h->hash);
                LRUHandle* old = *ptr;
                h->next_hash = (old == NULL ? NULL : old->next_hash);
                *ptr = h;
                // 找到的节点的值为NULL，说明h是新节点
                if (old == NULL)
                {
                    // 元素个数
                    ++elems_;
                    // 元素个数加1大于一级指针个数。如果每个节点h定位一级指针不存在哈希冲突，则每个一级指针存放一个节点
                    if (elems_ > length_)
                    {
                        // Since each cache entry is fairly large, we aim for a small
                        // average linked list length (<= 1).
                        Resize();
                    }
                }
                return old;
            }
            
            LRUHandle* Remove(const Slice& key, uint32_t hash)
            {
                LRUHandle** ptr = FindPointer(key, hash);
                LRUHandle* result = *ptr;
                if (result != NULL)
                {
                    *ptr = result->next_hash;
                    --elems_;
                }
                return result;
            }
            
        private:
            // The table consists of an array of buckets where each bucket is
            // a linked list of cache entries that hash into the bucket.
            uint32_t length_;
            uint32_t elems_;
            LRUHandle** list_;
            
            // Return a pointer to slot that points to a cache entry that
            // matches key/hash.  If there is no such cache entry, return a
            // pointer to the trailing slot in the corresponding linked list.
            LRUHandle** FindPointer(const Slice& key, uint32_t hash)
            {
                /* 
                 hash & (length_ - 1)的运算结果是0到length-1;
                 */
                LRUHandle** ptr = &list_[hash & (length_ - 1)];
                // 二级指针链表*ptr不为空，遍历二级指针链表找到hash相同且key也相同的节点
                while (*ptr != NULL && ((*ptr)->hash != hash || key != (*ptr)->key()))
                {
                    ptr = &(*ptr)->next_hash;
                }
                // 返回匹配节点的地址
                return ptr;
            }
            
            void Resize()
            {
                uint32_t new_length = 4;
                while (new_length < elems_)
                {
                    new_length *= 2;
                }
                // 下面的new方法，只表明给一级指针分配了内存块
                LRUHandle** new_list = new LRUHandle*[new_length];
                /*
                 避免一级指针分配的内存块，存有野指针，所以需要使用memset对内存块进行清零处理。
                 memset:作用是在一段内存块中存储某个给定的值，
                        它对较大的结构体或数组进行清零操作的一种最快方法。
                        存储0，就是置空。
                 new_list和&new_list[i]是一级指针，
                 *new_list和new_list[i]是二级指针，
                 **new_list是二级指针存储的值。
                 下面memset代码的意思是：
                 即将一级指针内存块中存储0，就是new_list[i] = 0或new_list[i] = NULL;
                 也就是将二级指针*new_list置空。
                 */
                memset(new_list, 0, sizeof(new_list[0]) * new_length);
                uint32_t count = 0;
                for (uint32_t i = 0; i < length_; i++) // 遍历一级指针
                {
                    /*
                     由于每个h通过表达式hash&(new_length - 1)得到属于一级指针的位置，
                     所以表达式计算结果相同（注：hash不相同，计算结果也可能相同）的h,会定位到相同的一级指针，
                     并组成一个二级链表存放在一级指针上。
                     一级指针上存放的二级指针链表，通过h的next_hash链接起来
                     */
                    LRUHandle* h = list_[i];
                    while (h != NULL)
                    {
                        /*
                         功能：下面遍历的逻辑是重新定位h属于的一级指针。并在新的一级指针上组成新的二级链表。
                         */
                        LRUHandle* next = h->next_hash;
                        uint32_t hash = h->hash;
                        // 定位新的一级指针 *ptr就是new_list[hash & (new_length - 1)]
                        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                        // 如果是第一次运行，则*ptr为NULL，其他则是取到上个循环h的地址
                        h->next_hash = *ptr;
                        // new_list[hash & (new_length - 1)] = h;
                        *ptr = h;
                        // 二级链表下一个节点
                        h = next;
                        count++;
                    }
                }
                assert(elems_ == count);
                delete[] list_;
                list_ = new_list;
                length_ = new_length;
            }
        };
        
        /*****************************************************************************
         类：LRUCache
         *****************************************************************************/
        
        // A single shard of sharded cache.
        class LRUCache
        {
        public:
            LRUCache();
            ~LRUCache();
            
            // Separate from constructor so caller can easily make an array of LRUCache
            void SetCapacity(size_t capacity) { capacity_ = capacity; }
            
            // Like Cache methods, but with an extra "hash" parameter.
            Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                                  size_t charge, void (*deleter)(const Slice& key, void* value));
            Cache::Handle* Lookup(const Slice& key, uint32_t hash);
            void Release(Cache::Handle* handle);
            void Erase(const Slice& key, uint32_t hash);
            
        private:
            void LRU_Remove(LRUHandle* e);
            void LRU_Append(LRUHandle* e);
            void Unref(LRUHandle* e);
            
            // Initialized before use.
            // 缓存的总容量
            size_t capacity_;
            
            // mutex_ protects the following state.
            port::Mutex mutex_;
            // 缓存数据的总大小
            size_t usage_;
            
            // Dummy head of LRU list.
            // lru.prev is newest entry, lru.next is oldest entry.
            // 双向循环链表，有大小限制，保证数据的新旧，当缓存不够时，保证先清除旧的数据
            LRUHandle lru_;
            /* 
             二级指针数组，链表没有大小限制，动态扩展大小，保证数据快速查找，
             hash定位一级指针，得到存放在一级指针上的二级指针链表，遍历查找数据
             */
            HandleTable table_;
        };
        
        LRUCache::LRUCache(): usage_(0)
        {
            // Make empty circular linked list
            lru_.next = &lru_;
            lru_.prev = &lru_;
        }
        
        LRUCache::~LRUCache()
        {
            for (LRUHandle* e = lru_.next; e != &lru_; )
            {
                LRUHandle* next = e->next;
                assert(e->refs == 1);  // Error if caller has an unreleased handle
                Unref(e);
                e = next;
            }
        }
        
        void LRUCache::Unref(LRUHandle* e)
        {
            assert(e->refs > 0);
            e->refs--;
            if (e->refs <= 0) // 引用计数小于等于0 释放
            {
                usage_ -= e->charge;
                (*e->deleter)(e->key(), e->value);
                free(e);
            }
        }
        
        void LRUCache::LRU_Remove(LRUHandle* e)
        {
            e->next->prev = e->prev;
            e->prev->next = e->next;
        }
        
        void LRUCache::LRU_Append(LRUHandle* e)
        {
            // Make "e" newest entry by inserting just before lru_
            // 新数据插到lru_的前面
            e->next = &lru_;
            e->prev = lru_.prev;
            e->prev->next = e;
            e->next->prev = e;
        }
        
        Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash)
        {
            MutexLock l(&mutex_);
            LRUHandle* e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                e->refs++;
                /*
                 为什么要先删除，再加入。
                 由于当缓存不够时，会清除lru_的next处的数据，保证清除比较旧的数据。
                 */
                LRU_Remove(e);
                LRU_Append(e);
            }
            return reinterpret_cast<Cache::Handle*>(e);
        }
        
        void LRUCache::Release(Cache::Handle* handle)
        {
            MutexLock l(&mutex_);
            Unref(reinterpret_cast<LRUHandle*>(handle));
        }
        
        Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value, size_t charge, void (*deleter)(const Slice& key, void* value))
        {
            MutexLock l(&mutex_);
            
            // 减去记录key的首地址大小(一个字节)，加上key实际大小
            LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle)-1 + key.size()));
            e->value = value;
            e->deleter = deleter;
            e->charge = charge;
            e->key_length = key.size();
            e->hash = hash;
            e->refs = 2;  // One from LRUCache, one for the returned handle
            // 记录key的首地址
            memcpy(e->key_data, key.data(), key.size());
            LRU_Append(e);
            // 缓存数据的大小
            usage_ += charge;
            
            LRUHandle* old = table_.Insert(e);
            if (old != NULL)
            {
                LRU_Remove(old);
                Unref(old);
            }
            
            // 缓存不够，清除比较旧的数据
            while (usage_ > capacity_ && lru_.next != &lru_)
            {
                LRUHandle* old = lru_.next;
                LRU_Remove(old);
                table_.Remove(old->key(), old->hash);
                Unref(old);
            }
            
            return reinterpret_cast<Cache::Handle*>(e);
        }
        
        void LRUCache::Erase(const Slice& key, uint32_t hash)
        {
            MutexLock l(&mutex_);
            LRUHandle* e = table_.Remove(key, hash);
            if (e != NULL)
            {
                LRU_Remove(e);
                Unref(e);
            }
        }
        
        /*****************************************************************************
         类：ShardedLRUCache
         *****************************************************************************/
        
        static const int kNumShardBits = 4;
        static const int kNumShards = 1 << kNumShardBits; // 2^4==16
        
        class ShardedLRUCache : public Cache
        {
        private:
            LRUCache shard_[kNumShards];
            port::Mutex id_mutex_;
            uint64_t last_id_;
            
            static inline uint32_t HashSlice(const Slice& s)
            {
                return Hash(s.data(), s.size(), 0);
            }
            
            // 得到shard_数组的下标
            static uint32_t Shard(uint32_t hash)
            {
                /*
                 hash是4个字节，32位，向右移动28位，则剩下高4位有效位，
                 即最小的是0000等于0，最大的是1111等于15 
                 则得到的数字在[0,15]范围内。
                 */
                return hash >> (32 - kNumShardBits);
            }
            
        public:
            explicit ShardedLRUCache(size_t capacity) : last_id_(0)
            {
                /*
                 将容量平均分成kNumShards份，如果有剩余，将剩余的补全。为什么要补全呢？
                 例如设置容量大小为10，则最多就能放下大小为10的数据，现在将容量分成3份，
                 如果不补全，余量被丢弃，每份容量则为3，总容量为9，需要放大小为10的数据则放不下了。
                 如果补全，剩余量1加上2，每份就多得1个容量，也就每份容量为4，总容量为12，能保证数据都放下。
                 */
                /*
                 //补全块，
                 如果capacity除以kNumShards有余数，那么余数加上(kNumShards - 1)，
                 除以kNumShards，就能多得到一块。
                 如果如果capacity除以kNumShards无余数，那么0加上(kNumShards - 1)，
                 除以kNumShards，还是0
                 */
                const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
                for (int s = 0; s < kNumShards; s++)
                {
                    shard_[s].SetCapacity(per_shard);
                }
            }
            virtual ~ShardedLRUCache() { }
            // charge 数据大小
            virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                                   void (*deleter)(const Slice& key, void* value))
            {
                const uint32_t hash = HashSlice(key);
                return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
            }
            virtual Handle* Lookup(const Slice& key)
            {
                const uint32_t hash = HashSlice(key);
                return shard_[Shard(hash)].Lookup(key, hash);
            }
            virtual void Release(Handle* handle)
            {
                LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
                shard_[Shard(h->hash)].Release(handle);
            }
            virtual void Erase(const Slice& key)
            {
                const uint32_t hash = HashSlice(key);
                shard_[Shard(hash)].Erase(key, hash);
            }
            virtual void* Value(Handle* handle)
            {
                return reinterpret_cast<LRUHandle*>(handle)->value;
            }
            virtual uint64_t NewId()
            {
                MutexLock l(&id_mutex_);
                return ++(last_id_);
            }
        };
        
    }  // end anonymous namespace
    
    /*****************************************************************************
     类：Cache
     *****************************************************************************/
    
    Cache* NewLRUCache(size_t capacity)
    {
        return new ShardedLRUCache(capacity);
    }
    
}  // namespace leveldb
