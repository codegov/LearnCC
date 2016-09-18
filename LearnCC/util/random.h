// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RANDOM_H_
#define STORAGE_LEVELDB_UTIL_RANDOM_H_

#include <stdint.h>

namespace leveldb {
    
    // A very simple random number generator.  Not especially good at
    // generating truly random bits, but good enough for our needs in this
    // package.
    
    
    /**
     C语言中伪随机数生成算法实际上是采用了"线性同余法"。具体的计算如下：
     
     Xi = (Xi-1 * A + C ) mod M
     
     其中A,C,M都是常数（一般会取质数）。当C=0时，叫做乘同余法。引出一个概念叫seed，它会被作为X0被代入上式中，然后每次调用rand()函数都会用上一次产生的随机值来生成新的随机值。可以看出实际上用rand()函数生成的是一个递推的序列，一切值都来源于最初的 seed。所以当初始的seed取一样的时候，得到的序列都相同。
     */
    /**
     一种常用的产生伪随机数的方法:
     y = (y * a + c) % m
     首先设定y一个初始值x,我们称其为种子(seed)
     一个常用的原则就是m尽可能的大。例如，对于32位的机器来说，选择m=2^31-1, a=7^5=16807时可以取得最佳效果。
     */
    
    class Random
    {
    private:
        uint32_t seed_;
    public:
        explicit Random(uint32_t s) : seed_(s & 0x7fffffffu)  // 0x7fffffffu == 2147483647L == 2^31-1 == 01111111 11111111 11111111 11111111
        {
            // Avoid bad seeds.
            if (seed_ == 0 || seed_ == 2147483647L)
            {
                seed_ = 1;
            }
        }
        // 16807随机数
        uint32_t Next()
        { //0111 1111 1111 1111 1111 1111 1111 1111
            static const uint32_t M = 2147483647L;   // 2^31-1
            //0100 0001 1010 0111
            static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
            // We are computing
            //       seed_ = (seed_ * A) % M,    where M = 2^31-1
            //
            // seed_ must not be zero or M, or else all subsequent computed values
            // will be zero or M respectively.  For all other values, seed_ will end
            // up cycling through every number in [1,M-1]
            uint64_t product = seed_ * A;
            
            // Compute (product % M) using the fact that ((x << 31) % M) == x.
            seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
            // The first reduction may overflow by 1 bit, so we may need to
            // repeat.  mod == M is not possible; using > allows the faster
            // sign-bit-based test.
            if (seed_ > M)
            {
                seed_ -= M;
            }
            /**
             通过公式可以知道，需要判断(product % M)是否与static_cast<uint32_t>((product >> 31) + (product & M))相等？
             分析以下情况：
             1、当product < 2^31-1时，
             可以计算(product >> 31)得到的结果为0，(product & M)得到的结果为product；
             则static_cast<uint32_t>((product >> 31) + (product & M))，判断条件if (seed_ > M)，不需要减去M，最终结果为product;
             而通过计算(product % M)的结果也为product。
             结果相等。
             
             2、当product == 2^31-1时，
             可以计算(product & M)得到的结果为M，(product >> 31)得到的结果为0；
             则static_cast<uint32_t>((product >> 31) + (product & M))得到的结果为M;判断条件if (seed_ > M)，需要减去M，最终结果为0;
             而通过计算(product % M)得到的结果也为0；
             结果都是0。
             
             3、当product >= 2^31 && product <= 2^32-1时，// 即是2^32-1==2^31+2^31-1==2^31+M;
             // 判断条件的二进制是从1000 0000 0000 0000 0000 0000 0000 0000 到 1111 1111 1111 1111 1111 1111 1111 1111
             可以计算(product & M)得到的结果为0、1、2、3...、M，(product >> 31)得到的结果为1，
             则static_cast<uint32_t>((product >> 31) + (product & M))得到的结果依次为从1到1+M；判断条件if (seed_ > M)，需要减去M，最终结果依次为从1到M;
             而通过计算(product % M)，可将product分解成M加上某个值t(t值依次为从1到2^31==M+1);即product == M + t;由此可得到(t + M) % M，即等同于t%M + M%M，得到的结果为t。
             结果都是依次为从1到M。
             
             
             4、当product > 2^31+M时，同理可以按上面逻辑分析；
             */
            
            return seed_;
        }
        // Returns a uniformly distributed value in the range [0..n-1]
        // REQUIRES: n > 0
        uint32_t Uniform(int n) { return Next() % n; }
        
        // Randomly returns true ~"1/n" of the time, and false otherwise.
        // REQUIRES: n > 0
        bool OneIn(int n) { return (Next() % n) == 0; }
        
        // Skewed: pick "base" uniformly from range [0,max_log] and then
        // return "base" random bits.  The effect is to pick a number in the
        // range [0,2^max_log-1] with exponential bias towards smaller numbers.
        uint32_t Skewed(int max_log)
        {
            return Uniform(1 << Uniform(max_log + 1));
        }
    };
    
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_RANDOM_H_
