// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "leveldb/dumpfile.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

#include "leveldb/db.h"
#include <iostream>
#include <leveldb/write_batch.h>
#include <leveldb/iterator.h>

#include "util/random.h"
#include <utility>
#include <set>
#include <math.h>

using namespace std;

namespace leveldb {
    namespace {
        
        class StdoutPrinter : public WritableFile {
        public:
            virtual Status Append(const Slice& data) {
                fwrite(data.data(), 1, data.size(), stdout);
                return Status::OK();
            }
            virtual Status Close() { return Status::OK(); }
            virtual Status Flush() { return Status::OK(); }
            virtual Status Sync() { return Status::OK(); }
        };
        
        bool HandleDumpCommand(Env* env, char** files, int num) {
            StdoutPrinter printer;
            bool ok = true;
            for (int i = 0; i < num; i++) {
                Status s = DumpFile(env, files[i], &printer);
                if (!s.ok()) {
                    fprintf(stderr, "%s\n", s.ToString().c_str());
                    ok = false;
                }
            }
            return ok;
        }
        
    }  // namespace
}  // namespace leveldb

static void Usage() {
    fprintf(
            stderr,
            "Usage: leveldbutil command...\n"
            "   dump files...         -- dump contents of specified files\n"
            );
}

void funy(int &x)
{
    std::cout<<"     funy1=地址："<<&x<<"；地址存的值："<<x<<std::endl;
    x = 3;
    std::cout<<"     funy2=地址："<<&x<<"；地址存的值："<<x<<std::endl;
    int *y = &x;
    *y = 8;
    std::cout<<"     funy3=地址："<<y<<"；地址存的值："<<*y<<std::endl;
}

void funp (int *x)
{
    std::cout<<"     funp1=地址："<<x<<"；地址存的值："<<*x<<std::endl;
    *x = 3;
    std::cout<<"     funp2=地址："<<x<<"；地址存的值："<<*x<<std::endl;
    std::cout<<"     funp3=地址："<<&x<<"；地址存的值："<<x<<std::endl;
}

void funv (int x)
{
    std::cout<<"     funv1=地址："<<&x<<"；地址存的值："<<x<<std::endl;
    x = 8;
    std::cout<<"     funv2=地址："<<&x<<"；地址存的值："<<x<<std::endl;
}

void funv2(const std::vector<int>& inputs1, const std::vector<int>& inputs2)
{
    std::vector<int> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end()); // 将inputs2中的元素从all的最后位置添加到all中
    for (int i = 0; i < all.size(); i++)
    {
        std::cout<<"std::vector<int> all:"<<all[i]<<std::endl;
    }
    
    for (int i = 0; i < inputs1.size(); i++)
    {
        std::cout<<"std::vector<int> inputs1:"<<inputs1[i]<<std::endl;
    }
}

void funv3(char *msg, ...)
{
    va_list ap;
    int num = 0;
    char *para;
    /*ap指向传入的第一个可选参数，msg是最后一个确定的参数*/
    va_start(ap, msg);
    while (1)
    {
        para = va_arg( ap, char *); // 注：变量para的类型要与va_arg里的第二参数类型要一致
        if ( strcmp( para, "") == 0 )
            break;
        printf("Parameter #%d is: %s\n", num, para);
        num++;
    }
    va_end( ap );
}

void funv4(int &seed)
{
    seed = (seed * 16807 ) % 2157483547;
}

bool funv5(int64_t x, int y)
{
    int64_t r1 = (x<<y)%((int64_t)pow(2, y)-1);
    bool v = r1 == x;
    if (v) {
        std::cout<<"相等"<<r1<<std::endl;
    } else
    {
        std::cout<<"不相等"<<r1<<std::endl;
    }
    return v;
}

uint32_t seed_;
// 16807随机数
uint32_t Next(uint32_t base)
{ //0111 1111 1111 1111 1111 1111 1111 1111
    static const uint32_t M = 2147483647L;   // 2^31-1
    //0100 0001 1010 0111
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    /**
     一种常用的产生伪随机数的方法:
     y = (y * a) % m
     首先设定y一个初始值x,我们称其为种子(seed)
     一个常用的原则就是m尽可能的大。例如，对于32位的机器来说，选择m=2^31-1, a=7^5=16807时可以取得最佳效果。
     */
    
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
    return seed_ % base;
}

class TestStatic
{
public:
    static int a;
    static void test()
    {
        static int b;
    }
};

int main(int argc, char** argv)
{
    //  leveldb::Env* env = leveldb::Env::Default();
    //  bool ok = true;
    //  if (argc < 2) {
    //    Usage();
    //    ok = false;
    //  } else {
    //    std::string command = argv[1];
    //    if (command == "dump") {
    //      ok = leveldb::HandleDumpCommand(env, argv+2, argc-2);
    //    } else {
    //      Usage();
    //      ok = false;
    //    }
    //  }
    //  return (ok ? 0 : 1);

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/Users/syq/Desktop/testdb", &db);
    assert(status.ok());
    
    //write key1,value1
    std::string key   = "key1HH";
    std::string value = "valueHH";
    
    status = db->Put(leveldb::WriteOptions(), key, value);
    assert(status.ok());
    
    status = db->Get(leveldb::ReadOptions(), key, &value);
    assert(status.ok());
    std::cout<<key<<"==="<<value<<std::endl;
    
    //move the value under key to key2
    std::string key2 = "key2HH";
    status = db->Put(leveldb::WriteOptions(), key2, value);
    assert(status.ok());
    
    status = db->Delete(leveldb::WriteOptions(), key);
    assert(status.ok());
    
    status = db->Get(leveldb::ReadOptions(), key2, &value);
    assert(status.ok());
    std::cout<<key2<<"==="<<value<<std::endl;
    if (status.ok())
    {
        leveldb::WriteBatch batch;
        batch.Delete(key2);
        batch.Put(key, value);
        status = db->Write(leveldb::WriteOptions(), &batch);
        assert(status.ok());
    }
    
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok())
    {
        std::cerr<<key<<"==="<<status.ToString()<<std::endl;
    } else
    {
        std::cout<<key<<"="<<value<<std::endl;
    }
    
    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::cout<<it->key().ToString()<<":"<<it->value().ToString()<<std::endl;
    }
    assert(it->status().ok());
    delete it;
    
    char c = 98;
    unsigned char a = c;
    uint32_t b = c;
    std::cout<<b<<"-----"<<sizeof(c)<<a<<std::endl;
    
    char buf[sizeof(b)];
    int *buf1;
    long l;
    
    memcpy(buf, &b, sizeof(b));
    std::cout<<"buf=="<<sizeof(buf)<<"=="<<sizeof(buf1)<<"--"<<sizeof(l)<<std::endl;
    std::string dst = "ww";
    //    std::string *d = &dst;
    dst.append(buf, sizeof(buf));
    std::cout<<"dst=="<<dst<<sizeof(uint32_t)<<sizeof(uint64_t)<<std::endl;
    
    size_t kf = 1 << 11; // 2的11方=2048
    uint64_t filter_index = (2049 / kf);
    std::cout<<"kf=="<<filter_index<<std::endl;
    
    // 地址传递包括引用传递和指针传递
    // 引用传递
    int x1 = 1;
    std::cout<<"\nfuny=地址："<<&x1<<"；地址存的值："<<x1<<std::endl;
    funy(x1);
    std::cout<<"funy=地址："<<&x1<<"；地址存的值："<<x1<<std::endl;
    // 指针传递
    int x2 = 2;
    std::cout<<"\nfunp=地址："<<&x2<<"；地址存的值："<<x2<<std::endl;
    funp(&x2);
    std::cout<<"funp=地址："<<&x2<<"；地址存的值："<<x2<<std::endl;
    // 值传递
    int x3 = 3;
    std::cout<<"\nfunv=地址："<<&x3<<"；地址存的值："<<x3<<std::endl;
    funv(x3);
    std::cout<<"funv=地址："<<&x3<<"；地址存的值："<<x3<<std::endl;
    
    
    
    char* string = "";
    std::cout<<"1-----"<<string<<"====="<<&string<<"----"<<*string<<std::endl;
    string = "horg";
    std::cout<<"2-----"<<string<<"====="<<&string<<"----"<<*string<<std::endl;
    printf("=1======%p\n", string);
    char* name = string;
    printf("=2======%p\n", name);
    std::cout<<"3-----"<<name<<"====="<<&name<<"----"<<*name<<std::endl;
    name = "org2";
    printf("=3======%p\n", name);
    std::cout<<"4-----"<<name<<"====="<<&name<<"----"<<*name<<std::endl;
    
    
    
    std::string aa = "a";
    std::string *p;
    p = &aa;
    std::string **pr = &p; // 相当于std::string **pr; pr = &p;
    std::string *f = *pr;
    std::cout<<p<<"="<<*p<<"="<<**pr<<"="<<*pr<<"="<<pr<<"="<<*f<<"="<<f<<std::endl;
    
    char** new_list = new char*[3];
    memset(new_list, 0, sizeof(new_list[0]) * 3);
    if (*new_list == NULL)
    {
        std::cout<<"*new_list is null"<<std::endl;
    }
    if (new_list == NULL)
    {
        std::cout<<"new_list is null"<<std::endl;
    }
    
    char  aaaa= 65;
    new_list[1] = &aaaa;
    std::cout<<"&aaaa=="<<&aaaa<<std::endl;
    
    char **list1 = &new_list[1];
    char *tempList = *list1;
    
    std::cout<<"list=="<<*tempList<<"=="<<tempList<<"--"<<**list1<<"=="<<*list1<<"--"<<list1<<std::endl;
    
    std::string diff;
    std::string *diff1 = &diff;
    (*diff1)[0]++;
    //    diff[0]++;
    //    diff1->resize(1);
    if (diff.empty())
    {
        std::cout<<"diff==empty"<<std::endl;
    }
    uint8_t diff_byte = static_cast<uint8_t>((*diff1)[0]);
    
    std::cout<<"diff=="<<diff<<"=="<<*diff1<<"=="<<diff1<<"=="<<diff_byte<<"=="<<std::endl;
    
    std::string str;
    std::string str1 = "1234 1234 1234 1234 1234 1234 1234 ";
    std::cout<<"str=="<<str.size()<<"  "<<str.max_size()<<"  "<<str.capacity()<<"  "<<str.length()<<"\nstr1=="<<str1.size()<<"  "<<str1.max_size()<<"  "<<str1.capacity()<<"  "<<str1.length()<<"  "<<std::endl;
    
    std::string saved_value_ = "saved_value_";
    std::string empty;
    swap(empty, saved_value_);
    
    uint32_t i32 =128;
    std::cout<<"swap=="<<empty<<"=="<<saved_value_<<"uint32_t="<<sizeof(uint32_t)<<";uint64_t="<<sizeof(uint64_t)<<";i32="<<i32<<";(1<<7)="<<(1<<7)<<std::endl;
    
    static const uint32_t M = 2147483647L;   // 2^31-1
    uint64_t x = 2147483647L;
    //std::cout<<((x << 31) % M);
    if (((x << 31) % M) == x)
    {
        std::cout<<"相等"<<std::endl;
    } else
    {
        std::cout<<"不相等"<<std::endl;
    }
    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    
    seed_ = 10;
    for (int i = 0; i < 20; i++)
    {
//        uint32_t rom = Next(10);
//        std::cout<<"random=="<<rom<<std::endl;
    }
    
    unsigned char cb = 0x80;
    char ca = cb;
    int ta = ca;
    int tb = cb;
    std::cout<<"ca=="<<ca<<"cb=="<<cb<<"ta=="<<ta<<"tb=="<<tb<<std::endl;
    
    class MyClass
    {
        public:
        int myValue;
        virtual ~MyClass()
        {
            std::cout<<"Destroying MyClass myValue="<<myValue<<std::endl;
        }
    };
    
    struct MyCompare
    {
        bool operator()(MyClass* f1, MyClass* f2) const
        {
            return (f1->myValue > f2->myValue); // 降序
        }
    };
    
    typedef std::set<MyClass*, MyCompare> MySet; //set自定义排序,FileMetaData*是元素，BySmallestKey是排序规则
    
    MySet *myset = new MySet();
    MyClass *class1 = new MyClass();
    class1->myValue = 3;
    myset->insert(class1);
    
    MyClass *class2 = new MyClass();
    class2->myValue = 2;
    myset->insert(class2);
    
    MyClass *class3 = new MyClass();
    class3->myValue = 33;
    myset->insert(class3);
    
    for (MySet::const_iterator it = myset->begin(); it != myset->end(); it++)
    {
        MyClass *classm = *it;
        std::cout<<"myset==="<<classm->myValue<<std::endl;
    }
    
    delete class1;
    delete class2;
    delete class3;
    myset->clear();
    delete myset;
    
    std::vector<int> all = {1,2,3};
    std::vector<int> inputs2 = {10,20,30};
    all.insert(all.begin(), inputs2.begin(), inputs2.end());
    for (int i = 0; i < all.size(); i++)
    {
        std::cout<<"set=="<<all[i]<<std::endl;
    }
    
//    for (int i = 1; i <=30; i++)
//    {
//        int m = 1;
//        int n = i;
////        std::cout<<"m<<n==m*(2^n):"<<((m<<n)==m*pow(2,n))<<"(m<<n):"<<(m<<n)<<";m*(2^n):"<<m*pow(2,n)<<std::endl;
//        m = i;
//        n = 3;
//        std::cout<<";m>>n==m/(2^n):"<<((m>>n)==m/(int)pow(2,n))<<";"<<(m>>n)<<";"<<m/(int)pow(2,n)<<std::endl;
//    }
    
    char key_data[1];
    std::string key_d = "11111";
    
    memcpy(key_data, key_d.data(), key_d.size());
//    printf("key_dada:%p\n", key_data);
    std::cout<<"key_data:"<<key_data<<"=="<<*key_data<<"--"<<key_data[0]<<std::endl;
    
    std::vector<int> input1 = {1,2,3};
    std::vector<int> input2 = {7,8,9};
    funv2(input1, input2);
    
    
    uint64_t num = 124312;
    std::string str12;
    char buf12[30];
    snprintf(buf12, sizeof(buf12), "%llu", (unsigned long long) num);
    str12.append(buf12);
    
    static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);//按位取反
    static const unsigned short kChar = ~static_cast<unsigned short>(1);
    std::cout<<"str12:"<<str12<<"kMaxUint64:"<<kMaxUint64<<"kChar:"<<kChar<<"unsigned short:"<<sizeof(unsigned short)<<std::endl;
    char sss1 = '9';
    int sss2 = 9;
    int sss3 = sss1 - '5';
    if (sss1 == sss2)
    {
        std::cout<<"YES"<<std::endl;
    } else
    {
        std::cout<<"NO"<<sss1<<sss2<<sss3<<std::endl;
    }
    
    funv3("1", "2", "3sa", "");
    
    int seed = 121312;
    funv4(seed);
    char msg[] = "Destroying Env::Default()\n";
    uint8_t ut = static_cast<uint8_t>(0xff);
    std::cout<<"seed:"<<seed<<"sizeof(void*)"<<sizeof(void*)<<"msg="<<sizeof(msg)<<"ut="<<ut<<std::endl;
    if (ut==255)
    {
        printf("ut==255");
    } else
    {
        printf("ut!=255");
    }
    printf("ut===%d", ut);

    funv5(2*(int64_t)pow(2, 31)-1, 31);
    funv5(33, 2);
    delete db;
    return 0;
}



