// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <set>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb
{
    
    namespace
    {
        
        static Status IOError(const std::string& context, int err_number)
        {
            // strerror通过标准错误的标号，获得错误的描述字符串
            return Status::IOError(context, strerror(err_number));
        }
        
        /*****************************************************************
         类：PosixSequentialFile
         *****************************************************************/
        class PosixSequentialFile: public SequentialFile
        {
        private:
            std::string filename_;
            FILE* file_;
            
        public:
            // 构造函数的作用是保证每个对象的数据成员都有何时的初始值。
            PosixSequentialFile(const std::string& fname, FILE* f): filename_(fname), file_(f) { }
            // 析构函数的作用是回收内存和资源，通常用于释放在构造函数或对象生命期内获取的资源。
            virtual ~PosixSequentialFile() { fclose(file_); }
            
            virtual Status Read(size_t n, Slice* result, char* scratch)
            {
                Status s;
                /* 
                 size_t fread ( void *buffer, size_t size, size_t count, FILE *stream) ;
                 buffer 用于接收数据的内存地址
                 size   要读的每个数据项的字节数，单位是字节
                 count  要读count个数据项，每个数据项size个字节.
                 stream 输入流
                 返回值：实际读取的元素个数。如果返回值与count不相同，则可能文件结尾或发生错误
                 */
                size_t r = fread_unlocked(scratch, 1, n, file_);
                *result = Slice(scratch, r);
                if (r < n)
                {
                    // 是否到达文件结尾
                    if (feof(file_))
                    {
                        // We leave status as ok if we hit the end of the file
                    } else
                    {
                        // A partial read with an error: return a non-ok status
                        s = IOError(filename_, errno);
                    }
                }
                return s;
            }
            
            virtual Status Skip(uint64_t n)
            {
                /*
                 int fseek( FILE *stream, long offset, int origin );
                 第一个参数stream为文件指针
                 第二个参数offset为偏移量，正数表示正向偏移，负数表示负向偏移
                 第三个参数origin设定从文件的哪里开始偏移,可能取值为：SEEK_CUR、 SEEK_END 或 SEEK_SET
                 SEEK_SET： 文件开头，将读写位置指向文件头后再增加offset个位移量。
                 SEEK_CUR： 当前位置，以目前的读写位置往后增加offset个位移量。
                 SEEK_END： 文件结尾，将读写位置指向文件尾后再增加offset个位移量。
                 返回值：成功，返回0，失败返回-1
                 */
                if (fseek(file_, n, SEEK_CUR))
                {
                    return IOError(filename_, errno);
                }
                return Status::OK();
            }
        };
        
        /*****************************************************************
         类：PosixRandomAccessFile
         *****************************************************************/
        // pread() based random-access
        class PosixRandomAccessFile: public RandomAccessFile
        {
        private:
            std::string filename_;
            int fd_;
            
        public:
            PosixRandomAccessFile(const std::string& fname, int fd): filename_(fname), fd_(fd) { }
            virtual ~PosixRandomAccessFile() { close(fd_); }
            
            virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
            {
                Status s;
                ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));//返回值：成功，返回成功读取数据的字节数；失败，返回-1；
                *result = Slice(scratch, (r < 0) ? 0 : r);
                if (r < 0)
                {
                    // An error: return a non-ok status
                    s = IOError(filename_, errno);
                }
                return s;
            }
        };
        
        /*****************************************************************
         类：MmapLimiter
         *****************************************************************/
        // Helper class to limit mmap file usage so that we do not end up
        // running out virtual memory or running into kernel performance
        // problems for very large databases.
        /*
         辅助类限制使用mmap文件以便我们不会最终耗尽虚拟内存或运行到非常大的数据库内核的性能问题。
         */
        class MmapLimiter
        {
        public:
            // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
            MmapLimiter()
            {
                SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
            }
            
            // If another mmap slot is available, acquire it and return true.
            // Else return false.
            bool Acquire()
            {
                if (GetAllowed() <= 0)
                {
                    return false;
                }
                MutexLock l(&mu_);
                intptr_t x = GetAllowed();
                if (x <= 0)
                {
                    return false;
                } else
                {
                    SetAllowed(x - 1);
                    return true;
                }
            }
            
            // Release a slot acquired by a previous call to Acquire() that returned true.
            void Release()
            {
                MutexLock l(&mu_);
                SetAllowed(GetAllowed() + 1);
            }
            
        private:
            port::Mutex mu_;
            port::AtomicPointer allowed_;
            
            intptr_t GetAllowed() const
            {
                return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
            }
            
            // REQUIRES: mu_ must be held
            void SetAllowed(intptr_t v)
            {
                allowed_.Release_Store(reinterpret_cast<void*>(v));
            }
            
            MmapLimiter(const MmapLimiter&);
            void operator=(const MmapLimiter&);
        };
        
        /*****************************************************************
         类：PosixMmapReadableFile
         *****************************************************************/
        // mmap() based random-access
        class PosixMmapReadableFile: public RandomAccessFile
        {
        private:
            std::string filename_;
            void* mmapped_region_;
            size_t length_;
            MmapLimiter* limiter_;
            
        public:
            // base[0,length-1] contains the mmapped contents of the file.
            PosixMmapReadableFile(const std::string& fname, void* base, size_t length, MmapLimiter* limiter)
            : filename_(fname), mmapped_region_(base), length_(length), limiter_(limiter)
            {
            }
            
            virtual ~PosixMmapReadableFile()
            {
                /**
                 定义函数 int munmap(void *start,size_t length);
                 函数说明 munmap()用来取消参数start所指的映射内存起始地址，参数length则是欲取消的内存大小。
                 当进程结束或利用exec相关函数来执行其他程序时，映射内存会自动解除，但关闭对应的文件描述符时不会解除映射。
                 返回值 如果解除映射成功则返回0，否则返回－1，错误原因存于errno中错误代码EINVAL
                 */
                munmap(mmapped_region_, length_);
                limiter_->Release();
            }
            
            virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
            {
                Status s;
                if (offset + n > length_)
                {
                    *result = Slice();
                    s = IOError(filename_, EINVAL);
                } else
                {
                    *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
                }
                return s;
            }
        };
        
        /*****************************************************************
         类：PosixWritableFile
         *****************************************************************/
        class PosixWritableFile : public WritableFile
        {
        private:
            std::string filename_;
            FILE* file_;
            
        public:
            PosixWritableFile(const std::string& fname, FILE* f): filename_(fname), file_(f) { }
            
            ~PosixWritableFile()
            {
                if (file_ != NULL)
                {
                    // Ignoring any potential errors
                    fclose(file_);
                }
            }
            
            virtual Status Append(const Slice& data)
            {
                /**
                 size_t fwrite(const void* buffer, size_t size, size_t count, FILE* stream);
                 注意：这个函数以二进制形式对文件进行操作，不局限于文本文件
                 返回值：返回实际写入的数据块数目
                 （1）buffer：是一个指针，对fwrite来说，是要获取数据的地址；
                 （2）size：要写入内容的单字节数；
                 （3）count:要进行写入size字节的数据项的个数；
                 （4）stream:目标文件指针；
                 （5）返回实际写入的数据项个数count。
                 说明：写入到文件的哪里？ 这个与文件的打开模式有关，如果是w+，则是从file pointer指向的地址开始写，替换掉之后的内容，文件的长度可以不变，stream的位置移动count个数；如果是a+，则从文件的末尾开始添加，文件长度加大。
                 fseek对此函数有作用，但是fwrite[1]  函数写到用户空间缓冲区，并未同步到文件中，所以修改后要将内存与文件同步可以用fflush（FILE *fp）函数同步。
                 */
                size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
                if (r != data.size())
                {
                    return IOError(filename_, errno);
                }
                return Status::OK();
            }
            
            virtual Status Close()
            {
                Status result;
                if (fclose(file_) != 0)
                {
                    result = IOError(filename_, errno);
                }
                file_ = NULL;
                return result;
            }
            
            virtual Status Flush()
            {
                /**
                 函数名: fflush
                 功 能: 清除读写缓冲区，需要立即把输出缓冲区的数据进行物理写入时
                 头文件：stdio.h
                 原型:int fflush(FILE *stream)
                 其中stream是要冲洗的流
                 */
                if (fflush_unlocked(file_) != 0)
                {
                    return IOError(filename_, errno);
                }
                return Status::OK();
            }
            
            Status SyncDirIfManifest()
            {
                const char* f = filename_.c_str();
                /**
                 函数名称： strrchr
                 函数原型：char *strrchr(const char *str, char c);
                 所属库： string.h
                 函数功能：查找一个字符c在另一个字符串str中末次出现的位置（也就是从str的右侧开始查找字符c首次出现的位置），并返回这个位置的地址。
                 如果未能找到指定字符，那么函数将返回NULL。
                 */
                const char* sep = strrchr(f, '/');
                Slice basename;
                std::string dir;
                if (sep == NULL)
                {
                    dir = ".";
                    basename = f;
                } else
                {
                    dir = std::string(f, sep - f);
                    basename = sep + 1;
                }
                Status s;
                if (basename.starts_with("MANIFEST"))
                {
                    int fd = open(dir.c_str(), O_RDONLY);
                    if (fd < 0)
                    {
                        s = IOError(dir, errno);
                    } else
                    {
                        /**
                         fsync函数同步内存中所有已修改的文件数据到储存设备。参数fd是该进程打开来的文件描述符。 
                         函数成功执行时，返回0。失败返回-1
                         */
                        if (fsync(fd) < 0)
                        {
                            s = IOError(dir, errno);
                        }
                        close(fd);
                    }
                }
                return s;
            }
            
            virtual Status Sync()
            {
                // Ensure new files referred to by the manifest are in the filesystem.
                Status s = SyncDirIfManifest();
                if (!s.ok())
                {
                    return s;
                }
                /**
                 函数功能：fileno()用来取得参数stream指定的文件流所使用的文件描述符
                 返回值：某个数据流的文件描述符
                 */
                if (fflush_unlocked(file_) != 0 || fdatasync(fileno(file_)) != 0)
                {
                    s = Status::IOError(filename_, strerror(errno));
                }
                return s;
            }
        };
        
        static int LockOrUnlock(int fd, bool lock)
        {
            errno = 0;
            /**
             struct flock
             {
             short int l_type;
             short int l_whence;
             off_t l_start;
             off_t l_len;
             pid_t l_pid;
             };
             l_type 有三种状态:
             F_RDLCK 建立一个供读取用的锁定
             F_WRLCK 建立一个供写入用的锁定
             F_UNLCK 删除之前建立的锁定
             l_whence 也有三种方式:
             SEEK_SET 以文件开头为锁定的起始位置。
             SEEK_CUR 以目前文件读写位置为锁定的起始位置
             SEEK_END 以文件结尾为锁定的起始位置。
             l_start 表示相对l_whence位置的偏移量，两者一起确定锁定区域的开始位置。
             l_len表示锁定区域的长度，若果为0表示从起点(由l_whence和 l_start决定的开始位置)开始直到最大可能偏移量为止。
             即不管在后面增加多少数据都在锁的范围内。
             返回值 成功返回依赖于cmd的值，若有错误则返回-1，错误原因存于errno.
             */
            struct flock f;
            memset(&f, 0, sizeof(f));
            f.l_type = (lock ? F_WRLCK : F_UNLCK);
            f.l_whence = SEEK_SET;
            f.l_start = 0;
            f.l_len = 0;        // Lock/unlock entire file
            /**
             fcntl函数可以改变已打开的文件性质
             */
            return fcntl(fd, F_SETLK, &f);
        }
        
        class PosixFileLock : public FileLock
        {
        public:
            int fd_;
            std::string name_;
        };
        
        /*****************************************************************
         类：PosixLockTable
         *****************************************************************/
        // Set of locked files.  We keep a separate set instead of just
        // relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
        // any protection against multiple uses from the same process.
        class PosixLockTable
        {
        private:
            port::Mutex mu_;
            std::set<std::string> locked_files_;
        public:
            bool Insert(const std::string& fname)
            {
                MutexLock l(&mu_);
                return locked_files_.insert(fname).second; // 返回值 pair<iterator,bool> bool是否插入成功
            }
            void Remove(const std::string& fname)
            {
                MutexLock l(&mu_);
                locked_files_.erase(fname);
            }
        };
        
        /*****************************************************************
         类：PosixEnv
         *****************************************************************/
        class PosixEnv : public Env
        {
        public:
            PosixEnv();
            virtual ~PosixEnv()
            {
                char msg[] = "Destroying Env::Default()\n";
                fwrite(msg, 1, sizeof(msg), stderr);
                abort();//异常终止一个进程。中止当前进程，返回一个错误代码。错误代码的缺省值是3。
            }
            // 顺序读文件
            virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result)
            {
                FILE* f = fopen(fname.c_str(), "r");
                if (f == NULL)
                {
                    *result = NULL;
                    return IOError(fname, errno);
                } else
                {
                    *result = new PosixSequentialFile(fname, f);
                    return Status::OK();
                }
            }
            // 随机读文件
            virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)
            {
                *result = NULL;
                Status s;
                int fd = open(fname.c_str(), O_RDONLY);
                if (fd < 0)
                {
                    s = IOError(fname, errno);
                } else if (mmap_limit_.Acquire())
                {
                    uint64_t size;
                    s = GetFileSize(fname, &size);
                    if (s.ok())
                    {
                        /**
                         mmap将一个文件或者其它对象映射进内存。文件被映射到多个页上，如果文件的大小不是所有页的大小之和，最后一个页不被使用的空间将会清零。
                         mmap在用户空间映射调用系统中作用很大。
                         void* mmap(void* start,size_t length,int prot,int flags,int fd,off_t offset);
                         参数说明:
                         start：映射区的开始地址，设置为0时表示由系统决定映射区的起始地址。
                         length：映射区的长度。//长度单位是 以字节为单位，不足一内存页按一内存页处理
                         prot：期望的内存保护标志，不能与文件的打开模式冲突。是以下的某个值，可以通过or运算合理地组合在一起
                         PROT_EXEC //页内容可以被执行
                         PROT_READ //页内容可以被读取
                         PROT_WRITE //页可以被写入
                         PROT_NONE //页不可访问
                         flags：指定映射对象的类型，映射选项和映射页是否可以共享。它的值可以是一个或者多个以下位的组合体
                         MAP_FIXED //使用指定的映射起始地址，如果由start和len参数指定的内存区重叠于现存的映射空间，重叠部分将会被丢弃。如果指定的起始地址不可用，操作将会失败。并且起始地址必须落在页的边界上。
                         MAP_SHARED //与其它所有映射这个对象的进程共享映射空间。对共享区的写入，相当于输出到文件。直到msync()或者munmap()被调用，文件实际上不会被更新。
                         MAP_PRIVATE //建立一个写入时拷贝的私有映射。内存区域的写入不会影响到原文件。这个标志和以上标志是互斥的，只能使用其中一个。
                         MAP_DENYWRITE //这个标志被忽略。
                         MAP_EXECUTABLE //同上
                         MAP_NORESERVE //不要为这个映射保留交换空间。当交换空间被保留，对映射区修改的可能会得到保证。当交换空间不被保留，同时内存不足，对映射区的修改会引起段违例信号。
                         MAP_LOCKED //锁定映射区的页面，从而防止页面被交换出内存。
                         MAP_GROWSDOWN //用于堆栈，告诉内核VM系统，映射区可以向下扩展。
                         MAP_ANONYMOUS //匿名映射，映射区不与任何文件关联。
                         MAP_ANON //MAP_ANONYMOUS的别称，不再被使用。
                         MAP_FILE //兼容标志，被忽略。
                         MAP_32BIT //将映射区放在进程地址空间的低2GB，MAP_FIXED指定时会被忽略。当前这个标志只在x86-64平台上得到支持。
                         MAP_POPULATE //为文件映射通过预读的方式准备好页表。随后对映射区的访问不会被页违例阻塞。
                         MAP_NONBLOCK //仅和MAP_POPULATE一起使用时才有意义。不执行预读，只为已存在于内存中的页面建立页表入口。
                         fd：有效的文件描述词。一般是由open()函数返回，其值也可以设置为-1，此时需要指定flags参数中的MAP_ANON,表明进行的是匿名映射。
                         off_toffset：被映射对象内容的起点。
                         返回说明:
                         成功执行时，mmap()返回被映射区的指针，munmap()返回0。失败时，mmap()返回MAP_FAILED[其值为(void *)-1]，munmap返回-1。
                         */
                        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
                        if (base != MAP_FAILED)
                        {
                            *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
                        } else
                        {
                            s = IOError(fname, errno);
                        }
                    }
                    close(fd);
                    if (!s.ok())
                    {
                        mmap_limit_.Release();
                    }
                } else
                {
                    *result = new PosixRandomAccessFile(fname, fd);
                }
                return s;
            }
            // 顺序写文件
            virtual Status NewWritableFile(const std::string& fname, WritableFile** result)
            {
                Status s;
                FILE* f = fopen(fname.c_str(), "w");
                if (f == NULL)
                {
                    *result = NULL;
                    s = IOError(fname, errno);
                } else
                {
                    *result = new PosixWritableFile(fname, f);
                }
                return s;
            }
            // 文件是否存在
            virtual bool FileExists(const std::string& fname)
            {
                /**
                 int access(const char *pathname, int mode);
                 参数说明：
                 pathname: 文件路径名。
                 mode: 操作模式，可能值是一个或多个R_OK(可读?), W_OK(可写?), X_OK(可执行?) 或 F_OK(文件存在?)组合体。
                 返回说明：
                 成功执行时，返回0。失败返回-1
                 */
                return access(fname.c_str(), F_OK) == 0;
            }
            // 得到目录子级
            virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result)
            {
                result->clear();
                DIR* d = opendir(dir.c_str());
                if (d == NULL)
                {
                    return IOError(dir, errno);
                }
                struct dirent* entry;
                while ((entry = readdir(d)) != NULL)
                {
                    result->push_back(entry->d_name);
                }
                closedir(d);
                return Status::OK();
            }
            // 删除文件
            virtual Status DeleteFile(const std::string& fname)
            {
                Status result;
                // 删除一个文件的目录项并减少它的链接数，若成功则返回0，否则返回-1
                if (unlink(fname.c_str()) != 0)
                {
                    result = IOError(fname, errno);
                }
                return result;
            }
            // 创建目录
            virtual Status CreateDir(const std::string& name)
            {
                Status result;
                /**
                 int mkdir(const char *pathname, mode_t mode);
                 函数说明：
                 mkdir()函数以mode方式创建一个以参数pathname命名的目录，mode定义新创建目录的权限。
                 mode方式：可多个权限相或，如0755表示S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH
                 代表：该文件所有者拥有读，写和执行操作的权限，该文件用户组拥有可读、可执行的权限，其他用户拥有可读、可执行的权限。
                 S_IRWXU 00700权限，代表该文件所有者拥有读，写和执行操作的权限
                 S_IRUSR(S_IREAD)  00400权限，代表该文件所有者拥有可读的权限
                 S_IWUSR(S_IWRITE) 00200权限，代表该文件所有者拥有可写的权限
                 S_IXUSR(S_IEXEC)  00100权限，代表该文件所有者拥有执行的权限
                 S_IRWXG 00070权限，代表该文件用户组拥有读，写和执行操作的权限
                 S_IRGRP 00040权限，代表该文件用户组拥有可读的权限
                 S_IWGRP 00020权限，代表该文件用户组拥有可写的权限
                 S_IXGRP 00010权限，代表该文件用户组拥有执行的权限
                 S_IRWXO 00007权限，代表其他用户拥有读，写和执行操作的权限
                 S_IROTH 00004权限，代表其他用户拥有可读的权限
                 S_IWOTH 00002权限，代表其他用户拥有可写的权限
                 S_IXOTH 00001权限，代表其他用户拥有执行的权限
                 返回值：
                 若目录创建成功，则返回0；否则返回-1。
                 */
                if (mkdir(name.c_str(), 0755) != 0)
                {
                    result = IOError(name, errno);
                }
                return result;
            }
            // 删除目录
            virtual Status DeleteDir(const std::string& name)
            {
                Status result;
                if (rmdir(name.c_str()) != 0)
                {
                    result = IOError(name, errno);
                }
                return result;
            }
            // 得到文件大小
            virtual Status GetFileSize(const std::string& fname, uint64_t* size)
            {
                Status s;
                struct stat sbuf;
                /**
                 int stat(const char *file_name, struct stat *buf);
                 函数说明:
                 通过文件名filename获取文件信息，并保存在buf所指的结构体stat中
                 返回值:
                 执行成功则返回0，失败返回-1
                 */
                if (stat(fname.c_str(), &sbuf) != 0)
                {
                    *size = 0;
                    s = IOError(fname, errno);
                } else
                {
                    *size = sbuf.st_size;
                }
                return s;
            }
            // 重命名文件
            virtual Status RenameFile(const std::string& src, const std::string& target)
            {
                Status result;
                if (rename(src.c_str(), target.c_str()) != 0)
                {
                    result = IOError(src, errno);
                }
                return result;
            }
            // 锁文件
            virtual Status LockFile(const std::string& fname, FileLock** lock)
            {
                *lock = NULL;
                Status result;
                int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
                if (fd < 0)
                {
                    result = IOError(fname, errno);
                } else if (!locks_.Insert(fname))
                {
                    close(fd);
                    result = Status::IOError("lock " + fname, "already held by process");
                } else if (LockOrUnlock(fd, true) == -1)
                {
                    result = IOError("lock " + fname, errno);
                    close(fd);
                    locks_.Remove(fname);
                } else
                {
                    PosixFileLock* my_lock = new PosixFileLock;
                    my_lock->fd_ = fd;
                    my_lock->name_ = fname;
                    *lock = my_lock;
                }
                return result;
            }
            // 解锁文件
            virtual Status UnlockFile(FileLock* lock)
            {
                PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
                Status result;
                if (LockOrUnlock(my_lock->fd_, false) == -1)
                {
                    result = IOError("unlock", errno);
                }
                locks_.Remove(my_lock->name_);
                close(my_lock->fd_);
                delete my_lock;
                return result;
            }
            
            virtual void Schedule(void (*function)(void*), void* arg);
            
            virtual void StartThread(void (*function)(void* arg), void* arg);
            // 得到测试目录
            virtual Status GetTestDirectory(std::string* result)
            {
                const char* env = getenv("TEST_TMPDIR");
                if (env && env[0] != '\0')
                {
                    *result = env;
                } else
                {
                    char buf[100];
                    snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
                    *result = buf;
                }
                // Directory may already exist
                CreateDir(*result);
                return Status::OK();
            }
            // 得到线程ID
            static uint64_t gettid()
            {
                pthread_t tid = pthread_self(); // 线程ID
                uint64_t thread_id = 0;
                memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
                return thread_id;
            }
            // 新建log文件
            virtual Status NewLogger(const std::string& fname, Logger** result)
            {
                FILE* f = fopen(fname.c_str(), "w");
                if (f == NULL)
                {
                    *result = NULL;
                    return IOError(fname, errno);
                } else
                {
                    *result = new PosixLogger(f, &PosixEnv::gettid);
                    return Status::OK();
                }
            }
            // 现在时间
            virtual uint64_t NowMicros()
            {
                struct timeval tv;
                gettimeofday(&tv, NULL);//得到时间。它的精度可以达到微妙
                return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
            }
            // 挂起进程
            virtual void SleepForMicroseconds(int micros)
            {
                usleep(micros);//把进程挂起一段时间，单位是微秒
            }
            
        private:
            void PthreadCall(const char* label, int result)
            {
                if (result != 0) // 线程调用失败时，关闭进程
                {
                    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
                    abort();
                }
            }
            
            // BGThread() is the body of the background thread
            void BGThread();
            static void* BGThreadWrapper(void* arg)
            {
                reinterpret_cast<PosixEnv*>(arg)->BGThread();
                return NULL;
            }
            
            pthread_mutex_t mu_;
            pthread_cond_t bgsignal_;
            pthread_t bgthread_;
            bool started_bgthread_;
            
            // Entry per Schedule() call
            struct BGItem { void* arg; void (*function)(void*); };
            typedef std::deque<BGItem> BGQueue;
            BGQueue queue_;
            
            PosixLockTable locks_;
            MmapLimiter mmap_limit_;
        };
        
        /*****************************************************************
         类：PosixEnv 一些方法的实现
         *****************************************************************/
        // PosixEnv的构造方法
        PosixEnv::PosixEnv() : started_bgthread_(false)
        {
            PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
            PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
        }
        // PosixEnv的虚构方法
        void PosixEnv::Schedule(void (*function)(void*), void* arg)
        {
            PthreadCall("lock", pthread_mutex_lock(&mu_));
            
            // Start background thread if necessary
            if (!started_bgthread_)
            {
                started_bgthread_ = true;
                /**
                 int pthread_create(pthread_t *tidp,const pthread_attr_t *attr,(void*)(*start_rtn)(void*),void *arg);
                 参数说明：
                 第一个参数为指向线程标识符的指针。
                 第二个参数用来设置线程属性。
                 第三个参数是线程运行函数的起始地址。
                 最后一个参数是运行函数的参数。
                 返回值：
                 若线程创建成功，则返回0。若线程创建失败，则返回出错编号
                 */
                PthreadCall("create thread", pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
            }
            
            // If the queue is currently empty, the background thread may currently be
            // waiting.
            if (queue_.empty())
            {
                /**
                 pthread_cond_signal函数的作用是发送一个信号给另外一个正在处于阻塞等待状态的线程,使其脱离阻塞状态,继续执行.
                 如果没有线程处在阻塞等待状态,pthread_cond_signal也会成功返回。
                 */
                PthreadCall("signal", pthread_cond_signal(&bgsignal_));
            }
            
            // Add to priority queue
            queue_.push_back(BGItem());
            queue_.back().function = function;
            queue_.back().arg = arg;
            
            PthreadCall("unlock", pthread_mutex_unlock(&mu_));
        }
        
        void PosixEnv::BGThread()
        {
            while (true)
            {
                // Wait until there is an item that is ready to run
                PthreadCall("lock", pthread_mutex_lock(&mu_));
                while (queue_.empty())
                {
                    PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
                }
                
                void (*function)(void*) = queue_.front().function;
                void* arg = queue_.front().arg;
                queue_.pop_front();
                
                PthreadCall("unlock", pthread_mutex_unlock(&mu_));
                (*function)(arg);
            }
        }
        
        namespace
        {
            struct StartThreadState
            {
                void (*user_function)(void*);
                void* arg;
            };
        }
        static void* StartThreadWrapper(void* arg)
        {
            StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
            state->user_function(state->arg);
            delete state;
            return NULL;
        }
        
        void PosixEnv::StartThread(void (*function)(void* arg), void* arg)
        {
            pthread_t t;
            StartThreadState* state = new StartThreadState;
            state->user_function = function;
            state->arg = arg;
            PthreadCall("start thread", pthread_create(&t, NULL,  &StartThreadWrapper, state));
        }
        
    }  // namespace
    
    // 单类
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    static Env* default_env;
    static void InitDefaultEnv() { default_env = new PosixEnv; }
    
    Env* Env::Default()
    {
        pthread_once(&once, InitDefaultEnv);
        return default_env;
    }
    
}  // namespace leveldb
