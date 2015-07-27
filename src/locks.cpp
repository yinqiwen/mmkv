/*
 *Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "locks.hpp"
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include "mmkv_logger.hpp"

namespace mmkv
{
    static long sys_futex(void *addr1, int op, int val1, struct timespec *timeout, void *addr2, int val3)
    {
        return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
    }

    FileLock::FileLock() :
            m_fd(-1)
    {
    }
    const std::string& FileLock::LastError()const
    {
        return m_error;
    }
    bool FileLock::Init(const std::string& path)
    {
        struct stat st;
        if (0 != stat(path.c_str(), &st))
        {
            int err = errno;
            char tmp[1024];
            sprintf(tmp, "Failed to init file lock for reason:%s", strerror(err));
            m_error = tmp;
            return false;
        }
        int flags = O_RDONLY;
        if (st.st_mode & S_IFDIR)
        {
            flags |= O_DIRECTORY;
        }
        m_fd = open(path.c_str(), flags);
        if (m_fd == -1)
        {
            int err = errno;
            char tmp[1024];
            sprintf(tmp, "Failed to open file lock for reason:%s", strerror(err));
            m_error = tmp;
            return false;
        }
        return true;
    }

    bool FileLock::Lock(LockMode mode)
    {
        if (-1 == m_fd)
        {
            return false;
        }
        int ret = -1;
        if (mode == WRITE_LOCK)
        {
            ret = flock(m_fd, LOCK_EX);
        }
        else
        {
            ret = flock(m_fd, LOCK_SH);
        }
        return ret == 0;
    }
    bool FileLock::Unlock(LockMode mode)
    {
        if (-1 == m_fd)
        {
            return false;
        }
        int ret = flock(m_fd, LOCK_UN);
        return ret == 0;
    }
    FileLock::~FileLock()
    {
        Unlock();
        if (-1 != m_fd)
        {
            close(m_fd);
        }
    }

    SleepingRWLock::SleepingRWLock()
    {
        memset(&m_lock, 0, sizeof(m_lock));
    }

    bool SleepingRWLock::LockWrite()
    {
        int i;

        /* There is a waiting writer */
        atomic_add(&m_lock.lock.waiters, 256);

        /* Spin for a bit to try to get the lock */
        for (i = 0; i < 100; i++)
        {
            if (!(xchg_8(&m_lock.lock.locked, 1) & 1))
                return true;

            cpu_relax();
        }

        /* Failed, so we need to sleep */
        while (xchg_8(&m_lock.lock.locked, 1) & 1)
        {
            sys_futex(&m_lock.lock, FUTEX_WAIT, m_lock.lock.waiters | 1, NULL, NULL, 0);
        }
        return true;
    }
    bool SleepingRWLock::LockRead()
    {
        while (1)
        {
            unsigned long long val;
            unsigned seq = m_lock.read_wait.seq;

            barrier();

            val = m_lock.lock.l;

            /* Fast path, no readers or writers */
            if (!val)
            {
                if (!cmpxchg(&m_lock.lock.l, 0, 1 + 2 + (1ULL << 33)))
                    return true;

                /* Failed, try again */
                cpu_relax();
                continue;
            }

            /* Fast path, only readers */
            if ((((uint32_t) val) == 1 + 2) && (val >> 33))
            {
                if (cmpxchg(&m_lock.lock.l, val, val + (1ULL << 33)) == val)
                    return true;

                /* Failed, try again */
                cpu_relax();
                continue;
            }

            /* read flag set? */
            if (val & (1ULL << 32))
            {
                /* Have a waiting reader, append to wait-list */
                m_lock.read_wait.contend = 1;

                sys_futex(&m_lock.read_wait, FUTEX_WAIT, seq | 1, NULL, NULL, 0);

                val = m_lock.lock.l;

                /* If there are readers, we can take the lock */
                if (((val & 3) == 3) && (val >> 33))
                {
                    /* Grab a read lock so long as we are allowed to */
                    if (cmpxchg(&m_lock.lock.l, val, val + (1ULL << 33)) == val)
                        return true;
                }

                /* Too slow, try again */
                cpu_relax();
                continue;
            }

            /* Try to set read flag + waiting */
            if (cmpxchg(&m_lock.lock.l, val, val + 256 + (1ULL << 32)) == val)
                break;

            cpu_relax();
        }

        /* Grab write lock */
        while (xchg_8(&m_lock.lock.locked, 1) & 1)
        {
            sys_futex(&m_lock.lock, FUTEX_WAIT, m_lock.lock.waiters | 1, NULL, NULL, 0);
        }

        /* Convert write lock into read lock. Use 2-bit to denote that readers can enter */
        atomic_add(&m_lock.lock.l, 2 - 256 - (1ULL << 32) + (1ULL << 33));

        /* We are waking everyone up */
        if (atomic_xadd(&m_lock.read_wait.seq, 256) & 1)
        {
            m_lock.read_wait.contend = 0;

            /* Wake one thread, and re-queue the rest on the mutex */
            sys_futex(&m_lock.read_wait, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
        }
        return true;
    }
    bool SleepingRWLock::UnlockWrite()
    {
        /* One less writer and also unlock simultaneously */
        if (atomic_add(&m_lock.lock.waiters, -257))
        {
            int i;

            /* Spin for a bit hoping someone will take the lock */
            for (i = 0; i < 200; i++)
            {
                if (m_lock.lock.locked)
                    return true;

                cpu_relax();
            }

            /* Failed, we need to wake someone */
            sys_futex(&m_lock.lock, FUTEX_WAKE, 1, NULL, NULL, 0);
        }
        return true;
    }
    bool SleepingRWLock::UnlockRead()
    {
        /* Unlock... and test to see if we are the last reader */
        if (!(atomic_add(&m_lock.lock.l, -(1ULL << 33)) >> 33))
        {
            m_lock.lock.locked = 0;

            barrier();

            /* We need to wake someone up? */
            if (m_lock.lock.waiters)
            {
                sys_futex(&m_lock.lock, FUTEX_WAKE, 1, NULL, NULL, 0);
            }
        }
        return true;
    }
    bool SleepingRWLock::Lock(LockMode mode)
    {
        switch (mode)
        {
            case READ_LOCK:
            {
                return LockRead();
            }
            case WRITE_LOCK:
            {
                return LockWrite();
            }
            default:
            {
                return false;
            }
        }
    }
    bool SleepingRWLock::Unlock(LockMode mode)
    {
        switch (mode)
        {
            case READ_LOCK:
            {
                return UnlockRead();
            }
            case WRITE_LOCK:
            {
                return UnlockWrite();
            }
            default:
            {
                return false;
            }
        }
    }
}

