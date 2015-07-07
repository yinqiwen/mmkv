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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <limits.h>
#include "utils.hpp"
#include "lz4.h"
namespace mmkv
{
    bool is_file_exist(const std::string& path)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        if (0 == ret)
        {
            return S_ISREG(buf.st_mode);
        }
        return false;
    }

    bool is_dir_exist(const std::string& path)
    {
        struct stat buf;
        int ret = stat(path.c_str(), &buf);
        if (0 == ret)
        {
            return S_ISDIR(buf.st_mode);
        }
        return false;
    }
    bool make_dir(const std::string& para_path)
    {
        if (is_dir_exist(para_path))
        {
            return true;
        }
        if (is_file_exist(para_path))
        {
            //ERROR_LOG("Exist file '%s' is not a dir.", para_path.c_str());
            return false;
        }
        std::string path = para_path;
        size_t found = path.rfind("/");
        if (found == path.size() - 1)
        {
            path = path.substr(0, path.size() - 1);
            found = path.rfind("/");
        }
        if (found != std::string::npos)
        {
            std::string base_dir = path.substr(0, found);
            if (make_dir(base_dir))
            {
                //mode is 0755
                return mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
            }
        }
        else
        {
            return mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0;
        }
        return false;
    }

    /* Convert a string into a long long. Returns 1 if the string could be parsed
     * into a (non-overflowing) long long, 0 otherwise. The value will be set to
     * the parsed value when appropriate. */
    int string2ll(const char *s, size_t slen, long long *value)
    {
        const char *p = s;
        size_t plen = 0;
        int negative = 0;
        unsigned long long v;

        if (plen == slen)
            return 0;

        /* Special case: first and only digit is 0. */
        if (slen == 1 && p[0] == '0')
        {
            if (value != NULL)
                *value = 0;
            return 1;
        }

        if (p[0] == '-')
        {
            negative = 1;
            p++;
            plen++;

            /* Abort on only a negative sign. */
            if (plen == slen)
                return 0;
        }

        /* First digit should be 1-9, otherwise the string should just be 0. */
        if (p[0] >= '1' && p[0] <= '9')
        {
            v = p[0] - '0';
            p++;
            plen++;
        }
        else if (p[0] == '0' && slen == 1)
        {
            *value = 0;
            return 1;
        }
        else
        {
            return 0;
        }

        while (plen < slen && p[0] >= '0' && p[0] <= '9')
        {
            if (v > (ULLONG_MAX / 10)) /* Overflow. */
                return 0;
            v *= 10;

            if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
                return 0;
            v += p[0] - '0';

            p++;
            plen++;
        }

        /* Return if not all bytes were used. */
        if (plen < slen)
            return 0;

        if (negative)
        {
            if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
                return 0;
            if (value != NULL)
                *value = -v;
        }
        else
        {
            if (v > LLONG_MAX) /* Overflow. */
                return 0;
            if (value != NULL)
                *value = v;
        }
        return 1;
    }
    int string2l(const char *s, size_t slen, long *lval)
    {
        long long llval;

        if (!string2ll(s, slen, &llval))
            return 0;

        if (llval < LONG_MIN || llval > LONG_MAX)
            return 0;

        *lval = (long) llval;
        return 1;
    }
    /* Convert a long long into a string. Returns the number of
     * characters needed to represent the number.
     * If the buffer is not big enough to store the string, 0 is returned.
     *
     * Based on the following article (that apparently does not provide a
     * novel approach but only publicizes an already used technique):
     *
     * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
     *
     * Modified in order to handle signed integers since the original code was
     * designed for unsigned integers. */
    int ll2string(char* dst, size_t dstlen, long long svalue)
    {
        static const char digits[201] = "0001020304050607080910111213141516171819"
                "2021222324252627282930313233343536373839"
                "4041424344454647484950515253545556575859"
                "6061626364656667686970717273747576777879"
                "8081828384858687888990919293949596979899";
        int negative;
        unsigned long long value;

        /* The main loop works with 64bit unsigned integers for simplicity, so
         * we convert the number here and remember if it is negative. */
        if (svalue < 0)
        {
            if (svalue != LLONG_MIN)
            {
                value = -svalue;
            }
            else
            {
                value = ((unsigned long long) LLONG_MAX) + 1;
            }
            negative = 1;
        }
        else
        {
            value = svalue;
            negative = 0;
        }

        /* Check length. */
        uint32_t const length = digits10(value) + negative;
        if (length > dstlen)
            return 0;

        /* Null term. */
        uint32_t next = length;
        //dst[next] = '\0';
        next--;
        while (value >= 100)
        {
            int const i = (value % 100) * 2;
            value /= 100;
            dst[next] = digits[i + 1];
            dst[next - 1] = digits[i];
            next -= 2;
        }

        /* Handle last 1-2 digits. */
        if (value < 10)
        {
            dst[next] = '0' + (uint32_t) value;
        }
        else
        {
            int i = (uint32_t) value * 2;
            dst[next] = digits[i + 1];
            dst[next - 1] = digits[i];
        }

        /* Add sign. */
        if (negative)
            dst[0] = '-';
        return length;
    }

    int string2double(const char *s, size_t slen, double *value)
    {
        char *eptr;
        double tmp = strtold(s, &eptr);
        if (isspace(s[0]) || eptr[0] != '\0' ||
        errno == ERANGE || isnan(tmp))
        {
            return 0;
        }
        *value = tmp;
        return 1;
    }

    int double2string(char* dst, size_t dstlen, long double dvalue, bool humanfriendly)
    {
        int len = 0;
        if (humanfriendly)
        {
            /* We use 17 digits precision since with 128 bit floats that precision
             * after rounding is able to represent most small decimal numbers in a
             * way that is "non surprising" for the user (that is, most small
             * decimal numbers will be represented in a way that when converted
             * back into a string are exactly the same as what the user typed.) */
            len = snprintf(dst, dstlen, "%.17Lf", dvalue);
            /* Now remove trailing zeroes after the '.' */
            if (strchr(dst, '.') != NULL)
            {
                char *p = dst + len - 1;
                while (*p == '0')
                {
                    p--;
                    len--;
                }
                if (*p == '.')
                    len--;
            }
        }
        else
        {
            len = snprintf(dst, dstlen, "%.17Lg", dvalue);
        }
        return len;
    }

    static const uint64_t P12 = 10000LL * 10000LL * 10000LL;
    static const uint64_t P11 = 1000000LL * 100000LL;
    static const uint64_t P10 = 100000LL * 100000LL;
    static const uint64_t P09 = 100000L * 10000L;
    static const uint64_t P08 = 10000L * 10000L;
    static const uint64_t P07 = 10000L * 1000L;
    static const uint64_t P06 = 1000L * 1000L;
    uint32_t digits10(uint64_t v)
    {
        if (v < 10)
            return 1;
        if (v < 100)
            return 2;
        if (v < 1000)
            return 3;
        if (v < P12)
        {
            if (v < P08)
            {
                if (v < P06)
                {
                    if (v < 10000)
                        return 4;
                    return 5 + (v >= 100000);
                }
                return 7 + (v >= P07);
            }
            if (v < P10)
            {
                return 9 + (v >= P09);
            }
            return 11 + (v >= P11);
        }
        return 12 + digits10(v / P12);
    }

    int lz4_compress_tofile(const char* in, size_t in_size, FILE *out, lz4_compress_callback* cb, void* data)
    {
        size_t messageMaxBytes = 1024 * 1024;
        LZ4_stream_t* const lz4Stream = LZ4_createStream();
        const size_t cmpBufBytes = LZ4_COMPRESSBOUND(messageMaxBytes);
        char cmpBuf[cmpBufBytes];
        int inp_offset = 0;

        size_t rest = in_size - inp_offset;
        while (rest > 0)
        {
            const char* inp_ptr = in + inp_offset;
            // Read line to the ring buffer.
            uint32_t inpBytes = rest > messageMaxBytes ? messageMaxBytes : rest;
            const int cmpBytes = LZ4_compress_continue(lz4Stream, inp_ptr, cmpBuf, inpBytes);
            if (cmpBytes <= 0)
                break;
            if (fwrite(&inpBytes, sizeof(uint32_t), 1, out) != 1)
            {
                LZ4_freeStream(lz4Stream);
                return -1;
            }
            if (fwrite(&cmpBytes, sizeof(uint32_t), 1, out) != 1)
            {
                LZ4_freeStream(lz4Stream);
                return -1;
            }
            if (fwrite(cmpBuf, 1, cmpBytes, out) != cmpBytes)
            {
                LZ4_freeStream(lz4Stream);
                return -1;
            }
            if (NULL != cb)
            {
                cb(inp_ptr, inpBytes, data);
            }
            inp_offset += inpBytes;
            rest = in_size - inp_offset;
        }
        uint32_t end_tag = 0;
        if (fwrite(&end_tag, sizeof(uint32_t), 1, out) != 1)
        {
            return -1;
        }
        LZ4_freeStream(lz4Stream);
        return 0;
    }

    int lz4_decompress_tofile(const char* in, size_t in_size, FILE *out, size_t* decomp_size,
            lz4_decompress_callback* cb, void* data)
    {
        LZ4_streamDecode_t* const lz4StreamDecode = LZ4_createStreamDecode();
        size_t messageMaxBytes = 1024 * 1024;
        uint32_t total_len = 0;
        char decom_buf[messageMaxBytes];

        size_t inp_offset = 0;
        size_t rest = in_size - inp_offset;
        *decomp_size = 0;
        while (rest > 0)
        {
            uint32_t orig_bytes = 0, cmp_bytes = 0;
            if (rest < sizeof(uint32_t))
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }
            memcpy(&orig_bytes, in + inp_offset, sizeof(uint32_t));

            if (orig_bytes > messageMaxBytes)
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }
            inp_offset += sizeof(uint32_t);
            rest -= sizeof(uint32_t);
            if (0 == orig_bytes)
            {
                break;
            }
            if (rest < sizeof(uint32_t))
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }
            memcpy(&cmp_bytes, in + inp_offset, sizeof(uint32_t));
            if (cmp_bytes == 0)
                break;
            inp_offset += sizeof(uint32_t);
            rest -= sizeof(uint32_t);
            if (rest < cmp_bytes)
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }
            const int decBytes = LZ4_decompress_safe_continue(lz4StreamDecode, in + inp_offset, decom_buf, cmp_bytes, messageMaxBytes);
            if (decBytes <= 0 || decBytes != orig_bytes)
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }

            if (fwrite(decom_buf, 1, orig_bytes, out) != orig_bytes)
            {
                LZ4_freeStreamDecode(lz4StreamDecode);
                return -1;
            }
            if (NULL != cb)
            {
                cb(decom_buf, orig_bytes, data);
            }
            inp_offset += cmp_bytes;
            rest -= cmp_bytes;
        }
        LZ4_freeStreamDecode(lz4StreamDecode);
        *decomp_size = inp_offset;
        return 0;
    }
}

