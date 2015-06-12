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

#ifndef UTILS_HPP_
#define UTILS_HPP_

#include <time.h>
#include <stdint.h>
#include <sys/time.h>
#include <string>
#include <math.h>
#include <stdlib.h>
namespace mmkv
{
    inline uint64_t get_current_micros()
    {
        struct timeval tv;
        long long ust;

        gettimeofday(&tv, NULL);
        ust = ((long) tv.tv_sec) * 1000000;
        ust += tv.tv_usec;
        return ust;
    }
    bool is_file_exist(const std::string& path);
    bool is_dir_exist(const std::string& path);
    bool make_dir(const std::string& para_path);

    int string2ll(const char *s, size_t slen, long long *value);
    int string2l(const char *s, size_t slen, long *lval);
    int ll2string(char* dst, size_t dstlen, long long svalue);
    int string2double(const char *s, size_t slen, double *value);
    int double2string(char* dst, size_t dstlen, long double dvalue, bool humanfriendly);

    uint32_t digits10(uint64_t v);
    inline bool is_integer(double v)
    {
        double int_part;
        return modf(v, &int_part) == 0.0;
    }

    inline int32_t random_between_int32(int32_t min, int32_t max)
    {
        if (min == max)
        {
            return min;
        }
        srandom(time(NULL));
        int diff = max - min;
        return (int) (((double) (diff + 1) / RAND_MAX) * rand() + min);
    }
}

#endif /* UTILS_HPP_ */
