/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#include "logger_macros.hpp"
#include "utils.hpp"
#include <stdarg.h>
#include <stdio.h>
#include <sstream>
#include <sys/types.h>
#include <unistd.h>
namespace mmkv
{
    static const char* kLogLevelNames[] =
        { "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE" };
    static void default_loghandler(LogLevel level, const char* filename, const char* function, int line,
            const char* msg, int msg_len)
    {
        uint64_t timestamp = get_current_micros();
        //uint32_t mills = (timestamp / 1000) % 1000;
        char timetag[256];
        struct tm tm;
        time_t now = timestamp / 1000000;
        localtime_r(&now, &tm);
        sprintf(timetag, "%02u-%02u %02u:%02u:%02u", tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        fprintf(stdout, "%s %s:%d %s\n", timetag, filename, line, msg);
        fflush(stdout);
    }
    Logger::Logger() :
            loglevel(INFO_LOG_LEVEL), logfunc(default_loghandler)
    {
    }

    void Logger::operator()(LogLevel level, const char* filename, const char* function, int line, const char* format,
            ...)
    {
        if (NULL == logfunc)
        {
            return;
        }
        const char* levelstr = 0;
        if (level > 0 && level < ALL_LOG_LEVEL)
        {
            levelstr = kLogLevelNames[level - 1];
        }
        else
        {
            levelstr = "???";
        }
        size_t log_line_size = 1024;
        va_list args;
        va_start(args, format);
        char content[log_line_size + 1];
        int sz = vsnprintf(content, log_line_size, format, args);
        va_end(args);
        content[sz] = 0;

        (*logfunc)(level, filename, function, line, content, sz);
    }
}

