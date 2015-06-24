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

#ifndef SRC_LOGGER_MACROS_HPP_
#define SRC_LOGGER_MACROS_HPP_
#include "mmkv_logger.hpp"

namespace mmkv{
    struct Logger
    {
            LogLevel loglevel;
            LoggerFunc* logfunc;
            Logger();
            inline bool IsLogEnable(LogLevel level)
            {
                return logfunc != NULL && level <= loglevel;
            }
            void operator()(LogLevel level, const char* filename, const char* function, int line, const char* format,
                                ...);
    };
}

#define DEBUG_ENABLED() (m_logger.IsLogEnable(mmkv::DEBUG_LOG_LEVEL))
#define TRACE_ENABLED() (m_logger.IsLogEnable(mmkv::TRACE_LOG_LEVEL))
#define ERROR_ENABLED() (m_logger.IsLogEnable(mmkv::ERROR_LOG_LEVEL))
#define INFO_ENABLED()  (m_logger.IsLogEnable(mmkv::INFO_LOG_LEVEL))
#define FATAL_ENABLED() (m_logger.IsLogEnable(mmkv::FATAL_LOG_LEVEL))
#define WARN_ENABLED() (m_logger.IsLogEnable(mmkv::WARN_LOG_LEVEL))
#define LOG_ENABLED(level) (m_logger.IsLogEnable(level))

#define DEBUG_LOG(...) do {\
   if(DEBUG_ENABLED())\
   {                 \
       m_logger(mmkv::DEBUG_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
   }\
}while(0)

#define WARN_LOG(...) do {\
    if(WARN_ENABLED())\
    {                 \
        m_logger(mmkv::WARN_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
    }\
}while(0)

#define TRACE_LOG(...) do {\
    if(TRACE_ENABLED())\
    {                 \
        m_logger(mmkv::TRACE_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
    }\
}while(0)

#define ERROR_LOG(...) do {\
    if(ERROR_ENABLED())\
    {                 \
        m_logger(mmkv::ERROR_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
    }\
}while(0)

#define FATAL_LOG(...) do {\
    if(FATAL_ENABLED())\
    {                 \
        m_logger(mmkv::FATAL_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
    }\
}while(0)

#define INFO_LOG(...)do {\
        if(INFO_ENABLED())\
        {                 \
            m_logger(mmkv::INFO_LOG_LEVEL, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
        }\
    }while(0)

#define LOG_WITH_LEVEL(level, ...) do {\
   if(LOG_ENABLED(level))\
   {                 \
       m_logger(level, __FILE__, __FUNCTION__, __LINE__,__VA_ARGS__); \
   }\
}while(0)


#endif /* SRC_LOGGER_MACROS_HPP_ */
