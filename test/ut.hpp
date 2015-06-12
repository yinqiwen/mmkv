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

#ifndef TEST_UT_HPP_
#define TEST_UT_HPP_

#include <stdio.h>
#include <string>
#include <map>
#include <vector>
#include <set>
#include <sstream>
#include "mmkv.hpp"
#include "types.hpp"

// Expands to the name of the class that implements the given test.
#define GTEST_TEST_CLASS_NAME_(test_case_name, test_name) \
  test_case_name##_##test_name##_Test

#define GTEST_TEST_(test_case_name, test_name)\
class GTEST_TEST_CLASS_NAME_(test_case_name, test_name) : public mmkv::TestCase {\
 public:\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)() {mmkv::GetGlobalRegister()->RegisterTestCase(#test_case_name, #test_name, this);}\
 private:\
  virtual void TestBody();\
  const char* TestName() { return #test_name;}\
  const char* TestCaseName() { return #test_case_name;}\
  static GTEST_TEST_CLASS_NAME_(test_case_name, test_name)* const singleton;\
};\
\
GTEST_TEST_CLASS_NAME_(test_case_name, test_name)* const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::singleton = new GTEST_TEST_CLASS_NAME_(test_case_name, test_name);\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestBody()

#define TEST(test_case_name, test_name)\
  GTEST_TEST_(test_case_name, test_name)


#define FORCE_CORE_DUMP() do {int *p = NULL; *p=0;} while(0)

/* Evaluates to the same boolean value as 'p', and hints to the compiler that
 * we expect this value to be false. */
#ifdef __GNUC__
#define __UNLIKELY(p) __builtin_expect(!!(p),0)
#else
#define __UNLIKELY(p) (p)
#endif

#define ABORT(msg) \
    do {                                \
            (void)fprintf(stderr,  \
                "%s:%d: Failed %s ",     \
                __FILE__,__LINE__,msg);      \
            abort();                    \
    } while (0)

#define ASSERT(cond)                     \
    do {                                \
        if (__UNLIKELY(!(cond))) {             \
            /* In case a user-supplied handler tries to */  \
            /* return control to us, log and abort here. */ \
            (void)fprintf(stderr,               \
                "%s:%d: Assertion %s failed in %s",     \
                __FILE__,__LINE__,#cond,__func__);      \
            abort();                    \
        }                           \
    } while (0)

#define ALLOC_ASSERT(x) \
    do {\
        if (__UNLIKELY (!x)) {\
            fprintf (stderr, "FATAL ERROR: OUT OF MEMORY (%s:%d)\n",\
                __FILE__, __LINE__);\
            abort();\
        }\
    } while (false)

#define CHECK_FATAL(cond, ...)  do{\
    if(cond){\
         (void)fprintf(stderr,               \
                        "\e[1;35m%-6s\e[m%s:%d: Assertion %s failed in %s\n",     \
                        "[FAIL]", __FILE__,__LINE__,#cond,__func__);      \
         fprintf(stderr, "\e[1;35m%-6s\e[m", "[FAIL]:"); \
         fprintf(stderr, __VA_ARGS__);\
         fprintf(stderr, "\n"); \
         exit(-1);\
    }else{\
    }\
}while(0)

#define CHECK_CMP(TYPE, cond1, cond2, cmp, ...)  do{\
    TYPE cond1_v  = cond1; TYPE cond2_v = cond2; \
    if(!(cond1_v cmp cond2_v)){\
         (void)fprintf(stderr,               \
                        "\e[1;35m%-6s\e[m%s:%d: Assertion %s %s %s failed in %s\n",     \
                        "[FAIL]", __FILE__,__LINE__,#cond1, #cmp, #cond2, __func__);      \
         std::stringstream ss; \
         ss << #cond1 << " == " << cond1_v << " NOT " << #cmp <<" value:" << cond2; \
         std::string tmpstr; \
         ss.str(tmpstr); \
         fprintf(stderr, "\e[1;35m%-6s\e[m", "[FAIL]:"); \
         fprintf(stderr, "%s ", tmpstr.c_str()); \
         fprintf(stderr, __VA_ARGS__);\
         fprintf(stderr, "\n"); \
         exit(-1);\
    }else{\
    }\
}while(0)

#define CHECK_EQ(TYPE, cond1, cond2, ...) CHECK_CMP(TYPE, cond1, cond2, ==, __VA_ARGS__)

namespace mmkv
{
    class TestCase
    {
        public:
            virtual void TestBody() = 0;
            virtual const char* TestName() = 0;
            virtual const char* TestCaseName() = 0;
            virtual ~TestCase()
            {
            }
    };

    class Register
    {
        private:
            typedef std::vector<TestCase*> TestCaseArray;
            TestCaseArray m_all_tests;
        public:
            void RegisterTestCase(const std::string& test_case_name, const std::string& test_name, TestCase* test_case);
            void RunAll();
    };
    void RunAllTests();
    Register* GetGlobalRegister();

}
extern mmkv::MMKV* g_test_kv;

#endif /* TEST_UT_HPP_ */
