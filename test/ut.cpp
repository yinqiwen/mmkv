/*
 * ut.cpp
 *
 *  Created on: 2015Äê5ÔÂ21ÈÕ
 *      Author: wangqiying
 */

#include "ut.hpp"

namespace mmkv
{
    static Register* g_register = NULL;

    void Register::RegisterTestCase(const std::string& test_case_name, const std::string& test_name, TestCase* test_case)
    {
        m_all_tests.push_back(test_case);
    }
    void Register::RunAll()
    {
        TestCaseArray::iterator it = m_all_tests.begin();
        while(it != m_all_tests.end())
        {
            TestCase* t = *it;
            t->TestBody();
            fprintf(stdout, "\e[1;32m%-6s\e[m %s %s\n", "[PASS]", t->TestName(), t->TestCaseName());
            it++;
        }
    }

    Register* GetGlobalRegister()
    {
        if(NULL == g_register)
        {
            g_register  = new Register;
        }
        return g_register;
    }

    void RunAllTests()
    {
        g_register->RunAll();
    }
}

