/*
 * test.cpp
 *
 *  Created on: 2015年9月8日
 *      Author: wangqiying
 */
#include <stdio.h>
#include <utility>
#include <string>
#include <algorithm>
#include <new>
#include <boost/functional/hash.hpp>
#include "fixed_hashtable.hpp"
#include "incremental_rehashmap.hpp"

using std::string;
template<typename Key, typename T>
struct SelectKey
{
        typedef const Key& result_type;
        const Key& operator()(const std::pair<const Key, T>& p) const
        {
            return p.first;
        }
};
template<typename Key, typename T>
struct SetKey
{
        void operator()(std::pair<const Key, T>* value,
                const Key& new_key) const
        {
            *const_cast<Key*>(&value->first) = new_key;
            // It would be nice to clear the rest of value here as well, in
            // case it's taking up a lot of memory.  We do this by clearing
            // the value.  This assumes T has a zero-arg constructor!
            value->second = T();
        }
};
int main()
{
    typedef mmkv::fixed_hashtable<std::pair<const string, string>, string,
            boost::hash<string>, SelectKey<string, string>,
            SetKey<string, string>, std::equal_to<string>,
            std::allocator<std::pair<const string, string> > > Table;

    Table ht(10000);
    std::pair<string, string> p;
    p.first = "100";
    p.second = "hello";
    std::pair<Table::iterator, bool> ret = ht.insert_noresize(p);
    printf("#### %d %s\n", ret.second, ret.first->second.c_str());
    Table::iterator fit = ht.find("100");
    printf("#### %d %d %s %s\n", fit == ht.begin(), fit == ht.end(),
            fit->second.c_str(), fit->first.c_str());
    ht.erase("100");
    printf("#### %d\n", ht.size());

    typedef mmkv::incremental_rehashmap<string, string, boost::hash<string>,
            std::equal_to<string>,
            std::allocator<std::pair<const string, string> > > ReHashMap;
    ReHashMap rmap;
    for(int i = 0; i < 100; i++)
    {
        char key[100];
        sprintf(key, "%02d", i);
        std::pair<string, string> p;
        p.first = key;
        p.second = key;
        rmap.insert(p);
    }
    ReHashMap::iterator it = rmap.begin();
    int i = 0;
    while(it != rmap.end())
    {
        printf("####%d %s->%s\n",i, it->first.c_str(), it->second.c_str());
        //rmap.erase(it);
        it++;
        i++;
    }
    rmap.incremental_rehash(1000);
    for(int i = 0; i < 100; i++)
    {
        char key[100];
        sprintf(key, "%02d", i);
        rmap.erase(key);
    }
    rmap.incremental_rehash(1000);
    printf("####$$$$$$$$$$%d\n",rmap.size());
    return 0;
}

