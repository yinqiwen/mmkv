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

#include "lock_guard.hpp"
#include "mmkv_impl.hpp"
#include <algorithm>
#include <vector>

#define OP_DIFF 1
#define OP_UNION 2
#define OP_INTER 3
namespace mmkv
{

    int MMKVImpl::SAdd(DBID db, const Data& key, const DataArray& elements)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        ObjectAllocator allocator = m_segment.ValueAllocator<Object>();
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, true, err)(std::less<Object>(), allocator);
        if (NULL == set || 0 != err)
        {
            return err;
        }
        int inserted = 0;
        for (size_t i = 0; i < elements.size(); i++)
        {
            std::pair<StringSet::iterator, bool> ret = set->insert(elements[i]);
            if (ret.second)
            {
                AssignObjectContent(*(ret.first), elements[i], false);
                inserted++;
            }
        }
        return inserted;
    }
    int MMKVImpl::SCard(DBID db, const Data& key)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (NULL == set || 0 != err)
        {
            return err;
        }
        return set->size();
    }

    int MMKVImpl::GenericSInterDiffUnion(DBID db, int op, const DataArray& keys, const Data* dest, StringArray* res)
    {
        StdObjectSet results[2];
        int result_index = 0;
        StringSetArray sets;
        sets.resize(keys.size());
        int err = 0;
        size_t start_index = 0;
        StringSet* destset = NULL;
        StdObjectSet* result = NULL;
        StdObjectSet* cmp = NULL;
        int current_result_index = 0;

        for (size_t i = 0; i < keys.size(); i++)
        {
            StringSet* set = GetObject<StringSet>(db, keys[i], V_TYPE_SET, false, err)();
            if (IS_NOT_EXISTS(err))
            {
                sets[i] = NULL;
                continue;
            }
            if (0 != err)
            {
                return err;
            }
            sets[i] = set;
        }

        if (NULL != dest)
        {
            ObjectAllocator allocator = m_segment.ValueAllocator<Object>();
            destset = GetObject<StringSet>(db, *dest, V_TYPE_SET, true, err)(std::less<Object>(), allocator);
            if (0 != err)
            {
                return err;
            }
        }

        if (NULL == sets[0])
        {
            if (op == OP_DIFF || op == OP_INTER)
            {
                result_index = 0;
                goto _end;
            }
        }

        for (size_t i = 0; i < keys.size(); i++)
        {
            if (sets[i] != NULL)
            {
                start_index = i;
                break;
            }
        }

        for (size_t i = start_index + 1; i < keys.size(); i++)
        {
            result = results + current_result_index;
            if (NULL == sets[i])
            {
                if (op == OP_INTER)
                {
                    results->clear();
                    result_index = 0;
                    goto _end;
                }
            }
            else
            {
                result->clear();
                switch (op)
                {
                    case OP_DIFF:
                    {
                        if (cmp == NULL)
                        {
                            std::set_difference(sets[start_index]->begin(), sets[start_index]->end(), sets[i]->begin(),
                                    sets[i]->end(), std::inserter(*result, result->end()), std::less<Object>());
                        }
                        else
                        {
                            std::set_difference(cmp->begin(), cmp->end(), sets[i]->begin(), sets[i]->end(),
                                    std::inserter(*result, result->end()), std::less<Object>());
                        }
                        if (result->empty())
                        {
                            result_index = current_result_index;
                            goto _end;
                        }
                        break;
                    }
                    case OP_INTER:
                    {
                        if (cmp == NULL)
                        {
                            std::set_intersection(sets[start_index]->begin(), sets[start_index]->end(),
                                    sets[i]->begin(), sets[i]->end(), std::inserter(*result, result->end()),
                                    std::less<Object>());
                        }
                        else
                        {
                            std::set_intersection(cmp->begin(), cmp->end(), sets[i]->begin(), sets[i]->end(),
                                    std::inserter(*result, result->end()), std::less<Object>());
                        }

                        if (result->empty())
                        {
                            result_index = current_result_index;
                            goto _end;
                        }
                        break;
                    }
                    case OP_UNION:
                    {
                        if (cmp == NULL)
                        {
                            std::set_union(sets[start_index]->begin(), sets[start_index]->end(), sets[i]->begin(),
                                    sets[i]->end(), std::inserter(*result, result->end()), std::less<Object>());
                        }
                        else
                        {
                            std::set_union(cmp->begin(), cmp->end(), sets[i]->begin(), sets[i]->end(),
                                    std::inserter(*result, result->end()), std::less<Object>());
                        }
                        break;
                    }
                }
                current_result_index = 1 - current_result_index;
                cmp = result;
            }
        }
        result_index = result == results ? 0 : 1;
        _end: if (NULL != res)
        {
            StdObjectSet::iterator it = results[result_index].begin();
            while (it != results[result_index].end())
            {
                std::string str;
                it->ToString(str);
                res->push_back(str);
                it++;
            }
        }
        if (NULL != destset)
        {
            //remove elements not in dest set
            StringSet::iterator it = destset->begin();
            while (it != destset->end())
            {
                Object element = *it;
                StdObjectSet::iterator cit = results[result_index].find(element);
                if (cit != results[result_index].end()) //remove elements from results which already in dest set
                {
                    results[result_index].erase(cit);
                    it++;
                }
                else
                {
                    it = destset->erase(it);
                    DestroyObjectContent(element);
                }
            }

            //insert rest elements
            StdObjectSet::iterator cit = results[result_index].begin();
            while (cit != results[result_index].end())
            {
                Object clone = CloneStrObject(*cit, false);
                destset->insert(clone);
                cit++;
            }
        }
        return 0;
    }

    int MMKVImpl::SDiff(DBID db, const DataArray& keys, StringArray& diffs)
    {
        if (keys.size() < 2)
        {
            return ERR_INVALID_TYPE;
        }
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        return GenericSInterDiffUnion(db, OP_DIFF, keys, NULL, &diffs);
    }
    int MMKVImpl::SDiffStore(DBID db, const Data& destination, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        return GenericSInterDiffUnion(db, OP_DIFF, keys, &destination, NULL);
    }
    int MMKVImpl::SInter(DBID db, const DataArray& keys, StringArray& inters)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        return GenericSInterDiffUnion(db, OP_INTER, keys, NULL, &inters);
    }
    int MMKVImpl::SInterStore(DBID db, const Data& destination, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        return GenericSInterDiffUnion(db, OP_INTER, keys, &destination, NULL);
    }
    int MMKVImpl::SIsMember(DBID db, const Data& key, const Data& member)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (NULL == set || 0 != err)
        {
            return err;
        }
        return set->find(member) != set->end();
    }
    int MMKVImpl::SMembers(DBID db, const Data& key, StringArray& members)
    {
        members.clear();
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (NULL == set || 0 != err)
        {
            return err;
        }
        StringSet::iterator it = set->begin();
        while (it != set->end())
        {
            std::string str;
            it->ToString(str);
            members.push_back(str);
            it++;
        }
        return 0;
    }
    int MMKVImpl::SMove(DBID db, const Data& source, const Data& destination, const Data& member)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringSet* set1 = GetObject<StringSet>(db, source, V_TYPE_SET, false, err)();
        if (NULL == set1 || 0 != err)
        {
            return err;
        }
        ObjectAllocator allocator = m_segment.ValueAllocator<Object>();
        StringSet* set2 = GetObject<StringSet>(db, destination, V_TYPE_SET, true, err)(std::less<Object>(), allocator);
        if (NULL == set2 || 0 != err)
        {
            return err;
        }
        StringSet::iterator found = set1->find(member);
        if (set1 == set2)
        {
            return found != set1->end() ? 1 : 0;
        }
        if (found != set1->end())
        {
            Object val = *found;
            set1->erase(found);
            if (!set2->insert(val).second)
            {
                DestroyObjectContent(val);
            }
            return 1;
        }
        return 0;
    }
    int MMKVImpl::SPop(DBID db, const Data& key, StringArray& members, int count)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        if (count < 0)
        {
            return ERR_INVALID_NUMBER;
        }
        members.clear();
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (0 != err)
        {
            return err;
        }
        if (set->empty())
        {
            return 0;
        }
        for (int i = 0; i < count; i++)
        {
            int rand = random_between_int32(0, set->size() - 1);
            StringSet::iterator it = set->begin();
            it.increment_by(rand);
            std::string str;
            it->ToString(str);
            members.push_back(str);
            Object cc = *it;
            set->erase(it);
            DestroyObjectContent(cc);
        }
        return 0;
    }
    int MMKVImpl::SRandMember(DBID db, const Data& key, StringArray& members, int count)
    {
        int err = 0;
        members.clear();
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (0 != err)
        {
            return err;
        }
        if (set->empty())
        {
            return 0;
        }
        std::set<int> uniq_set;
        for (int i = 0; i < std::abs(count); i++)
        {
            int rand = 0;
            if (count > 0)
            {
                bool has_duplicate_value = false;
                do
                {
                    rand = random_between_int32(0, set->size() - 1);
                    has_duplicate_value = uniq_set.insert(rand).second;
                } while (has_duplicate_value);
            }
            else
            {
                rand = random_between_int32(0, set->size() - 1);
            }
            StringSet::iterator it = set->begin();
            it.increment_by(rand);
            std::string str;
            it->ToString(str);
            members.push_back(str);
        }
        return 0;
    }
    int MMKVImpl::SRem(DBID db, const Data& key, const DataArray& members)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        StringSet* set = GetObject<StringSet>(db, key, V_TYPE_SET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (NULL == set || 0 != err)
        {
            return err;
        }
        int removed = 0;
        for (size_t i = 0; i < members.size(); i++)
        {
            StringSet::iterator found = set->find(members[i]);
            if (found != set->end())
            {
                Object cc = *found;
                set->erase(found);
                DestroyObjectContent(cc);
                removed++;
            }
        }
        return removed;
    }
    int MMKVImpl::SScan(DBID db, const Data& key, int cursor, const std::string& pattern, int32_t limit_count)
    {
        return -1;
    }
    int MMKVImpl::SUnion(DBID db, const DataArray& keys, StringArray& unions)
    {
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        return GenericSInterDiffUnion(db, OP_UNION, keys, NULL, &unions);;
    }
    int MMKVImpl::SUnionStore(DBID db, const Data& destination, const DataArray& keys)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        return GenericSInterDiffUnion(db, OP_UNION, keys, &destination, NULL);
    }
}