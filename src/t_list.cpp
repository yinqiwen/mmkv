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
namespace mmkv
{
    int MMKVImpl::LIndex(DBID db, const Data& key, int index, std::string& val)
    {
        val.clear();
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL == list || 0 != err)
        {
            return err;
        }
        if (index < 0)
        {
            index = list->size() + index;
        }
        if (index < 0 || (size_t) index >= list->size())
        {
            return ERR_OFFSET_OUTRANGE;
        }
        Object& ptr = list->at(index);
        ptr.ToString(val);
        return 0;
    }
    int MMKVImpl::LInsert(DBID db, const Data& key, bool before_ot_after, const Data& pivot, const Data& val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL == list)
        {
            if (IS_NOT_EXISTS(err))
            {
                return 0;
            }
            return err;
        }
        bool found = false;
        Object pivot_str(pivot, true);
        StringList::iterator it = list->begin();
        while (it != list->end())
        {
            const Object& data = *it;
            if (data == pivot_str)
            {
                found = true;
                if (!before_ot_after)
                {
                    it++;
                }
                Object& inserted = *(list->insert(it, Object()));
                m_segment.AssignObjectValue(inserted, val, true);
                break;
            }
            it++;
        }
        return found ? (int) list->size() : -1;
    }
    int MMKVImpl::LLen(DBID db, const Data& key)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL != list)
        {
            return list->size();
        }
        return IS_NOT_EXISTS(err) ? 0 : err;
    }
    int MMKVImpl::LPop(DBID db, const Data& key, std::string& val)
    {
        val.clear();
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (0 != err)
        {
            return err;
        }
        if (list->empty())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object& str = list->front();
        str.ToString(val);
        DestroyObjectContent(str);
        list->pop_front();
        if (list->empty())
        {
            GenericDel(GetMMKVTable(db, false), db, Object(key, false));
        }
        return 0;

    }
    int MMKVImpl::LPush(DBID db, const Data& key, const DataArray& vals, bool nx)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;

        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        ObjectAllocator alloc = m_segment.MSpaceAllocator<Object>();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, nx ? false : true, err)(alloc);
        if (0 != err)
        {
            if (nx && IS_NOT_EXISTS(err))
            {
                return 0;
            }
            return err;
        }
        for (size_t i = 0; i < vals.size(); i++)
        {
            list->push_front(Object());
            m_segment.AssignObjectValue(list->at(0), vals[i], true);
        }
        return list->size();
    }
    int MMKVImpl::LRange(DBID db, const Data& key, int start, int end, const StringArrayResult& vals)
    {
        int err = 0;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL == list || list->empty())
        {
            if (IS_NOT_EXISTS(err))
            {
                return 0;
            }
            return err;
        }
        int llen = list->size();
        if (start < 0)
            start = llen + start;
        if (end < 0)
            end = llen + end;
        if (start < 0)
            start = 0;
        if (end < 0)
        {
            return ERR_OFFSET_OUTRANGE;
        }
        for (int i = start; i <= end && (size_t) i < list->size(); i++)
        {
            list->at(i).ToString(vals.Get());
        }
        return 0;
    }
    int MMKVImpl::LRem(DBID db, const Data& key, int count, const Data& val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL == list)
        {
            return err;
        }
        Object val_obj(val, true);
        int actual_removed = 0;
        if(count == 0)
        {
            count = list->size();
        }
        if (count >= 0)
        {
            StringList::iterator it = list->begin();
            while (it != list->end() && count > 0)
            {
                const Object& data = *it;
                if (data == val_obj)
                {
                    DestroyObjectContent(data);
                    it = list->erase(it);
                    count--;
                    actual_removed++;
                }
                else
                {
                    it++;
                }
            }
        }
        else
        {
            StringList::reverse_iterator it = list->rbegin();
            while (it != list->rend() && count < 0)
            {
                const Object& data = *it;
                if (data == val_obj)
                {
                    DestroyObjectContent(data);
                    it = StringList::reverse_iterator(list->erase(--it.base()));
                    count++;
                    actual_removed++;
                }
                else
                {
                    it++;
                }
            }
        }
        if (list->empty())
        {
            GenericDel(GetMMKVTable(db, false), db, Object(key, false));
        }
        return actual_removed;
    }
    int MMKVImpl::LSet(DBID db, const Data& key, int index, const Data& val)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        //EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (NULL == list || 0 != err)
        {
            return err;
        }
        if (index < 0)
        {
            index = list->size() + index;
        }
        if (index < 0 || (size_t) index >= list->size())
        {
            return ERR_OFFSET_OUTRANGE;
        }
        Object& data = list->at(index);
        DestroyObjectContent(data);
        m_segment.AssignObjectValue(data, val, true);
        return 0;
    }
    int MMKVImpl::LTrim(DBID db, const Data& key, int start, int end)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (0 != err)
        {
            return err;
        }
        int llen = list->size();
        /* convert negative indexes */
        if (start < 0)
            start = llen + start;
        if (end < 0)
            end = llen + end;
        if (start < 0)
            start = 0;
        if (end < 0)
        {
            return ERR_OFFSET_OUTRANGE;
        }

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen)
        {
            /* Out of range start or start > end result in empty list */
            GenericDel(GetMMKVTable(db, false), db, Object(key, true));
        }
        else
        {
            if (end >= llen)
                end = llen - 1;
        }
        int front_pop_count = 0, back_pop_count = 0;
        while (front_pop_count < start && !list->empty())
        {
            Object& data = list->front();
            DestroyObjectContent(data);
            list->pop_front();
            front_pop_count++;
        }
        while (back_pop_count < (llen - 1 - end) && !list->empty())
        {
            Object& data = list->back();
            DestroyObjectContent(data);
            list->pop_back();
            back_pop_count++;
        }
        if (list->empty())
        {
            GenericDel(GetMMKVTable(db, false), db, Object(key, false));
        }
        return 0;
    }

    int MMKVImpl::RPop(DBID db, const Data& key, std::string& val)
    {
        val.clear();
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, false, err)();
        if (0 != err)
        {
            return err;
        }
        if (list->empty())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Object& data = list->back();
        data.ToString(val);
        DestroyObjectContent(data);
        list->pop_back();
        if (list->empty())
        {
            GenericDel(GetMMKVTable(db, false), db, Object(key, false));
        }
        return 0;
    }
    int MMKVImpl::RPopLPush(DBID db, const Data& source, const Data& destination, std::string& pop_value)
    {
        pop_value.clear();
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }

        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        ObjectAllocator alloc = m_segment.MSpaceAllocator<Object>();
        StringList* src_list = GetObject<StringList>(db, source, V_TYPE_LIST, false, err)();
        if (0 != err)
        {
            return err;
        }
        StringList* dst_list = GetObject<StringList>(db, destination, V_TYPE_LIST, true, err)(alloc);
        if (0 != err)
        {
            return err;
        }
        if (src_list->empty())
        {
            return ERR_ENTRY_NOT_EXIST;
        }

        Object& data = src_list->back();
        data.ToString(pop_value);
        dst_list->push_front(data);
        src_list->pop_back();
        if (src_list->empty())
        {
            GenericDel(GetMMKVTable(db, false), db, Object(source, false));
        }
        return 0;
    }
    int MMKVImpl::RPush(DBID db, const Data& key, const DataArray& vals, bool nx)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }

        int err = 0;
        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        ObjectAllocator alloc = m_segment.MSpaceAllocator<Object>();
        StringList* list = GetObject<StringList>(db, key, V_TYPE_LIST, nx ? false : true, err)(alloc);
        if (0 != err)
        {
            if (nx && IS_NOT_EXISTS(err))
            {
                return 0;
            }
            return err;
        }
        size_t old_size = list->size();
        list->resize(list->size() + vals.size());
        for (size_t i = 0; i < vals.size(); i++)
        {
            Object& data = list->at(old_size + i);
            data.Clear();
            m_segment.AssignObjectValue(data, vals[i], true);
        }
        return list->size();
    }
}

