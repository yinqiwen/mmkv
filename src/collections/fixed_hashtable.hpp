/*
 * hashmap.hpp
 *
 *  Created on: 2015年8月21日
 *      Author: wangqiying
 */

#ifndef COLLECTIONS_HASHMAP_HPP_
#define COLLECTIONS_HASHMAP_HPP_

#include <boost/interprocess/offset_ptr.hpp>
#include <assert.h>
#include <string.h>

// The probing method
// Linear probing
// #define JUMP_(key, num_probes)    ( 1 )
// Quadratic probing
#define JUMP_(key, num_probes)    ( num_probes )
namespace mmkv
{
    template<class V, class K, class HT>
    struct fixed_hashtable_iterator
    {
        public:
            typedef fixed_hashtable_iterator<V, K, HT> iterator;
            //typedef dense_hashtable_const_iterator<V,K,HF,ExK,SetK,EqK,A> const_iterator;

            typedef std::forward_iterator_tag iterator_category; // very little defined!
            typedef V value_type;
            typedef typename HT::difference_type difference_type;
            typedef typename HT::size_type size_type;
            typedef typename HT::reference ht_reference;
            typedef typename HT::pointer ht_pointer;
            typedef typename HT::internal_values_pointer ht_internal_values;
            //typedef typename value_alloc_type::pointer pointer;
            typedef value_type* pointer;

            // "Real" constructor and default constructor
            fixed_hashtable_iterator(const HT *h, size_type ht_pos,
                    ht_internal_values* ht_vals, bool advance) :
                    ht(h), pos(ht_pos), vals(ht_vals)
            {
                if (advance)
                    advance_past_empty_and_deleted();
            }
            fixed_hashtable_iterator() :
                    ht(NULL), pos(0), vals(NULL)
            {
            }
            // The default destructor is fine; we don't define one
            // The default operator= is fine; we don't define one

            // Happy dereferencer
            ht_reference operator*() const
            {
                return *((*vals)[pos]);
            }
            ht_pointer operator->() const
            {
                return &(operator*());
            }

            // Arithmetic.  The only hard part is making sure that
            // we're not on an empty or marked-deleted array element
            void advance_past_empty_and_deleted()
            {
                while (pos != ht->bucket_count()
                        && (ht->test_empty(*this) || ht->test_deleted(*this)))
                    ++pos;
            }
            void advance(size_t n)
            {
                if ((pos + n) < ht->bucket_count())
                {
                    pos += n;
                    if (ht->test_empty(*this) || ht->test_deleted(*this))
                    {
                        advance_past_empty_and_deleted();
                    }
                }
                else
                {
                    pos = ht->bucket_count();
                }
            }
            size_t rest_step() const
            {
                return ht->bucket_count() - pos;
            }
            iterator& operator++()
            {
                assert(pos != ht->bucket_count());
                ++pos;
                advance_past_empty_and_deleted();
                return *this;
            }
            iterator operator++(int)
            {
                iterator tmp(*this);
                ++*this;
                return tmp;
            }

            // Comparison.
            bool operator==(const iterator& it) const
            {
                return pos == it.pos && ht == it.ht;
            }
            bool operator!=(const iterator& it) const
            {
                return pos != it.pos || ht != it.ht;
            }
            size_type position() const
            {
                return pos;
            }
            // The actual data
            const HT *ht;
            size_type pos;
            ht_internal_values* vals;
    };

    // Now do it all again, but with const-ness!
    template<class V, class K, class HT>
    struct fixed_hashtable_const_iterator
    {
        public:
            typedef fixed_hashtable_iterator<V, K, HT> iterator;
            typedef fixed_hashtable_const_iterator<V, K, HT> const_iterator;

            typedef std::forward_iterator_tag iterator_category; // very little defined!
            typedef V value_type;
            typedef typename HT::difference_type difference_type;
            typedef typename HT::size_type size_type;
            typedef typename HT::const_reference ht_reference;
            typedef typename HT::pointer ht_pointer;
            //typedef typename value_alloc_type::const_pointer pointer;
            typedef const value_type* pointer;
            typedef typename HT::internal_values_pointer ht_internal_values;
            //typedef typename value_alloc_type::pointer pointer;

            // "Real" constructor and default constructor
            fixed_hashtable_const_iterator(const HT *h, size_type ht_pos,
                    ht_internal_values* ht_vals, bool advance) :
                    ht(h), pos(ht_pos), vals(ht_vals)
            {
                if (advance)
                    advance_past_empty_and_deleted();
            }
            fixed_hashtable_const_iterator() :
                    ht(NULL), pos(0), vals(NULL)
            {
            }
            // The default destructor is fine; we don't define one
            // The default operator= is fine; we don't define one

            // Happy dereferencer
            ht_reference operator*() const
            {
                return *((*vals)[pos]);
            }
            pointer operator->() const
            {
                return &(operator*());
            }

            // Arithmetic.  The only hard part is making sure that
            // we're not on an empty or marked-deleted array element
            void advance_past_empty_and_deleted()
            {
                while (pos != ht->bucket_count()
                        && (ht->test_empty(*this) || ht->test_deleted(*this)))
                    ++pos;
            }
            void advance(size_t n)
            {
                if ((pos + n) < ht->bucket_count())
                {
                    pos += n;
                    if (ht->test_empty(*this) || ht->test_deleted(*this))
                    {
                        advance_past_empty_and_deleted();
                    }
                }
                else
                {
                    pos = ht->bucket_count();
                }
            }
            const_iterator& operator++()
            {
                assert(pos != ht->bucket_count());
                ++pos;
                advance_past_empty_and_deleted();
                return *this;
            }
            const_iterator operator++(int)
            {
                iterator tmp(*this);
                ++*this;
                return tmp;
            }

            // Comparison.
            bool operator==(const iterator& it) const
            {
                return pos == it.pos;
            }
            bool operator!=(const iterator& it) const
            {
                return pos != it.pos;
            }
            size_type position() const
            {
                return pos;
            }
            // The actual data
            const HT *ht;
            size_type pos;
            ht_internal_values* vals;
    };

    template<class Value, class Key, class HashFcn, class ExtractKey,
            class SetKey, class EqualKey, class Alloc>
    class fixed_hashtable
    {
        public:
            typedef typename Alloc::template rebind<Value>::other value_alloc_type;
            typedef fixed_hashtable<Value, Key, HashFcn, ExtractKey, SetKey,
                    EqualKey, Alloc> ht_type;
            typedef Key key_type;
            typedef Value value_type;
            typedef HashFcn hasher;
            typedef EqualKey key_equal;
            typedef Alloc allocator_type;

            typedef typename value_alloc_type::size_type size_type;
            typedef typename value_alloc_type::difference_type difference_type;
            typedef typename value_alloc_type::reference reference;
            typedef typename value_alloc_type::const_reference const_reference;
            typedef boost::interprocess::offset_ptr<Value> offset_pointer;
            typedef boost::interprocess::offset_ptr<offset_pointer> internal_values_pointer;
            //typedef typename value_alloc_type::pointer pointer;
            typedef value_type* pointer;
            typedef const value_type* const_pointer;
        private:
            typedef typename Alloc::template rebind<offset_pointer>::other internal_value_alloc_type;
            typedef typename Alloc::template rebind<char>::other flags_value_alloc_type;

            typedef boost::interprocess::offset_ptr<char> flags_offset_pointer;

            static const size_type FLAG_DELETED = 2;
            static const size_type FLAG_SETTED = 1;
            static const size_type FLAG_EMPTY = 0;
        public:
            typedef fixed_hashtable_iterator<value_type, Key, ht_type> iterator;
            typedef fixed_hashtable_const_iterator<value_type, Key, ht_type> const_iterator;
            static const size_type ILLEGAL_BUCKET = size_type(-1);
        private:
            size_t num_elements;
            size_t num_buckets;
            size_t num_deleted;
            internal_values_pointer table;
            flags_offset_pointer flags;

            value_alloc_type allocator;

            // Packages ExtractKey and SetKey functors.
            class KeyInfo: public ExtractKey, public SetKey, public EqualKey
            {
                public:
                    KeyInfo()
                    {
                    }
                    // We want to return the exact same type as ExtractKey: Key or const Key&
                    typename ExtractKey::result_type get_key(
                            const_reference v) const
                    {
                        return ExtractKey::operator()(v);
                    }
                    void set_key(pointer v, const key_type& k) const
                    {
                        SetKey::operator()(v, k);
                    }
                    bool equals(const key_type& a, const key_type& b) const
                    {
                        return EqualKey::operator()(a, b);
                    }
            };
            KeyInfo key_info;

            size_type hash(const key_type& v) const
            {
                hasher h;
                return h(v);
            }
            bool equals(const key_type& a, const key_type& b) const
            {
                key_equal eq;
                return eq(a, b);
            }
            uint8_t get_flag(size_type bucknum) const
            {
                size_t bitoffset = bucknum << 1;
                size_t byte = bitoffset >> 3;
                uint8_t byteval = (uint8_t) flags[byte];
                uint8_t bit = (bitoffset & 0x7);
                return (byteval >> bit) & 0x3;
            }
            void set_flag(size_type bucknum, uint8_t v) const
            {
                size_t bitoffset = bucknum << 1;
                size_t byte = bitoffset >> 3;
                uint8_t* ss = (uint8_t*) (flags.get());
                uint8_t byteval = ss[byte];
                uint8_t bit = (bitoffset & 0x7);
                byteval = ((0xFF - (0x3 << bit)) & byteval) | (v << bit);
                //byteval = (((byteval >> bit) & v) << bit) | (byteval & (0xFF >> (8 - bit)));
                ss[byte] = byteval;
            }
            void set_occupied(size_type pos)
            {
                set_flag(pos, FLAG_SETTED);
            }
            void set_deleted(size_type pos)
            {
                set_flag(pos, FLAG_DELETED);
            }
            void set_empty(size_type pos)
            {
                set_flag(pos, FLAG_EMPTY);
            }
            bool test_setted(size_type bucknum) const
            {
                return get_flag(bucknum) == FLAG_SETTED;
            }
            bool test_deleted(size_type bucknum) const
            {
                if (num_deleted > 0)
                {
                    return get_flag(bucknum) & FLAG_DELETED;
                }
                return false;
            }
            bool test_empty(size_type bucknum) const
            {
                return get_flag(bucknum) == FLAG_EMPTY;
            }

            // Returns a pair of positions: 1st where the object is, 2nd where
            // it would go if you wanted to insert it.  1st is ILLEGAL_BUCKET
            // if object is not found; 2nd is ILLEGAL_BUCKET if it is.
            // Note: because of deletions where-to-insert is not trivial: it's the
            // first deleted bucket we see, as long as we don't find the key later
            std::pair<size_type, size_type> find_position(
                    const key_type &key) const
            {
                size_type num_probes = 0;         // how many times we've probed
                const size_type bucket_count_minus_one = bucket_count() - 1;
                size_type bucknum = hash(key) & bucket_count_minus_one;
                size_type insert_pos = ILLEGAL_BUCKET; // where we would insert
                while (1)
                {                          // probe until something happens
                    if (test_empty(bucknum))
                    {         // bucket is empty
                        if (insert_pos == ILLEGAL_BUCKET) // found no prior place to insert
                            return std::pair<size_type, size_type>(
                                    ILLEGAL_BUCKET, bucknum);
                        else
                            return std::pair<size_type, size_type>(
                                    ILLEGAL_BUCKET, insert_pos);
                    }
                    else if (test_deleted(bucknum))
                    {   // keep searching, but mark to insert
                        if (insert_pos == ILLEGAL_BUCKET)
                            insert_pos = bucknum;

                    }
                    else if (equals(key, get_key(*(table[bucknum]))))
                    {
                        return std::pair<size_type, size_type>(bucknum,
                                ILLEGAL_BUCKET);
                    }
                    ++num_probes;                   // we're doing another probe
                    bucknum = (bucknum + JUMP_(key, num_probes))
                            & bucket_count_minus_one;
                    assert(
                            num_probes < bucket_count()
                                    && "Hashtable is full: an error in key_equal<> or hash<>");
                }
            }
            typename ExtractKey::result_type get_key(const_reference v) const
            {
                return key_info.get_key(v);
            }
            // Annoyingly, we can't copy values around, because they might have
            // const components (they're probably pair<const X, Y>).  We use
            // explicit destructor invocation and placement new to get around
            // this.  Arg.
            void set_value(size_t bucket_pos, const_reference src)
            {
                value_type* v = NULL;
                if (test_setted(bucket_pos))
                {
                    v = table[bucket_pos].get();
                    v->~value_type();
                }
                else
                {
                    v = allocator.allocate(1);
                    table[bucket_pos] = v;
                }
                new (v) value_type(src);
                set_occupied(bucket_pos);
            }

            void destroy_buckets(size_type first, size_type last)
            {
                for (; first != last; ++first)
                {
                    if (test_setted(first))
                    {
                        allocator.destroy(table[first].get());
                    }
                    set_empty(first);
                }
            }
            // Private method used by insert_noresize and find_or_insert.
            iterator insert_at(const_reference obj, size_type pos)
            {
                if (test_deleted(pos))
                {      // just replace if it's been del.
                       // shrug: shouldn't need to be const.
                       //clear_deleted(pos);
                    assert(num_deleted > 0);
                    --num_deleted;                // used to be, now it isn't
                }
                else
                {
                    ++num_elements;               // replacing an empty bucket
                }
                set_value(pos, obj);
                return iterator(this, pos, &table, false);
            }
            bool set_deleted(iterator &it)
            {
                if (!test_setted(it.position()))
                {
                    return false;
                }
                set_deleted(it.position());
                value_type* val = table[it.position()].get();
                val->~value_type();
                allocator.deallocate(val, 1);
                return true;
            }
        public:
            fixed_hashtable(size_t init_capacity, const Alloc& alloc = Alloc()) :
                    num_elements(0), num_buckets(0), num_deleted(0), allocator(
                            alloc)
            {
                internal_value_alloc_type internal_allocator(allocator);
                flags_value_alloc_type flags_allocator(allocator);
                offset_pointer* vals = internal_allocator.allocate(
                        init_capacity + 1);
                table = vals;
                size_t flags_size = (init_capacity / 4) + 1;
                flags = flags_allocator.allocate(flags_size);
                memset(flags.get(), 0, flags_size);
                num_buckets = init_capacity;
            }
            static size_t estimate_memory_size(size_t capacity)
            {
                size_t n = sizeof(offset_pointer) * (capacity + 1);
                size_t flags_size = (capacity / 4) + 1;
                return n + flags_size;
            }
            value_alloc_type get_allocator() const
            {
                return allocator;
            }

            iterator get_iterator(size_t bucket)
            {
                return iterator(this,
                        bucket >= num_buckets ? num_buckets : bucket, &table,
                        true);
            }

            iterator begin()
            {
                return iterator(this, 0, &table, true);
            }
            iterator end()
            {
                return iterator(this, num_buckets, &table, true);
            }
            const_iterator begin() const
            {
                return const_iterator(this, 0, &table, true);
            }
            const_iterator end() const
            {
                return const_iterator(this, num_buckets, &table.get(), true);
            }
            size_type size() const
            {
                return num_elements - num_deleted;
            }

            bool empty() const
            {
                return size() == 0;
            }
            size_type bucket_count() const
            {
                return num_buckets;
            }
            size_type nonempty_bucket_count() const
            {
                return num_elements;
            }
            // If you know *this is big enough to hold obj, use this routine
            std::pair<iterator, bool> insert_noresize(const_reference obj)
            {
                const std::pair<size_type, size_type> pos = find_position(
                        get_key(obj));
                if (pos.first != ILLEGAL_BUCKET)
                {      // object was already there
                    return std::pair<iterator, bool>(
                            iterator(this, pos.first, &table, false), false); // false: we didn't insert
                }
                else
                {                             // pos.second says where to put it
                    return std::pair<iterator, bool>(insert_at(obj, pos.second),
                            true);
                }
            }
            iterator find(const key_type& key)
            {
                if (size() == 0)
                    return end();
                std::pair<size_type, size_type> pos = find_position(key);
                if (pos.first == ILLEGAL_BUCKET)     // alas, not there
                    return end();
                else
                    return iterator(this, pos.first, &table, false);
            }
            // DELETION ROUTINES
            size_type erase(const key_type& key)
            {
                iterator pos = find(key);   // shrug: shouldn't need to be const
                if (pos != end())
                {
                    assert(!test_deleted(pos)); // or find() shouldn't have returned it
                    set_deleted(pos);
                    ++num_deleted;
                    return 1;                    // because we deleted one thing
                }
                else
                {
                    return 0;                    // because we deleted nothing
                }
            }
            bool valid_iterator(iterator it)
            {
                return it.ht == this;
            }
            // We return the iterator past the deleted item.
            void erase(iterator pos)
            {
                if (pos == end())
                    return;    // sanity check
                if (!valid_iterator(pos))
                {
                    return;
                }
                if (set_deleted(pos))
                {      // true if object has been newly deleted
                    ++num_deleted;
                }
            }
            size_type count(const key_type &key) const
            {
                std::pair<size_type, size_type> pos = find_position(key);
                return pos.first == ILLEGAL_BUCKET ? 0 : 1;
            }
            void clear()
            {
                if (num_elements > 0)
                {
                    assert(table);
                    destroy_buckets(0, num_buckets);
                }
                num_elements = 0;
                num_deleted = 0;
            }
            bool test_deleted(iterator& iter) const
            {
                return test_deleted(iter.position());
            }
            bool test_empty(iterator& iter) const
            {
                return test_empty(iter.position());
            }
            bool test_deleted(const_iterator& iter) const
            {
                return test_deleted(iter.position());
            }
            bool test_empty(const_iterator& iter) const
            {
                return test_empty(iter.position());
            }

            ~fixed_hashtable()
            {
                clear();
                internal_value_alloc_type internal_allocator(allocator);
                internal_allocator.deallocate(table.get(), 1);
                flags_value_alloc_type flags_allocator(allocator);
                flags_allocator.deallocate(flags.get(), 1);
            }
    };
    template<class V, class K, class HF, class ExK, class SetK, class EqK,
            class A>
    const typename fixed_hashtable<V, K, HF, ExK, SetK, EqK, A>::size_type fixed_hashtable<
            V, K, HF, ExK, SetK, EqK, A>::ILLEGAL_BUCKET;
}

#endif /* SRC_COLLECTIONS_HASHMAP_HPP_ */
