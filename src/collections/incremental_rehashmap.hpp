/*
 * hashmap.hpp
 *
 *  Created on: 2015年9月7日
 *      Author: wangqiying
 */

#ifndef SRC_COLLECTIONS_INCREMENTAL_REHASHMAP_HPP_
#define SRC_COLLECTIONS_INCREMENTAL_REHASHMAP_HPP_

#include <stdexcept>
#include <limits>

#include "fixed_hashtable.hpp"
namespace mmkv
{
    template<class V, class K, class HT>
    struct incremental_rehashmap_iterator
    {
        public:
            typedef incremental_rehashmap_iterator<V, K, HT> iterator;

            typedef V value_type;
            typedef typename HT::difference_type difference_type;
            typedef typename HT::size_type size_type;
            typedef typename HT::reference ht_reference;
            typedef typename HT::pointer ht_pointer;
            typedef typename HT::ht ht_table;
            typedef typename HT::ht_iterator ht_iter;
            //typedef typename value_alloc_type::pointer pointer;
            typedef value_type* pointer;

            // "Real" constructor and default constructor
            incremental_rehashmap_iterator(HT *h, ht_iter iter, ht_table* t0,
                    ht_table* t1) :
                    ht(h), rep_it(iter)
            {
                rep[0] = t0;
                rep[1] = t1;
                if (rep_it == rep[0]->end())
                {
                    if (NULL != rep[1])
                    {
                        rep_it = rep[1]->begin();
                    }
                }
            }
            incremental_rehashmap_iterator() :
                    ht(NULL)
            {
            }

            // Happy dereferencer
            ht_reference operator*() const
            {
                return *rep_it;
            }
            pointer operator->() const
            {
                return &(operator*());
            }
            iterator& operator++()
            {
                rep_it++;
                if (rep_it == rep[0]->end())
                {
                    if (NULL != rep[1])
                    {
                        rep_it = rep[1]->begin();
                    }
                }
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
                return rep_it == it.rep_it;
            }
            bool operator!=(const iterator& it) const
            {
                return rep_it != it.rep_it;
            }
            size_t position() const
            {
                if (rep[0]->valid_iterator(rep_it))
                {
                    return rep_it.pos;
                }
                else
                {
                    return rep_it.pos + rep[0]->bucket_count();
                }
            }

        private:
            // The actual data
            HT *ht;
            ht_table* rep[2];
        public:
            ht_iter rep_it;

    };
    template<class V, class K, class HT>
    struct incremental_rehashmap_const_iterator
    {

        public:
            typedef incremental_rehashmap_iterator<V, K, HT> iterator;
            typedef incremental_rehashmap_const_iterator<V, K, HT> const_iterator;
            typedef V value_type;
            typedef typename HT::difference_type difference_type;
            typedef typename HT::size_type size_type;
            typedef typename HT::const_reference ht_const_reference;
            typedef typename HT::pointer ht_pointer;
            typedef typename HT::ht ht_table;
            typedef typename HT::ht_iterator ht_iter;
            //typedef typename value_alloc_type::pointer pointer;
            typedef const value_type* pointer;

            // "Real" constructor and default constructor
            incremental_rehashmap_const_iterator(HT *h, ht_iter iter,
                    ht_table* t0, ht_table* t1) :
                    ht(h), rep_it(iter)
            {
                rep[0] = t0;
                rep[1] = t1;
            }
            incremental_rehashmap_const_iterator() :
                    ht(NULL)
            {
            }

            // Happy dereferencer
            ht_const_reference operator*() const
            {
                return *rep_it;
            }
            pointer operator->() const
            {
                return &(operator*());
            }
            iterator& operator++()
            {
                rep_it++;
                if (rep_it == rep[0]->end())
                {
                    if (NULL != rep[1])
                    {
                        rep_it = rep[1].begin();
                    }
                }
                return *this;
            }
            iterator operator++(int)
            {
                iterator tmp(*this);
                ++*this;
                return tmp;
            }

            void advance(size_t n)
            {
                //if()
            }

            // Comparison.
            bool operator==(const iterator& it) const
            {
                return rep_it == it.rep_it;
            }
            bool operator!=(const iterator& it) const
            {
                return rep_it != it.rep_it;
            }
        private:
            // The actual data
            HT *ht;
            ht_iter rep_it;
            ht_table* rep[2];
    };

    template<class Key, class T, class HashFcn, class EqualKey, class Alloc>
    class incremental_rehashmap
    {
        private:
            // Apparently select1st is not stl-standard, so we define our own
            struct SelectKey
            {
                    typedef const Key& result_type;
                    const Key& operator()(
                            const std::pair<const Key, T>& p) const
                    {
                        return p.first;
                    }
            };
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

        public:
            typedef incremental_rehashmap<Key, T, HashFcn, EqualKey, Alloc> hashmap_type;
            // The actual data
            typedef fixed_hashtable<std::pair<const Key, T>, Key, HashFcn,
                    SelectKey, SetKey, EqualKey, Alloc> ht;
            typedef boost::interprocess::offset_ptr<ht> ht_offset;
            typedef typename ht::iterator ht_iterator;
            typedef typename Alloc::template rebind<ht>::other ht_alloc_type;
            typedef typename ht::key_type key_type;
            typedef T data_type;
            typedef T mapped_type;
            typedef typename ht::value_type value_type;
            typedef typename ht::hasher hasher;
            typedef typename ht::key_equal key_equal;
            typedef Alloc allocator_type;

            typedef typename ht::size_type size_type;
            typedef typename ht::difference_type difference_type;
            typedef typename ht::pointer pointer;
            typedef typename ht::const_pointer const_pointer;
            typedef typename ht::reference reference;
            typedef typename ht::const_reference const_reference;

            typedef incremental_rehashmap_iterator<value_type, Key, hashmap_type> iterator;
            typedef incremental_rehashmap_const_iterator<value_type, Key,
                    hashmap_type> const_iterator;
            // Minimum size we're willing to let hashtables be.
            // Must be a power of two, and at least 4.
            // Note, however, that for a given hashtable, the initial size is a
            // function of the first constructor arg, and may be >HT_MIN_BUCKETS.
            static const size_type HT_MIN_BUCKETS = 4;

            // By default, if you don't specify a hashtable size at
            // construction-time, we use this size.  Must be a power of two, and
            // at least HT_MIN_BUCKETS.
            static const size_type HT_DEFAULT_STARTING_BUCKETS = 32;
        private:
            ht_alloc_type get_ht_allocator() const
            {
                return rep[0]->get_allocator();
            }
            int resize(size_t size)
            {
                if (rehashing() || rep[0]->size() >= size)
                {
                    return -1;
                }

                rep[1] = get_ht_allocator().allocate(1);
                ::new (rep[1].get()) ht(size, get_allocator());
                rehash_iter_pos = rep[0]->begin().pos;
                return 0;
            }

            // This is the smallest size a hashtable can be without being too crowded
            // If you like, you can give a min #buckets as well as a min #elts
            size_type min_buckets(size_type num_elts,
                    size_type min_buckets_wanted)
            {
                size_type sz = HT_MIN_BUCKETS;            // min buckets allowed
                while (sz < min_buckets_wanted
                        || num_elts
                                >= static_cast<size_type>(sz * enlarge_factor_))
                {
                    // This just prevents overflowing size_type, since sz can exceed
                    // max_size() here.
                    if (static_cast<size_type>(sz * 2) < sz)
                    {
                        throw std::length_error("resize overflow"); // protect against overflow
                    }
                    sz *= 2;
                }
                return sz;
            }
            size_type enlarge_size(size_type x) const
            {
                return static_cast<size_type>(x * enlarge_factor_);
            }
            size_type shrink_size(size_type x) const
            {
                return static_cast<size_type>(x * shrink_factor_);
            }

            bool try_expand(size_t delta)
            {
                if (rehashing())
                {
                    return false;
                }

                if (rep[0]->bucket_count() >= HT_MIN_BUCKETS
                        && (rep[0]->nonempty_bucket_count() + delta)
                                <= enlarge_threshold_)
                    return false;                     // we're ok as we are

                // Sometimes, we need to resize just to get rid of all the
                // "deleted" buckets that are clogging up the hashtable.  So when
                // deciding whether to resize, count the deleted buckets (which
                // are currently taking up room).  But later, when we decide what
                // size to resize to, *don't* count deleted buckets, since they
                // get discarded during the resize.
                const size_type needed_size = min_buckets(
                        rep[0]->nonempty_bucket_count() + delta, 0);

                if (needed_size <= rep[0]->bucket_count()) // we have enough buckets
                    return false;

                size_type resize_to = min_buckets(rep[0]->size() + delta,
                        rep[0]->bucket_count());

                if (resize_to < needed_size &&    // may double resize_to
                        resize_to < (std::numeric_limits<size_type>::max)() / 2)
                {
                    // This situation means that we have enough deleted elements,
                    // that once we purge them, we won't actually have needed to
                    // grow.  But we may want to grow anyway: if we just purge one
                    // element, say, we'll have to grow anyway next time we
                    // insert.  Might as well grow now, since we're already going
                    // through the trouble of copying (in order to purge the
                    // deleted elements).
                    const size_type target = static_cast<size_type>(shrink_size(
                            resize_to * 2));
                    if (rep[0]->size() + delta >= target)
                    {
                        // Good, we won't be below the shrink threshhold even if we double.
                        resize_to *= 2;
                    }
                }
                resize(resize_to);

                return true;
            }
            void reset_threshold(size_t size)
            {
                enlarge_threshold_ = static_cast<size_type>(size * enlarge_factor_);
                shrink_threshold_ = static_cast<size_type>(size * shrink_factor_);
            }
        public:

            //typedef fixed_hashtable_const_iterator<value_type, Key, ht_type> const_iterator;

            // Accessor functions
            allocator_type get_allocator() const
            {
                return rep[0]->get_allocator();
            }

            // Constructors
            explicit incremental_rehashmap(const allocator_type& alloc =
                    allocator_type()) :
                    rehash_iter_pos((size_t) -1), enlarge_factor_(0.5), shrink_factor_(
                            0.1), enlarge_threshold_(0), shrink_threshold_(0)
            {
                rep[0] = rep[1] = NULL;
                ht_alloc_type ht_alloc(alloc);
                rep[0] = ht_alloc.allocate(1);
                size_t init_size = HT_DEFAULT_STARTING_BUCKETS;
                ::new (rep[0].get()) ht(init_size, alloc);
                reset_threshold(init_size);
            }
            bool rehashing() const
            {
                return rehash_iter_pos != (size_t) -1;
            }
            void clear()
            {
                if (NULL != rep[0])
                {
                    rep[0]->clear();
                }
                if (NULL != rep[1])
                {
                    rep[1]->clear();
                }
                rehash_iter_pos = (size_t) -1;
            }

            // Functions concerning size
            size_type size() const
            {
                size_type n = rep[0]->size();
                if (rehashing())
                {
                    n += rep[1]->size();
                }
                return n;
            }
            size_type bucket_count() const
            {
                size_type n = rep[0]->bucket_count();
                if (NULL != rep[1])
                {
                    n += rep[1]->bucket_count();
                }
                return n;
            }
            bool empty() const
            {
                if (rep[0]->empty())
                {
                    if (rehashing())
                    {
                        return rep[1]->empty();
                    }
                    return true;
                }
                return false;
            }

            iterator begin()
            {
                return iterator(this, rep[0]->begin(), rep[0].get(),
                        rep[1].get());
            }

            iterator get_iterator(size_t bucket)
            {
                if (bucket < rep[0]->bucket_count() || NULL == rep[1])
                {
                    if (bucket > rep[0]->bucket_count())
                    {
                        bucket = rep[0]->bucket_count();
                    }
                    return iterator(this, rep[0]->get_iterator(bucket),
                            rep[0].get(), rep[1].get());
                }
                bucket -= rep[0]->bucket_count();
                if (bucket > rep[1]->bucket_count())
                {
                    bucket = rep[1]->bucket_count();
                }
                return iterator(this, rep[1]->get_iterator(bucket),
                        rep[0].get(), rep[1].get());
            }

            iterator end()
            {
                if (!rehashing())
                {
                    return iterator(this, rep[0]->end(), rep[0].get(),
                            rep[1].get());
                }
                else
                {
                    return iterator(this, rep[1]->end(), rep[0].get(),
                            rep[1].get());
                }
            }
            const_iterator begin() const
            {
                return const_iterator(this, rep[0]->begin(), rep[0].get(),
                        rep[1].get());
            }

            const_iterator end() const
            {
                if (rehashing())
                {
                    return const_iterator(this, rep[0]->end(), rep[0].get(),
                            rep[1].get());
                }
                else
                {
                    return const_iterator(this, rep[1]->end(), rep[0].get(),
                            rep[1].get());
                }
            }

            // Lookup routines
            iterator find(const key_type& key)
            {
                ht_iterator fit = rep[0]->find(key);
                if (fit == rep[0]->end())
                {
                    if (rehashing())
                    {
                        fit = rep[1]->find(key);
                    }
                }
                return iterator(this, fit, rep[0].get(), rep[1].get());
            }
            const_iterator find(const key_type& key) const
            {
                ht_iterator fit = rep[0]->find(key);
                if (fit == rep[0]->end())
                {
                    if (rehashing())
                    {
                        fit = rep[1]->find(key);
                    }
                }
                return const_iterator(this, fit, rep[0].get(), rep[1].get());
            }

            data_type& operator[](const key_type& key)
            {       // This is our value-add!
                // If key is in the hashtable, returns find(key)->second,
                // otherwise returns insert(value_type(key, T()).first->second.
                // Note it does not create an empty T unless the find fails.
                //todo
            }

            size_type count(const key_type& key) const
            {
                size_type n = rep[0]->count(key);
                if (n == 0 && rehashing())
                {
                    return rep[1]->count(key);
                }
                return n;
            }

            // Insertion routines
            std::pair<iterator, bool> insert(const value_type& obj)
            {
                if (rehashing())
                {
                    incremental_rehash(1);
                }
                if (!rehashing())
                {
                    try_expand(1);
                }
                if (!rehashing())
                {
                    std::pair<ht_iterator, bool> ret = rep[0]->insert_noresize(
                            obj);
                    return std::pair<iterator, bool>(
                            iterator(this, ret.first, rep[0].get(),
                                    rep[1].get()), ret.second);
                }
                else
                {
                    ht_iterator it = rep[0]->find(obj.first);
                    if (it == rep[0]->end())
                    {
                        std::pair<ht_iterator, bool> ret =
                                rep[1]->insert_noresize(obj);
                        return std::pair<iterator, bool>(
                                iterator(this, ret.first, rep[0].get(),
                                        rep[1].get()), ret.second);
                    }
                    else
                    {
                        return std::pair<iterator, bool>(
                                iterator(this, it, rep[0].get(), rep[1].get()),
                                false);
                    }
                }
            }

            // These are standard
            size_type erase(const key_type& key)
            {
                incremental_rehash(1);
                size_type n = rep[0]->erase(key);
                if (n == 0 && rehashing())
                {
                    n = rep[1]->erase(key);
                }
                try_shrink();
                return n;
            }
            void erase(iterator it)
            {
                if (rep[0]->valid_iterator(it.rep_it))
                {
                    rep[0]->erase(it.rep_it);
                }
                else
                {
                    if (rep[1].get() != NULL)
                    {
                        rep[1]->erase(it.rep_it);
                    }
                }
                //incremental_rehash(1);
                //try_shrink();
            }
            void incremental_rehash(size_t count)
            {
                if (!rehashing())
                {
                    return;
                }
                ht* table0 = rep[0].get();
                ht* table1 = rep[1].get();
                size_t rehashed = 0;
                ht_iterator rehash_iter = table0->get_iterator(rehash_iter_pos);
                //printf("####start rehash  %d \n", count);
                rehash_iter.advance_past_empty_and_deleted();
                while (rehash_iter != table0->end() && rehashed < count)
                {
                    //printf("#### rehash  %d \n", rehashed);
                    table1->insert_noresize(*rehash_iter);
                    table0->erase(rehash_iter);
                    rehash_iter++;
                    rehashed++;
                }
                if (rehash_iter == table0->end())
                {
                    rehash_iter_pos = (size_t) -1;
                    rep[0] = table1;
                    table0->~ht();
                    get_ht_allocator().deallocate(table0, 1);
                    rep[1] = NULL;
                    //printf("####rehash to %d finished\n", rep[0]->bucket_count());
                }
                else
                {
                    rehash_iter_pos = rehash_iter.pos;
                }
            }

            float rehash_progress() const
            {
                if (!rehashing())
                {
                    return 0;
                }
                return (rehash_iter_pos * 1.0) / rep[0]->bucket_count();
            }
            bool try_shrink()
            {
                if (rehashing())
                {
                    return false;
                }
                const size_type num_remain = rep[0]->size();
                if (shrink_threshold_ > 0 && num_remain < shrink_threshold_
                        && rep[0]->bucket_count() > HT_DEFAULT_STARTING_BUCKETS)
                {
                    size_type sz = rep[0]->bucket_count() / 2; // find how much we should shrink
                    while (sz > HT_DEFAULT_STARTING_BUCKETS
                            && num_remain < sz * shrink_factor_)
                    {
                        sz /= 2;                            // stay a power of 2
                    }
                    resize(sz);
                    return true;
                }
                return false;
            }
            ~incremental_rehashmap()
            {
                for (size_t i = 0; i < 2; i++)
                {
                    if (NULL != rep[i].get())
                    {
                        rep[i]->~ht();
                        get_ht_allocator().deallocate(rep[i].get(), 1);
                    }
                }
            }
        private:
            ht_offset rep[2];
            size_t rehash_iter_pos;
            float enlarge_factor_;         // how full before resize
            float shrink_factor_;          // how empty before resize
            size_type enlarge_threshold_;  // table.size() * enlarge_factor
            size_type shrink_threshold_;   // table.size() * shrink_factor
    };
}

#endif /* SRC_COLLECTIONS_INCREMENTAL_REHASHMAP_HPP_ */
