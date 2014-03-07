#pragma once

#include "storage/HashTable.h"
#ifdef HYRISE_USE_FOLLY
    #include <folly/AtomicHashMap.h>
    #include <folly/Memory.h>
#else
    #include <tbb/concurrent_unordered_map.h>
#endif

namespace hyrise {
namespace storage {

template <typename R>
class atomic_aggregate_value_t : public std::atomic<R> {
public:
    atomic_aggregate_value_t(const atomic_aggregate_value_t& copyValue) : std::atomic<R>((const R&)copyValue) {
    }

    atomic_aggregate_value_t(R copyValue) : std::atomic<R>(copyValue) {
    }
};

template<class MAP, class KEY> class SharedHashTableBase;
template<class MAP, class KEY> class SharedHashTable;
template<class MAP, class KEY, class AtomicAggregateFun> class AtomicSharedHashTable;

/// folly usage is a prototype for performance measurements yet
#ifdef HYRISE_USE_FOLLY
    // Multi Keys
    typedef folly::AtomicHashMap<hyrise_int_t, hyrise_int_t> shared_aggregate_hash_map_t;
    // Single Keys
    typedef folly::AtomicHashMap<hyrise_int_t, hyrise_int_t> shared_aggregate_single_hash_map_t;
#else
    // Multi Keys
    typedef tbb::concurrent_unordered_multimap<aggregate_key_t, pos_t, GroupKeyHash<aggregate_key_t> > shared_aggregate_hash_map_t;
    // Single Keys
    typedef tbb::concurrent_unordered_multimap<aggregate_single_key_t, pos_t, SingleGroupKeyHash<aggregate_single_key_t> > shared_aggregate_single_hash_map_t;
#endif

typedef SharedHashTableBase<shared_aggregate_hash_map_t, aggregate_key_t> AggregateSharedHashTableBase;
typedef SharedHashTableBase<shared_aggregate_single_hash_map_t, aggregate_single_key_t> SingleAggregateSharedHashTableBase;

class AbstractSharedHashTable {
public:
    virtual void populateMap() = 0;
};

template <class MAP, class KEY>
class SharedHashTableBase : public HashTableBase<std::shared_ptr<MAP>,KEY>, public AbstractSharedHashTable, public std::enable_shared_from_this<SharedHashTableBase<MAP,KEY> > {
public:
    typedef HashTableBase<std::shared_ptr<MAP>,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;
    typedef std::shared_ptr<MAP> map_ptr_t;
    typedef typename map_t::const_iterator map_const_iterator_t;

    SharedHashTableBase(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, unsigned row_offset = 0)
        : SharedHashTableBase(t, f, nullptr, row_offset) {
    }

    SharedHashTableBase(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, map_ptr_t map_ptr)
        : base_t(t, f) {
        setMap(map_ptr);
    }

    /// Get const interators to underlying map's begin or end.
    virtual map_const_iterator_t getMapBegin() const {
        return base_t::_map->begin();
    }
    virtual map_const_iterator_t getMapEnd() const {
        return base_t::_map->end();
    }

    void setMap(map_ptr_t map_ptr) {
        base_t::_map = map_ptr;
    }

    virtual size_t size() const {
        return base_t::_map->size();
      }


    std::shared_ptr<HashTableView<MAP, KEY, SharedHashTableBase> > view(size_t first, size_t last) const {
        #ifdef HYRISE_USE_FOLLY
            return nullptr;
        #else
            return std::make_shared<HashTableView<MAP, KEY, SharedHashTableBase> >(this->shared_from_this(), first, last);
        #endif
    }


    virtual uint64_t numKeys() const {
        if (base_t::_dirty) {
          uint64_t result = 0;
          for (map_const_iterator_t it1 = base_t::_map->begin(), it2 = it1, end = base_t::_map->end(); it1 != end; it1 = it2) {
            for (; (it2 != end) && (it1->first == it2->first); ++it2) {}
            ++result;
          }

          base_t::_numKeys = result;
          base_t::_dirty = false;
        }
        return base_t::_numKeys;
      }
protected:
    map_ptr_t getSharedMap() {
        // lazy initialization
        /*if(!base_t::_map) {
            base_t::_map = std::make_shared<map_t>();
        }*/
        return base_t::_map;
    }
};

// container must support concurrent insertion
template <class MAP, class KEY>
class SharedHashTable : public SharedHashTableBase<MAP, KEY> {
public:
    typedef SharedHashTableBase<MAP,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;
    typedef std::shared_ptr<MAP> map_ptr_t;
    typedef typename map_t::const_iterator map_const_iterator_t;
    typedef decltype(std::declval<const MAP>().equal_range(key_t())) map_const_range_t;

    SharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, map_ptr_t map_ptr, unsigned row_offset = 0)
        : base_t(t, f, map_ptr) {
        setRowOffset(row_offset);
    }

    virtual void populateMap() {
        base_t::_dirty = true;
        size_t fieldSize = base_t::_fields.size();
        size_t tableSize = base_t::_table->size();
        typename base_t::map_ptr_t map = base_t::getSharedMap();
        for (pos_t row = 0; row < tableSize; ++row) {
            key_t key = MAP::hasher::getGroupKey(base_t::_table, base_t::_fields, fieldSize, row);
            map->insert(typename map_t::value_type(key, row + _rowOffset));
        }
    }

    /// Get positions for values in the table cells of given row and columns.
    virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t& table,
                           const field_list_t &columns,
                           const pos_t row) const {
        key_t key = MAP::hasher::getGroupKey(table, columns, columns.size(), row);
        auto range = base_t::_map->equal_range(key);
        return constructPositions(range);
      }

    virtual pos_list_t get(const key_t &key) const {
      auto range = base_t::_map->equal_range(key);
      return constructPositions(range);
    }

    void setRowOffset(size_t rowOffset) {
        _rowOffset = rowOffset;
    }

protected:
    size_t _rowOffset;

private:
    pos_list_t constructPositions(const map_const_range_t &range) const {
      return constructPositions(range.first, range.second);
    }

    pos_list_t constructPositions(const map_const_iterator_t &begin,  const map_const_iterator_t &end) const {
      pos_list_t positions(std::distance(begin, end));
      // decltype(*range.first) returns the type of iterator elements
      std::transform(begin, end, positions.begin(), [&](decltype(*begin)& value) {
        return value.second;
      });
      return positions;
    }
};

// container must support concurrent insertion & value type has to be a member of std::atomic
// currently only supports commutative aggregation on integer values
template <class MAP, class KEY, class AtomicAggregateFun>
class AtomicSharedHashTable : public SharedHashTableBase<MAP, KEY> {
public:
    typedef SharedHashTableBase<MAP,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;
    typedef std::shared_ptr<MAP> map_ptr_t;

    AtomicSharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, map_ptr_t map_ptr, field_t aggrFuncField)
        : base_t(t, f, map_ptr) {
        _aggrFuncFields.push_back(aggrFuncField);
    }

    virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t& table,
                           const field_list_t &columns,
                           const pos_t row) const {
        return pos_list_t();
      }

    virtual pos_list_t get(const key_t &key) const {
        return pos_list_t();
    }

    virtual void populateMap() {
        base_t::_dirty = true;
        size_t fieldSize = base_t::_fields.size();
        size_t tableSize = base_t::_table->size();
        typename base_t::map_ptr_t map = base_t::getSharedMap();
        for (pos_t row = 0; row < tableSize; ++row) {
            //key_t key = MAP::hasher::getGroupKey(base_t::_table, base_t::_fields.front(), fieldSize, row);
            //value_id_t aggregateFuncValue = SingleGroupKeyHash<aggregate_single_key_t>::getGroupKey(base_t::_table, _aggrFuncFields, fieldSize, row);
            //auto elementPair = map->insert(typename map_t::value_type(key, atomic_aggregate_value_t<value_id_t>(AtomicAggregateFun::firstValue(aggregateFuncValue))));
            auto elementPair = map->insert(std::make_pair(SingleGroupKeyHash<aggregate_single_key_t>::getGroupKey(base_t::_table, base_t::_fields, fieldSize, row), 0));
            // if key was already present we have to update the value instead
            if(elementPair.second == false) {
                __sync_fetch_and_add(&elementPair.first->second, 1);
                //AtomicAggregateFun::update(elementPair.first->second, aggregateFuncValue);
            }
        }
    }

protected:
    field_list_t _aggrFuncFields;
};

// Atomic Aggregation Functions
// must implement:
// R    operator()(const R& value)
// void operator()(std::atomic<R>& containerRef, const R& value)
// -----------------------------

struct AtomicCountAggregationFunc {
    template <typename R>
    static R firstValue(const R& value) {
        return 1;
    }

    template <typename R>
    static void update(std::atomic<R>& containerRef, const R& value) {
        containerRef++;
    }
};

struct AtomicSumAggregationFunc {
    template <typename R>
    static R firstValue(const R& value) {
        return value;
    }

    template <typename R>
    static void update(std::atomic<R>& containerRef, const R& value) {
        containerRef += value;
    }
};

struct AtomicMaxAggregationFunc {
    template <typename R>
    static R firstValue(const R& value) {
        return value;
    }

    template <typename R>
    static void update(std::atomic<R>& containerRef, const R& value) {
        R prev_value = containerRef;
        // whole max operation is not atomic, but compare_exchange is which is sufficient
        while(prev_value < value && !containerRef.compare_exchange_weak(prev_value, value)) ;
    }
};

struct AtomicMinAggregationFunc {
    template <typename R>
    static R firstValue(const R& value) {
        return value;
    }

    template <typename R>
    static void update(std::atomic<R>& containerRef, const R& value) {
        R prev_value = containerRef;
        // see AtomicMaxAggregationFunc
        while(prev_value > value && !containerRef.compare_exchange_weak(prev_value, value)) ;
    }
};

} } // namespace hyrise::storage
