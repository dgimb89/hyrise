#pragma once

#include "storage/HashTable.h"
#include "helper/types.h"
#include <map>
#include <unordered_map>


namespace hyrise {
namespace storage {

// forward declaration
template <class KEY, class MAPPED, class HASHER, class AGGR_FUN>
class AggregateHashMap;

// Single Key Hash
template <typename VALUE_TYPE, typename AGGR_FUN>
using SingleKeyAggregateHashMap = AggregateHashMap<aggregate_single_key_t, VALUE_TYPE, SingleGroupKeyHash<aggregate_single_key_t>, AGGR_FUN>;

// Multiple Key Hash
template <typename VALUE_TYPE, typename AGGR_FUN>
using MultiKeyAggregateHashMap = AggregateHashMap<aggregate_key_t, VALUE_TYPE, GroupKeyHash<aggregate_key_t>, AGGR_FUN>;

class AbstractAggregateHashMap {
public:
    virtual void mergeWith(hyrise::storage::c_ahashtable_ptr_t aggregateHashMap) = 0;
};

template <class KEY, class MAPPED, class HASHER, class AGGR_FUN>
class AggregateHashMap :
        public HashTableBase<std::unordered_map<KEY, MAPPED, HASHER>, KEY>,
        public std::enable_shared_from_this<AggregateHashMap<KEY,MAPPED,HASHER,AGGR_FUN> >,
        public AbstractAggregateHashMap{

public:
    typedef std::unordered_map<KEY, MAPPED, HASHER> map_t;
    typedef MAPPED mapped_t;
    typedef KEY key_t;
    typedef HashTableBase<map_t,key_t> base_t;
    typedef std::map<pos_t, key_t> posMap_t;
    typedef typename posMap_t::const_iterator posmap_const_iterator_t;

    AggregateHashMap(hyrise::storage::c_ahashtable_ptr_t aggregateHashMap) {
        auto copyHashMap = checked_pointer_cast<const AggregateHashMap<KEY,MAPPED,HASHER,AGGR_FUN> >(aggregateHashMap);
        base_t::_dirty = true;
        base_t::_map = copyHashMap->getMap();
    }

    AggregateHashMap(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, field_t aggrFunField)
        : base_t(t, f) {
        populateMap(aggrFunField);
    }

    virtual void mergeWith(hyrise::storage::c_ahashtable_ptr_t aggregateHashMap) {
        auto mergeHashMap = std::dynamic_pointer_cast<const AggregateHashMap<KEY,MAPPED,HASHER,AGGR_FUN> >(aggregateHashMap);
        for(auto element : mergeHashMap->getMap()) {
            auto result = base_t::_map.insert(element);
            if(!result.second) {
                AGGR_FUN::update(result.first->second, element.second);
            }
        }
        _posMap.insert(mergeHashMap->getPosMapBegin(), mergeHashMap->getPosMapEnd());
    }

    virtual uint64_t numKeys() const {
        if (base_t::_dirty) {
          uint64_t result = 0;

          for (auto it1 = base_t::_map.begin(), it2 = it1, end = base_t::_map.end(); it1 != end; it1 = it2) {
            for (; (it2 != end) && (it1->first == it2->first); ++it2) {}
            ++result;
          }

          base_t::_numKeys = result;
          base_t::_dirty = false;
        }
        return base_t::_numKeys;
    }

    virtual size_t size() const {
        return base_t::_map.size();
    }

    const map_t& getMap() const {
        return base_t::_map;
    }

    posmap_const_iterator_t getPosMapBegin() const {
      return _posMap.begin();
    }

    posmap_const_iterator_t getPosMapEnd() const {
      return _posMap.end();
    }

    /// Get positions for values in the table cells of given row and columns.
    virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t& table,
                           const field_list_t &columns,
                           const pos_t row) const {

    }

protected:
    inline void populateMap(const field_t aggrFuncField) {
        base_t::_dirty = true;
        size_t fieldSize = base_t::_fields.size();
        size_t tableSize = base_t::_table->size();
        for (pos_t row = 0; row < tableSize; ++row) {
            key_t key = HASHER::getGroupKey(base_t::_table, base_t::_fields, fieldSize, row);
            _posMap[row] = key;
            hyrise_int_t rowValue = AGGR_FUN::firstValue(base_t::_table->template getValue<hyrise_int_t>(aggrFuncField, row));
            auto result = base_t::_map.insert(typename map_t::value_type(key, rowValue));
            if(!result.second) {
                AGGR_FUN::update(result.first->second, rowValue);
            }
        }
    }

    posMap_t _posMap;
};

struct SumAggregationFunc {
    template <typename R>
    static R firstValue(const R& value) {
        return value;
    }

    template <typename R>
    static void update(R& curr, const R& value) {
        curr += value;
    }
};

} } // namespace hyrise::storage
