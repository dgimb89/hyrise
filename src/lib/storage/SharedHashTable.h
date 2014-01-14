#ifndef SRC_LIB_STORAGE_SHAREDHASHTABLE_H
#define SRC_LIB_STORAGE_SHAREDHASHTABLE_H

#include "storage/HashTable.h"
#include <mutex>


template<class MAP, class KEY> class SharedHashTable;
template<class MAP, class KEY> class AtomicSharedHashTable;

/// HashTable based on a map; key specifies the key for the given map
typedef SharedHashTable<aggregate_hash_map_t, aggregate_key_t> AggregateLockingSharedHashTable;
typedef SharedHashTable<join_hash_map_t, join_key_t> JoinLockingSharedHashTable;

/// HashTables for single values
typedef SharedHashTable<aggregate_single_hash_map_t, aggregate_single_key_t> SingleAggregateLockingSharedHashTable;
typedef SharedHashTable<join_single_hash_map_t, join_single_key_t> SingleJoinLockingSharedHashTable;

/// HashTable based on a map; key specifies the key for the given map
typedef AtomicSharedHashTable<aggregate_hash_map_t, aggregate_key_t> AggregateAtomicSharedHashTable;
typedef AtomicSharedHashTable<join_hash_map_t, join_key_t> JoinAtomicSharedHashTable;

/// HashTables for single values
typedef AtomicSharedHashTable<aggregate_single_hash_map_t, aggregate_single_key_t> SingleAggregateAtomicSharedHashTable;
typedef AtomicSharedHashTable<join_single_hash_map_t, join_single_key_t> SingleJoinAtomicSharedHashTable;

class AbstractSharedHashTable {
public:
    virtual void populateMap() = 0;
};

template <class MAP, class KEY>
class SharedHashTableBase : public HashTableBase<std::shared_ptr<MAP>,KEY>, public AbstractSharedHashTable, public std::enable_shared_from_this<SharedHashTableBase<MAP,KEY>> {
public:
    typedef HashTableBase<std::shared_ptr<MAP>,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;
    typedef std::shared_ptr<MAP> map_ptr_t;
    typedef typename map_t::const_iterator map_const_iterator_t;
    typedef decltype(std::declval<const MAP>().equal_range(key_t())) map_const_range_t;

    SharedHashTableBase(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, unsigned row_offset = 0)
        : SharedHashTableBase(t, f, nullptr, row_offset) {
    }

    SharedHashTableBase(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, map_ptr_t map_ptr, unsigned row_offset = 0)
        : base_t(t, f) {
        setMap(map_ptr);
        setRowOffset(row_offset);
    }

    map_ptr_t getMap() {
        // lazy initialization
        if(!base_t::_map) {
            base_t::_map = std::make_shared<map_t>();
        }
        return base_t::_map;
    }

    void setMap(map_ptr_t map_ptr) {
        base_t::_map = map_ptr;
    }

    virtual size_t size() const {
        return base_t::_map->size();
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

    void setRowOffset(size_t rowOffset) {
        m_rowOffset = rowOffset;
    }

protected:
    size_t m_rowOffset;

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

template <class MAP, class KEY>
class SharedHashTable : public SharedHashTableBase<MAP, KEY> {
public:
    typedef SharedHashTableBase<MAP,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;

    SharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, size_t row_offset = 0)
        : base_t(t, f, nullptr) {
    }
public:
    virtual void populateMap() {
        base_t::_dirty = true;
        size_t fieldSize = base_t::_fields.size();
        size_t tableSize = base_t::_table->size();
        typename base_t::map_ptr_t map = base_t::getMap();
        for (pos_t row = 0; row < tableSize; ++row) {
            key_t key = MAP::hasher::getGroupKey(base_t::_table, base_t::_fields, fieldSize, row);
            map->insert(typename map_t::value_type(key, row + base_t::m_rowOffset));
        }
    }
};

template <class MAP, class KEY>
class AtomicSharedHashTable : public SharedHashTableBase<MAP, KEY> {

};

#endif // SRC_LIB_STORAGE_SHAREDHASHTABLE_H
