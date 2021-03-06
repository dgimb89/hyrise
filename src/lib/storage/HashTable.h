// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include <atomic>
#include <algorithm>
#include <set>
#include <unordered_map>
#include <memory>
#include <sstream>

#include "helper/types.h"
#include "helper/checked_cast.h"

#include "storage/AbstractHashTable.h"
#include "storage/AbstractTable.h"
#include "storage/storage_types.h"
#include "storage/HashTableView.h"


namespace hyrise {
namespace storage {

// Group of value_ids as key to an unordered map
typedef std::vector<value_id_t> aggregate_key_t;
// Group of hashed values as key to an unordered map
typedef std::vector<size_t> join_key_t;

// Single Value ID as key to unordered map
typedef value_id_t aggregate_single_key_t;
// Single Hashed Value
typedef size_t join_single_key_t;

size_t hash_value(const c_atable_ptr_t &source, const size_t &f, const ValueId &vid);

template <typename HashResult>
inline typename HashResult::value_type extract(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid);

template <>
inline typename join_key_t::value_type extract<join_key_t>(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid) {
  return hash_value(table, field, vid);
}

template <>
inline typename aggregate_key_t::value_type extract<aggregate_key_t>(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid) {
  return vid.valueId;
}

// Helper Functions for Single Values
template<typename HashResult>
inline HashResult extractSingle(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid);

template <>
inline join_single_key_t extractSingle<join_single_key_t>(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid) {
  return hash_value(table, field, vid);
}

template <>
inline aggregate_single_key_t extractSingle<aggregate_single_key_t>(const c_atable_ptr_t &table,
    const size_t &field,
    const ValueId &vid) {
  return vid.valueId;
}

// hash function for aggregate_key_t
template<class T>
class GroupKeyHash {
public:
  size_t operator()(const T &key) const {
    static auto hasher = std::hash<size_t>();

    std::size_t seed = 0;
    for (size_t i = 0, key_size = key.size();
         i < key_size; ++i) {
      // compare boost hash_combine
      seed ^= hasher(key[i]) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }

  static T getGroupKey(const c_atable_ptr_t &table,
                       const field_list_t &columns,
                       const size_t fieldCount,
                       const pos_t row) {
    ValueIdList value_list = table->copyValueIds(row, &columns);
    T key;
    for (size_t i = 0, key_size = fieldCount; i < key_size; i++)
      key.push_back(extract<T>(table, columns[i], value_list[i]));
    return key;
  }
};

// Simple Hash Function for single values
template<class T>
class SingleGroupKeyHash {
public:
  size_t operator()(const T &key) const {
    return std::hash<size_t>()(key);
  }

  static T getGroupKey(const c_atable_ptr_t &table,
                       const field_list_t &columns,
                       const size_t fieldCount,
                       const pos_t row){
    return extractSingle<T>(table, columns[0], table->getValueId(columns[0], row));
  }
};

// Multi Keys
typedef std::unordered_multimap<aggregate_key_t, pos_t, GroupKeyHash<aggregate_key_t> > aggregate_hash_map_t;
typedef std::unordered_multimap<join_key_t, pos_t, GroupKeyHash<join_key_t> > join_hash_map_t;

// Single Keys
typedef std::unordered_multimap<aggregate_single_key_t, pos_t, SingleGroupKeyHash<aggregate_single_key_t> > aggregate_single_hash_map_t;
typedef std::unordered_multimap<join_single_key_t, pos_t, SingleGroupKeyHash<join_single_key_t> > join_single_hash_map_t;

/// HashTable based on a map; key specifies the key for the given map
template<class MAP, class KEY> class HashTable;
typedef HashTable<aggregate_hash_map_t, aggregate_key_t> AggregateHashTable;
typedef HashTable<join_hash_map_t, join_key_t> JoinHashTable;

// HashTables for single values
typedef HashTable<aggregate_single_hash_map_t, aggregate_single_key_t> SingleAggregateHashTable;
typedef HashTable<join_single_hash_map_t, join_single_key_t> SingleJoinHashTable;

/// Uses valueIds of specified columns as key for an unordered_multimap
template <class MAP, class KEY>
class HashTable : public HashTableBase<MAP,KEY>, public std::enable_shared_from_this<HashTable<MAP, KEY> > {
public:
    typedef HashTableBase<MAP,KEY> base_t;
    typedef KEY key_t;
    typedef MAP map_t;
    typedef typename map_t::const_iterator map_const_iterator_t;
    typedef decltype(std::declval<const map_t>().equal_range(key_t())) map_const_range_t;

private:
  // populates map with values
  inline void populate_map(size_t row_offset = 0) {
    base_t::_dirty = true;
    size_t fieldSize = base_t::_fields.size();
    size_t tableSize = base_t::_table->size();
    for (pos_t row = 0; row < tableSize; ++row) {
      key_t key = MAP::hasher::getGroupKey(base_t::_table, base_t::_fields, fieldSize, row);
      base_t::_map.insert(typename map_t::value_type(key, row + row_offset));
    }
  }

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

public:
  HashTable() {}

  // create a new HashTable based on a number of HashTables
  explicit HashTable(const std::vector<std::shared_ptr<const AbstractHashTable> >& hashTables) {
    base_t::_dirty = true;

    // use copy operator for the first given hashtable
    base_t::_map = checked_pointer_cast<const HashTable<MAP, KEY>>(hashTables.front())->getMap();

    // insert entries of the remaining hash tables
    for(auto htIt = hashTables.begin()+1; htIt != hashTables.end(); ++htIt) {
      const auto& ht = checked_pointer_cast<const HashTable<MAP, KEY>>(*htIt);
      base_t::_map.insert(ht->getMapBegin(), ht->getMapEnd());
    }
  }

  // Hash given table's columns directly into the new HashTable
  // row_offset is used if t is a TableRangeView, so that the HashTable can build the pos_lists based on the row numbers of the original table
  HashTable(c_atable_ptr_t t, const field_list_t &f, size_t row_offset = 0)
      : base_t(t, f) {
    populate_map(row_offset);
  }

  virtual ~HashTable() {}

  std::string stats() const {
    std::stringstream s;
    s << "Load Factor " << base_t::_map.load_factor() << " / ";
    s << "Max Load Factor " << base_t::_map.max_load_factor() << " / ";
    s << "Bucket Count " << base_t::_map.bucket_count();
    return s.str();
  }

  std::shared_ptr<HashTableView<MAP, KEY, HashTable> > view(size_t first, size_t last) const {
    return std::make_shared<HashTableView<MAP, KEY, HashTable> >(this->shared_from_this(), first, last);
  }

  /// Get const interators to underlying map's begin or end.
  virtual map_const_iterator_t getMapBegin() const {
      return base_t::_map.begin();
  }
  virtual map_const_iterator_t getMapEnd() const {
      return base_t::_map.end();
  }

  virtual const map_t &getMap() const {
    return base_t::_map;
  }


  /// Returns the number of key value pairs of underlying hash map structure.
  virtual size_t size() const {
      return base_t::_map.size();
    }

  /// Get positions for values in the table cells of given row and columns.
  virtual pos_list_t get(const c_atable_ptr_t& table,
                         const field_list_t &columns,
                         const pos_t row) const {
      key_t key = MAP::hasher::getGroupKey(table, columns, columns.size(), row);
      auto range = base_t::_map.equal_range(key);
      return constructPositions(range);
    }

  virtual pos_list_t get(const key_t &key) const {
    auto range = base_t::_map.equal_range(key);
    return constructPositions(range);
  }

  virtual uint64_t numKeys() const {
      if (base_t::_dirty) {
        uint64_t result = 0;

        for (map_const_iterator_t it1 = base_t::_map.begin(), it2 = it1, end = base_t::_map.end(); it1 != end; it1 = it2) {
          for (; (it2 != end) && (it1->first == it2->first); ++it2) {}
          ++result;
        }

        base_t::_numKeys = result;
        base_t::_dirty = false;
      }
      return base_t::_numKeys;
    }
};

} } // namespace hyrise::storage

