// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_ABSTRACTHASHTABLE_H_
#define SRC_LIB_STORAGE_ABSTRACTHASHTABLE_H_

#include <cstdint>
#include <memory>
#include <algorithm>


#include "helper/types.h"
#include "storage/AbstractResource.h"
#include "storage/storage_types.h"

class AbstractTable;

/// HashTable that maps table cells' hashed values of arbitrary columns to their rows.
class AbstractHashTable : public AbstractResource {
protected:
    AbstractHashTable() {}
public:
  virtual ~AbstractHashTable() {}

  /// Returns the number of key value pairs of underlying hash map structure.
  virtual size_t size() const = 0;

  /// Get positions for values in the table cells of given row and columns.
  virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t& table,
                         const field_list_t &columns,
                         const pos_t row) const = 0;

  virtual hyrise::storage::c_atable_ptr_t getTable() const = 0;

  virtual field_list_t getFields() const = 0;

  virtual size_t getFieldCount() const = 0;

  virtual uint64_t numKeys() const = 0;
};

template <class MAP, class KEY>
class HashTableBase : public AbstractHashTable {
public:
    typedef KEY key_t;
    typedef MAP map_t;
    typedef typename map_t::const_iterator map_const_iterator_t;
    typedef decltype(std::declval<const map_t>().equal_range(key_t())) map_const_range_t;

protected:
  // Underlaying storage type
  map_t _map;

  // Reference to the table
  hyrise::storage::c_atable_ptr_t _table;

  // Fields in map
  const field_list_t _fields;

  // Cached num keys
  mutable std::atomic<uint64_t> _numKeys;
  mutable std::atomic<bool> _dirty;

  // abstract class for basic hash table functionality
  HashTableBase(hyrise::storage::c_atable_ptr_t t, const field_list_t &f) : _table(t), _fields(f), _numKeys(0), _dirty(true) {}
  HashTableBase() : _numKeys(0), _dirty(true) {}

public:

  virtual ~HashTableBase() {}

  /// Returns the number of key value pairs of underlying hash map structure.
  virtual size_t size() const {
      return _map.size();
    }

  /// Get positions for values in the table cells of given row and columns.
  virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t& table,
                         const field_list_t &columns,
                         const pos_t row) const {
      key_t key = MAP::hasher::getGroupKey(table, columns, columns.size(), row);
      auto range = _map.equal_range(key);
      return constructPositions(range);
    }

  virtual pos_list_t get(const key_t &key) const {
    auto range = _map.equal_range(key);
    return constructPositions(range);
  }

  virtual hyrise::storage::c_atable_ptr_t getTable() const {
      return _table;
    }

  virtual field_list_t getFields() const {
      return _fields;
    }

  virtual size_t getFieldCount() const {
      return _fields.size();
    }

  virtual uint64_t numKeys() const {
      if (_dirty) {
        uint64_t result = 0;
        for (map_const_iterator_t it1 = _map.begin(), it2 = it1, end = _map.end(); it1 != end; it1 = it2) {
          for (; (it2 != end) && (it1->first == it2->first); ++it2) {}
          ++result;
        }

        _numKeys = result;
        _dirty = false;
      }
      return _numKeys;
    }
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

#endif  // SRC_LIB_STORAGE_ABSTRACTHASHTABLE_H_

