// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#pragma once

#include <cstdint>
#include <memory>
#include <algorithm>
#include <atomic>


#include "helper/types.h"
#include "storage/AbstractResource.h"
#include "storage/storage_types.h"

namespace hyrise {
namespace storage {

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

  virtual hyrise::storage::c_atable_ptr_t getTable() const {
      return _table;
    }

  virtual field_list_t getFields() const {
      return _fields;
    }

  virtual size_t getFieldCount() const {
      return _fields.size();
    }
};

} } // namespace hyrise::storage

