#ifndef SRC_LIB_STORAGE_SHAREDHASHTABLE_H
#define SRC_LIB_STORAGE_SHAREDHASHTABLE_H

#include "storage/HashTable.h"
#include <mutex>


template<class MAP, class KEY> class SharedHashTable;

// SharedHashTable for multiple values
typedef SharedHashTable<aggregate_hash_map_t, aggregate_key_t> AggregateSharedHashTable;

// SharedHashTable for single values
typedef SharedHashTable<aggregate_single_hash_map_t, aggregate_single_key_t> SingleAggregateSharedHashTable;

template <class MAP, class KEY>
class SharedHashTable : public AbstractHashTable, public std::enable_shared_from_this<SharedHashTable<MAP, KEY> > {
public:
  typedef KEY key_t;
  typedef MAP map_t;
  typedef typename map_t::const_iterator map_const_iterator_t;
  typedef decltype(std::declval<const map_t>().equal_range(key_t())) map_const_range_t;

protected:

  // Underlaying storage type
  map_t* _map;

  // Reference to the table
  hyrise::storage::c_atable_ptr_t _table;

  // Fields in map
  const field_list_t _fields;

  // Cached num keys
  mutable std::atomic<uint64_t> _numKeys;
  mutable std::atomic<bool> _dirty;

  // mutex
  std::mutex* _mutex;


private:

  // populates map with values
  inline virtual void populate_map(size_t row_offset = 0) {
    _dirty = true;
    size_t fieldSize = _fields.size();
    size_t tableSize = _table->size();
    for (pos_t row = 0; row < tableSize; ++row) {
      key_t key = MAP::hasher::getGroupKey(_table, _fields, fieldSize, row);
      requireLock();
      _map->insert(typename map_t::value_type(key, row + row_offset));
      releaseLock();
    }
  }

  inline void requireLock() {
    _mutex->lock();
  }

  inline void releaseLock() {
    _mutex->unlock();
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
  SharedHashTable() {}

  // create a new HashTable based on a number of HashTables
  explicit SharedHashTable(const std::vector<std::shared_ptr<const AbstractHashTable> >& hashTables) {
    _dirty = true;
    for (auto & nextElement: hashTables) {
      const auto& ht = checked_pointer_cast<const HashTable<MAP, KEY>>(nextElement);
      requireLock();
      _map->insert(ht->getMapBegin(), ht->getMapEnd());
      releaseLock();
    }
  }

  // Hash given table's columns directly into the new HashTable
  // row_offset is used if t is a TableRangeView, so that the HashTable can build the pos_lists based on the row numbers of the original table
  SharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, size_t row_offset = 0)
    : _table(t), _fields(f), _numKeys(0), _dirty(true) {
    populate_map(row_offset);
  }

  virtual ~SharedHashTable() {}

  std::string stats() const {
    std::stringstream s;
    s << "Load Factor " << _map->load_factor() << " / ";
    s << "Max Load Factor " << _map->max_load_factor() << " / ";
    s << "Bucket Count " << _map->bucket_count();
    return s.str();
  }

  std::shared_ptr<HashTableView<MAP, KEY> > view(size_t first, size_t last) const {
    return std::make_shared<HashTableView<MAP, KEY>>(this->shared_from_this(), first, last);
  }

  /// Returns the number of key value pairs of underlying hash map structure.
  virtual size_t size() const {
    return _map->size();
  }

  /// Get positions for values given in the table by row and columns.
  virtual pos_list_t get(const hyrise::storage::c_atable_ptr_t &table,
                         const field_list_t &columns,
                         const pos_t row) const {
    pos_list_t pos_list;
    key_t key = MAP::hasher::getGroupKey(table, columns, columns.size(), row);
    auto range = _map->equal_range(key);
    return constructPositions(range);
  }

  /// Get const interators to underlying map's begin or end.
  map_const_iterator_t getMapBegin() const {
    return _map->begin();
  }

  map_const_iterator_t getMapEnd() const {
    return _map->end();
  }

  hyrise::storage::c_atable_ptr_t getTable() const {
    return _table;
  }

  field_list_t getFields() const {
    return _fields;
  }

  size_t getFieldCount() const {
    return _fields.size();
  }

  map_t &getMap() {
    return *_map;
  }

  virtual pos_list_t get(const key_t &key) const {
    auto range = _map->equal_range(key);
    return constructPositions(range);
  }

  uint64_t numKeys() const {
    if (_dirty) {
      uint64_t result = 0;
      for (map_const_iterator_t it1 = _map->begin(), it2 = it1, end = _map->end(); it1 != end; it1 = it2) {
        for (; (it2 != end) && (it1->first == it2->first); ++it2) {}
        ++result;
      }

      _numKeys = result;
      _dirty = false;
    }
    return _numKeys;
  }
};

#endif // SRC_LIB_STORAGE_SHAREDHASHTABLE_H
