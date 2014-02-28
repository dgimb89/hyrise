#ifndef SRC_LIB_STORAGE_HASHTABLEVIEW_H
#define SRC_LIB_STORAGE_HASHTABLEVIEW_H

#include "storage/AbstractHashTable.h"

/// Maps table cells' hashed values of arbitrary columns to their rows.
/// This subclass maps only a range of key value pairs of its underlying
/// HashTable for an easy splitting
template <class MAP, class KEY, template<class,class> class HASHTABLE>
class HashTableView : public AbstractHashTable {

protected:
  typedef std::shared_ptr<const HASHTABLE<MAP, KEY> > hash_table_t;
  hash_table_t _hashTable;

  typedef typename MAP::const_iterator map_const_iterator_t;
  map_const_iterator_t _begin;
  map_const_iterator_t _end;
  typedef KEY key_t;

  mutable std::atomic<uint64_t> _numKeys;
  mutable std::atomic<bool> _dirty;

public:
  /// Given a HashTable and a range, only the n-ths key value pairs of the
  /// given HashTable corresponding to the range will be mapped by this view.
  HashTableView(const hash_table_t& tab,
                const size_t start,
                const size_t end) :
  _hashTable(tab), _begin(_hashTable->getMapBegin()), _end(_hashTable->getMapBegin()), _numKeys(0), _dirty(true) {

    _begin = advance(start);
    _end = advance(end);
  }


  map_const_iterator_t advance(size_t val) {
    size_t counter = 0;
    if (val == 0)
      return _hashTable->getMapBegin();

    for (map_const_iterator_t it1 = _hashTable->getMapBegin(), it2 = it1, end = _hashTable->getMapEnd();
      it1 != end; it1 = it2) {

      // Skip to next key-value pair
      for (; (it2 != end) && (it1->first == it2->first); ++it2);
      if (++counter == val)
        return it2;
    }
    //if (counter == val)
      return _hashTable->getMapEnd();
    //throw std::runtime_error("Could not advance to position");
  }


  virtual ~HashTableView() {}

  /// Returns the number of key value pairs of underlying hash map structure.
  size_t size() const {
    return std::distance(_begin, _end);
  }

  /// Get positions for values in the table cells of given row and columns.
  /// TODO: check whether copy to new unordered_map and search via equal_range is faster
  virtual pos_list_t get(
    const hyrise::storage::c_atable_ptr_t &table,
    const field_list_t &columns,
    const pos_t row) const {

    pos_list_t pos_list;
    // produce key
    key_t key = MAP::hasher::getGroupKey(table, columns, columns.size(), row);

    for (map_const_iterator_t it = _begin; it != _end; ++it) {
      if (it->first == key) {
        pos_list.push_back(it->second);
      }
    }
    return pos_list;
  }

  pos_list_t get(key_t key) const {
    pos_list_t pos_list;

    for (map_const_iterator_t it = _begin; it != _end; ++it) {
      if (it->first == key) {
        pos_list.push_back(it->second);
      }
    }
    return pos_list;
  }


  /// Get const interators to underlying map's begin or end.
  map_const_iterator_t getMapBegin() const {
    return _begin;
  }
  map_const_iterator_t getMapEnd() const {
    return _end;
  }

  hyrise::storage::atable_ptr_t getHashTable() const {
    return _hashTable;
  }

  field_list_t getFields() const {
    return _hashTable->getFields();
  }

  size_t getFieldCount() const {
    return _hashTable->getFieldCount();
  }

  hyrise::storage::c_atable_ptr_t getTable() const {
    return _hashTable->getTable();
  }

  uint64_t numKeys() const {
    if (_dirty) {
      uint64_t result = 0;
      for (map_const_iterator_t it1 = _begin, it2 = it1, end = _end; it1 != end; it1 = it2) {
        for (; (it2 != end) && (it1->first == it2->first); ++it2);
        ++result;
      }
      _numKeys = result;
      _dirty = false;
    }
    return _numKeys;
  }
};

#endif // !SRC_LIB_STORAGE_HASHTABLEVIEW_H
