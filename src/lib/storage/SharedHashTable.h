#ifndef SRC_LIB_STORAGE_SHAREDHASHTABLE_H
#define SRC_LIB_STORAGE_SHAREDHASHTABLE_H

#include "storage/HashTable.h"
#include <mutex>

template <class MAP, class KEY>
class SharedHashTable : public HashTableBase<std::shared_ptr<MAP>,KEY>, public std::enable_shared_from_this<SharedHashTable<MAP,KEY>> {
public:
    typedef HashTableBase<std::shared_ptr<MAP>,KEY> base_t;
    typedef KEY key_t;
    typedef std::shared_ptr<MAP> map_ptr_t;

    SharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, size_t row_offset = 0)
        : base_t(t, f) {
        // create new map container
    }

    SharedHashTable(hyrise::storage::c_atable_ptr_t t, const field_list_t &f, map_ptr_t map, size_t row_offset = 0)
        : base_t(t, f), base_t::_map(map) {

    }

};

#endif // SRC_LIB_STORAGE_SHAREDHASHTABLE_H
