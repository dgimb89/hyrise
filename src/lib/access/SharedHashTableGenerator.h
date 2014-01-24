#ifndef SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
#define SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H

#include "access/SharedHashBuild.h"
#include <tbb/concurrent_unordered_map.h>

namespace hyrise {
namespace access {

// Multi Keys
typedef tbb::concurrent_unordered_multimap<aggregate_key_t, pos_t, GroupKeyHash<aggregate_key_t> > tbb_aggregate_hash_map_t;
typedef tbb::concurrent_unordered_multimap<aggregate_key_t, std::atomic<value_id_t>, GroupKeyHash<aggregate_key_t> tbb_value_hash_map_t;
// Single Keys
typedef tbb::concurrent_unordered_multimap<aggregate_single_key_t, pos_t, SingleGroupKeyHash<aggregate_single_key_t> > tbb_aggregate_single_hash_map_t;
typedef tbb::concurrent_unordered_multimap<aggregate_single_key_t, std::atomic<value_id_t>, SingleGroupKeyHash<aggregate_single_key_t> > tbb_value_single_hash_map_t;

class SharedHashTableGenerator : public PlanOperation {
public:
    void executePlanOperation();
    const std::string vname();

    void setNumberOfSpawns(const size_t number);
    size_t getNumberOfSpawns();

    void setKey(const std::string &key);
    void setAggregationType(const std::string& aggrFuncType);

    static std::shared_ptr<PlanOperation> parse(const Json::Value &data);

protected:
    std::string _key;
    size_t _numberOfSpawns;
    std::string _aggrFuncType;
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
