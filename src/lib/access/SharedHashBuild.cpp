// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "access/SharedHashBuild.h"
#include "storage/TableRangeView.h"
#include <memory>

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<SharedHashBuild>("SharedHashBuild");
}

SharedHashBuild::~SharedHashBuild() {
}

void SharedHashBuild::executePlanOperation() {
    if(!_hashTable)
        std::runtime_error("Failed converting given HashTable to SharedHashTable");

    _hashTable->populateMap();
}

const std::string SharedHashBuild::vname() {
    return "SharedHashBuild";
}

void SharedHashBuild::setKey(const std::string &key) {
    // TODO: not yet implemented
    // only works for groupby yet
}

template <template <typename, typename> class MAP_CONTAINER, typename KEY, typename VALUE>
void SharedHashBuild::setMap(std::shared_ptr<MAP_CONTAINER<KEY, VALUE>> map) {
    const size_t row_offset = 0;
    // check if table is a TableRangeView; if yes, provide the offset to HashTable
    auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
    if(input)
        row_offset = input->getStart();

    // map / key type tuple already determinated in SharedHashGenerator, take given one
    if(_preferAtomic) {
        auto hashTable = std::make_shared<AtomicSharedHashTable<MAP_CONTAINER<KEY,VALUE>, KEY>>(getInputTable(), _field_definition, map, row_offset);
        addResult(hashTable);
        setHashTable(hashTable);
    } else {
        auto hashTable = std::make_shared<SharedHashTable<MAP_CONTAINER<KEY,VALUE>, KEY>>(getInputTable(), _field_definition, map, row_offset);
        addResult(hashTable);
        setHashTable(hashTable);
    }
}

std::shared_ptr<PlanOperation> SharedHashBuild::parse(const Json::Value &data) {
    std::shared_ptr<PlanOperation> instance = BasicParser<SharedHashBuild>::parse(data);
    return instance;
}

void SharedHashBuild::setHashTable(std::shared_ptr<AbstractSharedHashTable> hashTable) {
    _hashTable = hashTable;
}

}
}
