// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "access/SharedHashBuild.h"
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

void SharedHashBuild::setAggregationFunctionField(field_t aggrFuncField) {
    _aggrFuncField = aggrFuncField;
}

void SharedHashBuild::setAggregationType(const std::string &aggrFuncType) {
    _aggrFuncType = aggrFuncType;
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
