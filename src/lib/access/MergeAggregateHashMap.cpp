// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/MergeAggregateHashMap.h"

#include "access/system/QueryParser.h"

#include "storage/AggregateHashMap.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<MergeAggregateHashMap>("MergeAggregateHashMap");
}

void MergeAggregateHashMap::executePlanOperation() {
    auto result = std::make_shared<SingleKeyAggregateHashMap<hyrise_int_t, SumAggregationFunc> >(getInputHashTable(0));
    for(auto i = 1; i < input.numberOfHashTables(); ++i) {
        result->mergeWith(getInputHashTable(i));
    }
    addResult(result);
}

std::shared_ptr<PlanOperation> MergeAggregateHashMap::parse(const Json::Value &data) {
  return std::make_shared<MergeAggregateHashMap>();
}

const std::string MergeAggregateHashMap::vname() {
  return "MergeAggregateHashMap";
}

}
}
