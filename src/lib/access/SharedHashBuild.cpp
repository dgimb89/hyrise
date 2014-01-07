// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "access/SharedHashBuild.h"
#include "storage/SharedHashTable.h"
#include "storage/TableRangeView.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<HashBuild>("SharedHashBuild");
}

SharedHashBuild::~SharedHashBuild() {
}

void SharedHashBuild::executePlanOperation() {
  size_t row_offset = 0;
  // check if table is a TableRangeView; if yes, provide the offset to HashTable
  auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
  if(input)
    row_offset = input->getStart();

  if(_key == "groupby") {
    if (_field_definition.size() == 1)
        addResult(std::make_shared<SingleAggregateLockingSharedHashTable>(getInputTable(), _field_definition, row_offset));
    else
        addResult(std::make_shared<AggregateLockingSharedHashTable>(getInputTable(), _field_definition, row_offset));
  } /*else if(_key == "groupby_atomic") {
    if (_field_definition.size() == 1)
        addResult(std::make_shared<SingleAggregateAtomicSharedHashTable>(getInputTable(), _field_definition, row_offset));
    else
        addResult(std::make_shared<AggregateAtomicSharedHashTable>(getInputTable(), _field_definition, row_offset));
  } */ else {
    throw std::runtime_error("Type in Plan operation SharedHashBuild not supported; key: " + _key);
  }
}

const std::string SharedHashBuild::vname() {
  return "SharedHashBuild";
}

}
}
