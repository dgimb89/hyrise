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
    if(!getMap()) throw std::runtime_error("SharedHashBuild can not run without defined map container");

    // check if table is a TableRangeView; if yes, provide the offset to HashTable
    auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
    if(input)
        row_offset = input->getStart();

    // map / key type tuple already determinated in SharedHashGenerator, take given one
    if(_preferAtomic) {
        addResult(std::make_shared<AtomicSharedHashTable<map_t, key_t>(getInputTable(), _field_definition, _map, row_offset));
    } else {
        addResult(std::make_shared<SharedHashTable<map_t, key_t>(getInputTable(), _field_definition, _map, row_offset));
    }
}

const std::string SharedHashBuild::vname() {
    return "SharedHashBuild";
}

void SharedHashBuild::setMap(SharedHashBuild::map_ptr_t map) {
    _map = map;
}

SharedHashBuild::map_ptr_t SharedHashBuild::getMap() {
    return _map;
}

}
}
