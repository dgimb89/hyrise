// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_MERGEAGGREGATEHASHMAP_H_
#define SRC_LIB_ACCESS_MERGEAGGREGATEHASHMAP_H_

#include "access/system/PlanOperation.h"

namespace hyrise {
namespace access {

/// PlanOp that merges several aggregate hash maps
class MergeAggregateHashMap : public PlanOperation {
public:
  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
  const std::string vname();
};

}
}

#endif  // SRC_LIB_ACCESS_MERGEAGGREGATEHASHMAP_H_
