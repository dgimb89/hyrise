// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_GROUPBYBUILD_H_
#define SRC_LIB_ACCESS_GROUPBYBUILD_H_

#include "access/system/ParallelizablePlanOperation.h"
#include "storage/AggregateHashMap.h"

namespace hyrise {
namespace access {


/// IMPORTANT: Is just a prototype for performance evaluation -- only SUM on Int-Values supported yet
/// does not build a full hashtable! but instead a aggregate result
/// GroupByBuild optimizes the non-shared parallel groupby aggregation by only holding for aggregation necessary data in hashtable
/// More performant than parallel GroupByScan
class GroupByBuild : public ParallelizablePlanOperation {
public:
  virtual void executePlanOperation();
  /// {
  ///     "operators": {
  ///         "0": {
  ///             "type": "TableLoad",
  ///             "table": "table",
  ///             "filename": "..."
  ///         },
  ///         "1": {
  ///             "type": "GroupByBuild",
  ///             "instances": 4,
  ///             "fields" : [1],
  ///             "aggrFun" : "COUNT",
  ///             "aggrFunField" : 1
  ///         },
  ///     },
  ///         "edges": [["0", "1"]]
  /// }
  static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
  const std::string vname();
  void setAggrFunField(field_t aggrFunField);

protected:

  field_t _aggrFunField;
};

}
}

#endif  // SRC_LIB_ACCESS_HASHBUILD_H_
