#include "GroupByBuild.h"
#include "storage/TableRangeView.h"

namespace hyrise {
namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<GroupByBuild>("GroupByBuild");
}

std::shared_ptr<PlanOperation> GroupByBuild::parse(const Json::Value &data) {
    auto instance = std::make_shared<GroupByBuild>();
    if (data.isMember("fields")) {
      for (unsigned i = 0; i < data["fields"].size(); ++i) {
        instance->addField(data["fields"][i]);
      }
    }

    instance->setAggrFunField(data["aggrFunField"].asUInt());
    return instance;
}

const std::string GroupByBuild::vname() {
    return "GroupByBuild";
}

void GroupByBuild::setAggrFunField(field_t aggrFunField) {
    _aggrFunField = aggrFunField;
}

void GroupByBuild::executePlanOperation() {
    if (_field_definition.size() == 1)
        addResult(std::make_shared<storage::SingleKeyAggregateHashMap<hyrise_int_t, storage::SumAggregationFunc> >(getInputTable(), _field_definition, _aggrFunField));
      else
        addResult(std::make_shared<storage::MultiKeyAggregateHashMap<hyrise_int_t, storage::SumAggregationFunc> >(getInputTable(), _field_definition, _aggrFunField));

}

}
}
