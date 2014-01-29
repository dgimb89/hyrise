#include "SharedHashTableGenerator.h"
#include "access/system/QueryParser.h"
#include "access/system/ResponseTask.h"
#include "taskscheduler/SharedScheduler.h"
#include "storage/TableRangeView.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<SharedHashTableGenerator>("SharedHashTableGenerator");
}

void SharedHashTableGenerator::executePlanOperation() {
    std::vector<std::shared_ptr<SharedHashBuild>> children;
    std::vector<std::shared_ptr<Task>> successors;
    auto scheduler = SharedScheduler::getInstance().getScheduler();

    {
        std::lock_guard<decltype(_observerMutex)> lk(_observerMutex);
        for (const auto& weakDoneObserver : _doneObservers) {
            if (auto doneObserver = weakDoneObserver.lock()) {
                if (const auto task = std::dynamic_pointer_cast<Task>(doneObserver)) {
                    successors.push_back(std::move(task));
                }
            }
        }
    }

    for (size_t i = 0; i < _numberOfSpawns; ++i) {
        auto child = std::dynamic_pointer_cast<SharedHashBuild>(QueryParser::instance().parse("SharedHashBuild", Json::Value()));
        child->addInput(getInputTable());
        child->setCount(_numberOfSpawns);
        child->setPart(i);
        for(auto field : _field_definition)
            child->addField(field);

        // will not be used if no aggrFuncType is set
        child->setAggregationFunctionField(_aggrFuncField);

        children.push_back(child);
        auto r = getResponseTask();
        if(r != nullptr)
            r->registerPlanOperation(children[i]);

      for (auto successor : successors)
        successor->addDependency(children[i]);
    }
    // create map container here and schedule workers
    // we have to do it ugly in order too keep template typenames

    if (_key == "groupby") {
        if (_field_definition.size() == 1) {
            if(_aggrFuncType.empty()) {
                auto map = std::make_shared<tbb_aggregate_single_hash_map_t>();
                map->rehash(20000000);
                for (auto child : children) {
                    addSharedHashtableResult(child->setMap<tbb_aggregate_single_hash_map_t, aggregate_single_key_t>(map));
                    scheduler->schedule(child);
                }
            } else {
                auto map = std::make_shared<tbb_value_single_hash_map_t>();
                map->rehash(20000000);
                for (auto child : children) {
                    addSharedHashtableResult(child->setMap<tbb_value_single_hash_map_t, aggregate_single_key_t>(map, _aggrFuncType));
                    scheduler->schedule(child);
                }
            }
        }
        else {
            if(_aggrFuncType.empty()) {
                auto map = std::make_shared<tbb_aggregate_hash_map_t>();
                map->rehash(20000000);
                for (auto child : children) {
                    addSharedHashtableResult(child->setMap<tbb_aggregate_hash_map_t, aggregate_key_t>(map));
                    scheduler->schedule(child);
                }
            } else {
                auto map = std::make_shared<tbb_value_hash_map_t>();
                map->rehash(20000000);
                for (auto child : children) {
                    addSharedHashtableResult(child->setMap<tbb_value_hash_map_t, aggregate_key_t>(map, _aggrFuncType));
                    scheduler->schedule(child);
                }
            }
        }
    } else {
        throw std::runtime_error("Type in Plan operation SharedHashGenerator not supported; key: " + _key);
    }
}

void SharedHashTableGenerator::setNumberOfSpawns(const size_t number) {
    _numberOfSpawns = number;
}

size_t SharedHashTableGenerator::getNumberOfSpawns() {
    return _numberOfSpawns;
}

void SharedHashTableGenerator::setKey(const std::string &key) {
    _key = key;
}

void SharedHashTableGenerator::setAggregationType(const std::string &aggrFuncType) {
    _aggrFuncType = aggrFuncType;
}

void SharedHashTableGenerator::setAggregationFunctionField(field_t aggrFuncField) {
    _aggrFuncField = aggrFuncField;
}

std::shared_ptr<PlanOperation> SharedHashTableGenerator::parse(const Json::Value &data) {
  auto instance = std::make_shared<SharedHashTableGenerator>();
  if (data.isMember("numCores")) {
      instance->setNumberOfSpawns(data["numCores"].asUInt());
  }
  if (data.isMember("fields")) {
    for (unsigned i = 0; i < data["fields"].size(); ++i) {
      instance->addField(data["fields"][i]);
    }
  }

  if (data.isMember("key")) {
    instance->setKey(data["key"].asString());
  }
  if (data.isMember("aggregateFunction")) {
      // only accepting field indices for aggregateFunction parameters yet
      instance->setAggregationFunctionField(data["aggregateFunction"]["field"].asUInt());
      instance->setAggregationType(data["aggregateFunction"]["type"].asString());
  }
  return instance;
}

void SharedHashTableGenerator::addSharedHashtableResult(const storage::c_aresource_ptr_t &hashtable) {
    // as the same shared hashtable is filled by all workers, we have to only add one of them as result resource
    if(!output.size()) {
        addResult(hashtable);
    }
}

const std::string SharedHashTableGenerator::vname() {
  return "SharedHashTableGenerator";
}

}
}