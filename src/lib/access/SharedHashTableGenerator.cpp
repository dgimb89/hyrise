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
    auto scheduler = taskscheduler::SharedScheduler::getInstance().getScheduler();

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
            #ifdef HYRISE_USE_FOLLY
                std::shared_ptr<storage::shared_aggregate_single_hash_map_t> map(folly::make_unique<storage::shared_aggregate_single_hash_map_t>(50000000));
            #else
                auto map = std::make_shared<storage::shared_aggregate_single_hash_map_t>();
            #endif

            for (auto child : children) {
                addSharedHashtableResult(child->setMap<storage::shared_aggregate_single_hash_map_t, storage::aggregate_single_key_t>(map));
                scheduler->schedule(child);
            }
        }
        else {
           #ifdef HYRISE_USE_FOLLY
                std::shared_ptr<storage::shared_aggregate_hash_map_t> map(folly::make_unique<storage::shared_aggregate_hash_map_t>(50000000));
            #else
                auto map = std::make_shared<storage::shared_aggregate_hash_map_t>();
            #endif

            for (auto child : children) {
                addSharedHashtableResult(child->setMap<storage::shared_aggregate_hash_map_t, storage::aggregate_key_t>(map));
                scheduler->schedule(child);
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
