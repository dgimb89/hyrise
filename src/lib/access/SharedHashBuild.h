#pragma once

#include "access/HashBuild.h"
#include "storage/SharedHashTable.h"
#include "storage/TableRangeView.h"

namespace hyrise {
namespace access {

//SharedHashGenerator (PlanOp) -> spawn instances of SharedHashBuild(ParPlanOp)
class SharedHashBuild : public ParallelizablePlanOperation {
public:
    virtual ~SharedHashBuild();
    virtual void executePlanOperation();
    virtual const std::string vname();

    template <typename MAP, typename KEY>
    std::shared_ptr<storage::AbstractHashTable> setMap(std::shared_ptr<MAP> map) {
        // refresh input here as row_offset is needed here already
        refreshInput();

        // map / key type tuple already determinated in SharedHashGenerator, take given one
        size_t row_offset = 0;
        // check if table is a TableRangeView; if yes, provide the offset to HashTable
        auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
        if(input)
            row_offset = input->getStart();

        computeDeferredIndexes();
        auto hashTable = std::make_shared<storage::SharedHashTable<MAP,KEY> >(getInputTable(), _field_definition, map, row_offset);
        addResult(hashTable);
        setHashTable(hashTable);
        return hashTable;
    }

    template <typename MAP, typename KEY>
    std::shared_ptr<storage::AbstractHashTable> setMap(std::shared_ptr<MAP> map, const std::string& aggrFuncType) {
        // map / key type tuple already determinated in SharedHashGenerator, take given one
        std::shared_ptr<storage::AbstractHashTable> hashTable;
        if(aggrFuncType == "SUM") {
            hashTable = std::make_shared<storage::AtomicSharedHashTable<MAP,KEY, storage::AtomicSumAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "COUNT") {
            hashTable = std::make_shared<storage::AtomicSharedHashTable<MAP,KEY, storage::AtomicCountAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "MIN") {
            hashTable = std::make_shared<storage::AtomicSharedHashTable<MAP,KEY, storage::AtomicMinAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "MAX") {
            hashTable = std::make_shared<storage::AtomicSharedHashTable<MAP,KEY, storage::AtomicMaxAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        }

        // aggrFuncType not valid
        if(!hashTable) throw std::runtime_error("Aggregation Function Type in PlanOperation SharedHashBuild not supported; aggrFuncType: " + aggrFuncType);

        addResult(hashTable);
        setHashTable(std::dynamic_pointer_cast<storage::AbstractSharedHashTable>(hashTable));
        return hashTable;
    }

    void setAggregationFunctionField(field_t aggrFuncField);

    /// Do not use SharedHashBuild via JSON Query! Use SharedHashTableGenerator instead
    /// fields in integer-list notation
    /// make sure to call setMap<...> method before the operation is executed
    static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

protected:
    void setHashTable(std::shared_ptr<storage::AbstractSharedHashTable> hashTable);
    field_t _aggrFuncField;

private:
    std::shared_ptr<storage::AbstractSharedHashTable> _hashTable = nullptr;
};

}
}
