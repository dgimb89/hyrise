#ifndef SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
#define SRC_LIB_ACCESS_SHAREDHASHBUILD_H_

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
    std::shared_ptr<AbstractHashTable> setMap(std::shared_ptr<MAP> map) {
        // refresh input here as row_offset is needed here already
        refreshInput();

        // map / key type tuple already determinated in SharedHashGenerator, take given one
        size_t row_offset = 0;
        // check if table is a TableRangeView; if yes, provide the offset to HashTable
        auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
        if(input)
            row_offset = input->getStart();

        auto hashTable = std::make_shared<SharedHashTable<MAP,KEY> >(getInputTable(), _field_definition, map, row_offset);
        addResult(hashTable);
        setHashTable(hashTable);
        return hashTable;
    }

    template <typename MAP, typename KEY>
    std::shared_ptr<AbstractHashTable> setMap(std::shared_ptr<MAP> map, const std::string& aggrFuncType) {
        // map / key type tuple already determinated in SharedHashGenerator, take given one
        std::shared_ptr<AbstractHashTable> hashTable;
        if(aggrFuncType == "SUM") {
            hashTable = std::make_shared<AtomicSharedHashTable<MAP,KEY, AtomicSumAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "COUNT") {
            hashTable = std::make_shared<AtomicSharedHashTable<MAP,KEY, AtomicCountAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "MIN") {
            hashTable = std::make_shared<AtomicSharedHashTable<MAP,KEY, AtomicMinAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        } else if(aggrFuncType == "MAX") {
            hashTable = std::make_shared<AtomicSharedHashTable<MAP,KEY, AtomicMaxAggregationFunc> >(getInputTable(), _field_definition, map, _aggrFuncField);
        }

        // aggrFuncType not valid
        if(!hashTable) throw std::runtime_error("Aggregation Function Type in PlanOperation SharedHashBuild not supported; aggrFuncType: " + aggrFuncType);

        addResult(hashTable);
        setHashTable(std::dynamic_pointer_cast<AbstractSharedHashTable>(hashTable));
        return hashTable;
    }

    void setAggregationFunctionField(field_t aggrFuncField);
    static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

protected:
    void setHashTable(std::shared_ptr<AbstractSharedHashTable> hashTable);
    field_t _aggrFuncField;

private:
    std::shared_ptr<AbstractSharedHashTable> _hashTable = nullptr;
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHBUILD_H_l
