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
        size_t row_offset = 0;
        // check if table is a TableRangeView; if yes, provide the offset to HashTable
        auto input = std::dynamic_pointer_cast<const storage::TableRangeView>(getInputTable());
        if(input)
            row_offset = input->getStart();

        // map / key type tuple already determinated in SharedHashGenerator, take given one
        if(_preferAtomic) {
            auto hashTable = std::make_shared<AtomicSharedHashTable<MAP,KEY> >(getInputTable(), _field_definition, map, row_offset);
            addResult(hashTable);
            setHashTable(hashTable);
            return hashTable;
        } else {
            auto hashTable = std::make_shared<SharedHashTable<MAP,KEY> >(getInputTable(), _field_definition, map, row_offset);
            addResult(hashTable);
            setHashTable(hashTable);
            return hashTable;
        }
    }

    static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
protected:
    void setHashTable(std::shared_ptr<AbstractSharedHashTable> hashTable);
private:
    bool _preferAtomic = false;
    std::shared_ptr<AbstractSharedHashTable> _hashTable = nullptr;
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHBUILD_H_l
