#ifndef SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
#define SRC_LIB_ACCESS_SHAREDHASHBUILD_H_

#include "access/HashBuild.h"
#include "storage/SharedHashTable.h"

namespace hyrise {
namespace access {

//SharedHashGenerator (PlanOp) -> spawn instances of SharedHashBuild(ParPlanOp)
class SharedHashBuild : public ParallelizablePlanOperation {
public:
    virtual ~SharedHashBuild();
    virtual void executePlanOperation();
    virtual const std::string vname();

    template <template <typename, typename> class MAP_CONTAINER, typename KEY, typename VALUE>
    void setMap(std::shared_ptr<MAP_CONTAINER<KEY, VALUE>> map);
    void setKey(const std::string &key);

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
