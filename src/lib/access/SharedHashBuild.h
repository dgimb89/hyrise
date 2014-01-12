#ifndef SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
#define SRC_LIB_ACCESS_SHAREDHASHBUILD_H_

#include "access/HashBuild.h"

namespace hyrise {
namespace access {

//SharedHashGenerator (PlanOp) -> spawn instances of SharedHashBuild(ParPlanOp)
template <class MAP, class KEY>
class SharedHashBuild : public ParallelizablePlanOperation {
public:
    typedef MAP map_t;
    typedef std::shared_ptr<map_t> map_ptr_t;
    typedef KEY key_t;
    virtual ~SharedHashBuild();
    virtual void executePlanOperation();
    virtual const std::string vname();

    void setMap(map_ptr_t map);
    map_ptr_t getMap();
protected:
    bool _preferAtomic = false;
    std::shared_ptr<map_t> _map = nullptr;
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
