#ifndef SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
#define SRC_LIB_ACCESS_SHAREDHASHBUILD_H_

#include "access/HashBuild.h"

namespace hyrise {
namespace access {

class SharedHashBuild : public HashBuild {
public:
    virtual ~SharedHashBuild();
    virtual void executePlanOperation();
    virtual const std::string vname();
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHBUILD_H_
