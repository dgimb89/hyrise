#ifndef SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
#define SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H

#include "access/SharedHashBuild.h"

namespace hyrise {
namespace access {

class SharedHashTableGenerator : public PlanOperation {
public:
    void executePlanOperation();
    static std::shared_ptr<PlanOperation> parse(const Json::Value &data);
    const std::string vname();
    void setNumberOfSpawns(const size_t number);

private:
    size_t m_numberOfSpawns;
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
