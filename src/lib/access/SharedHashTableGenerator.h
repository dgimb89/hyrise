#ifndef SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
#define SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H

#include "access/SharedHashBuild.h"

namespace hyrise {
namespace access {

class SharedHashTableGenerator : public PlanOperation {
public:
    void executePlanOperation();
    const std::string vname();

    void setNumberOfSpawns(const size_t number);
    size_t getNumberOfSpawns();

    void setKey(const std::string &key);

    static std::shared_ptr<PlanOperation> parse(const Json::Value &data);

protected:
    std::string _key;
    size_t _numberOfSpawns;
private:
    void addSharedHashtableResult(const storage::c_aresource_ptr_t& hashtable);
};

}
}

#endif // SRC_LIB_ACCESS_SHAREDHASHTABLEGENERATOR_H
