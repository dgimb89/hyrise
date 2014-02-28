#pragma once

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

    /// Reacts to
    /// fields in either integer-list notation or std::string-list notation
    /// When given to a GroupByScan make sure to set the sharedHT option to true
    /// Number of spawned tasks can be set by "numCores"
    /// {
    ///     "operators": {
    ///         "0": {
    ///              "type": "TableLoad",
    ///              "table": "table1",
    ///              "filename": "..."
    ///         },
    ///         "1": {
    ///             "type": "SharedHashTableGenerator",
    ///             "numCores": 4,
    ///             "fields": ["employee_company_id"],
    ///             "key": "groupby"
    ///         },
    ///          "2": {
    ///              "type": "GroupByScan",
    ///              "fields": [1],
    ///              "sharedHT": true
    ///          }
    ///      },
    ///      "edges": [["0", "1"], ["0", "2"], ["1", "2"]]
    ///  }
    static std::shared_ptr<PlanOperation> parse(const Json::Value &data);

protected:
    std::string _key;
    size_t _numberOfSpawns;
private:
    void addSharedHashtableResult(const storage::c_aresource_ptr_t& hashtable);
};

}
}
