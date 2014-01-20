// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/SharedHashBuild.h"
#include "access/SharedHashTableGenerator.h"
#include "io/shortcuts.h"
#include "storage/SharedHashTable.h"
#include "testing/test.h"

namespace hyrise {
namespace access {

class SharedHashBuildTests : public AccessTest {};

TEST_F(SharedHashBuildTests, shared_hash_build_vname_test) {
    SharedHashBuild hb;
    ASSERT_STREQ("SharedHashBuild", hb.vname().c_str());
}

TEST_F(SharedHashBuildTests, shared_hash_table_generator_vname_test) {
    SharedHashTableGenerator hb;
    ASSERT_STREQ("SharedHashTableGenerator", hb.vname().c_str());
}

}
}
