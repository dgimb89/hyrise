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

TEST_F(SharedHashBuildTests, basic_hash_table_generator_test) {
    auto t = Loader::shortcuts::load("test/10_30_group.tbl");

    SharedHashTableGenerator htg;
    htg.setNumberOfSpawns(4);
    htg.addInput(t);
    htg.addField(0);
    htg.setKey("groupby");
    htg.execute();

    /*ASSERT_NE(htg.getResultHashTable(0).get(), (AbstractHashTable*) nullptr);
    ASSERT_NE(htg.getResultHashTable(1).get(), (AbstractHashTable*) nullptr);
    ASSERT_NE(htg.getResultHashTable(2).get(), (AbstractHashTable*) nullptr);
    ASSERT_NE(htg.getResultHashTable(3).get(), (AbstractHashTable*) nullptr);*/
}


TEST_F(SharedHashBuildTests, basic_shared_hash_build_for_groupby_test) {
  /*auto t = Loader::shortcuts::load("test/10_30_group.tbl");

  SharedHashBuild hb;
  hb.addInput(t);
  hb.addField(0);
  hb.setKey("groupby");
  hb.execute();

  const auto &result = std::dynamic_pointer_cast<const SingleAggregateLockingSharedHashTable>(hb.getResultHashTable());

  ASSERT_NE(result.get(), (SingleAggregateLockingSharedHashTable *) nullptr);*/
}

TEST_F(SharedHashBuildTests, parrallel_shared_hash_build_operate_on_same_map_test) {
    /*SharedHashBuild hb1;
    hb1.setCount(2);*/
}

}
}
