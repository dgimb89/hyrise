// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/SharedHashBuild.h"
#include "access/HashBuild.h"
#include "io/shortcuts.h"
#include "storage/HashTable.h"
#include "testing/test.h"

namespace hyrise {
namespace access {

class HashBuildTests : public AccessTest {};

TEST_F(HashBuildTests, basic_hash_build_for_groupby_test) {
  auto t = io::Loader::shortcuts::load("test/10_30_group.tbl");

  HashBuild hb;
  hb.addInput(t);
  hb.addField(0);
  hb.setKey("groupby");
  hb.execute();

  const auto &result = std::dynamic_pointer_cast<const storage::SingleAggregateHashTable>(hb.getResultHashTable());

  ASSERT_NE(result.get(), (storage::SingleAggregateHashTable *) nullptr);
}

TEST_F(HashBuildTests, basic_shared_hash_build_for_groupby_test) {
#ifndef HYRISE_USE_FOLLY
  auto t = io::Loader::shortcuts::load("test/10_30_group.tbl");

  SharedHashBuild hb;
  auto map = std::make_shared<storage::shared_aggregate_single_hash_map_t>();
  hb.addInput(t);
  hb.addField(0);
  hb.setMap<storage::shared_aggregate_single_hash_map_t, storage::aggregate_single_key_t>(map);
  hb.execute();

  const auto &result = std::dynamic_pointer_cast<const storage::SingleAggregateSharedHashTableBase>(hb.getResultHashTable());

  ASSERT_NE(result.get(), (storage::SingleAggregateSharedHashTableBase *) nullptr);

  // has to be filled
  ASSERT_NE(result->size(), 0);
#endif
}

TEST_F(HashBuildTests, parallelized_shared_hash_build_for_groupby_test) {
#ifndef HYRISE_USE_FOLLY
  auto t = io::Loader::shortcuts::load("test/10_30_group.tbl");

  auto map1 = std::make_shared<storage::shared_aggregate_single_hash_map_t>();

  SharedHashBuild hb1;
  hb1.addInput(t);
  hb1.setCount(2);
  hb1.setPart(0);
  hb1.addField(0);
  hb1.setMap<storage::shared_aggregate_single_hash_map_t, storage::aggregate_single_key_t>(map1);

  SharedHashBuild hb2;
  hb2.addInput(t);
  hb2.setCount(2);
  hb2.setPart(1);
  hb2.addField(0);
  hb2.setMap<storage::shared_aggregate_single_hash_map_t, storage::aggregate_single_key_t>(map1);

  hb1.execute();
  hb2.execute();

  // non-parallelized shared hash build as reference
  auto map2 = std::make_shared<storage::shared_aggregate_single_hash_map_t>();
  SharedHashBuild hb;
  hb.addInput(t);
  hb.addField(0);
  hb.setMap<storage::shared_aggregate_single_hash_map_t, storage::aggregate_single_key_t>(map2);
  hb.execute();

  // has to be filled
  ASSERT_NE(map1->size(), 0);
  ASSERT_NE(map2->size(), 0);

  ASSERT_EQ(map1->size(), map2->size());
#endif
}

TEST_F(HashBuildTests, basic_hash_build_for_join_test) {
  auto t = io::Loader::shortcuts::load("test/10_30_group.tbl");

  HashBuild hb;
  hb.addInput(t);
  hb.addField(0);
  hb.setKey("join");
  hb.execute();

  const auto &result = hb.getResultHashTable();

  ASSERT_NE(result.get(), (storage::JoinHashTable *) nullptr);
}

}
}
