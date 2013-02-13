// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "NestedLoopEquiJoin.h"
#include "storage/MutableVerticalTable.h"
#include "QueryParser.h"
#include "storage/PointerCalculatorFactory.h"
#include "storage/PointerCalculator.h"
#include "access/BasicParser.h"
#include <chrono>
#include <ctime>

namespace hyrise {
namespace access {

bool NestedLoopEquiJoin::is_registered = QueryParser::registerPlanOperation<NestedLoopEquiJoin>();

std::shared_ptr<_PlanOperation> NestedLoopEquiJoin::parse(Json::Value &data) {
  auto instance = BasicParser<NestedLoopEquiJoin>::parse(data);
  instance->setBits1(data["bits1"].asUInt());
  instance->setBits2(data["bits2"].asUInt());
  if (data.isMember("partitions")) {
    for (unsigned i = 0; i < data["partitions"].size(); ++i) {
      instance->addPartition(data["partitions"][i].asInt());
    }
  }
  return instance;
}
void NestedLoopEquiJoin::executePlanOperation() {
  // get the two input tables and its hashmaps
  // First part is the probe table
  auto left = input.getTable(0);
  auto hleft = input.getTable(1);
  auto pleft = input.getTable(2);
  size_t left_size = left->size();

  // Now fetch the hash table
  auto right = input.getTable(3);
  auto hright = input.getTable(4);
  auto pright = input.getTable(5);
  size_t right_size = right->size();

  // cast down left hash table, first column contains hashes (value_id_t), second column contains pos_t
  const auto& lavs0 = hleft->getAttributeVectors(0);
  const auto& lhvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(lavs0.at(0).attribute_vector);
  const auto& lavs1 = hleft->getAttributeVectors(1);
  const auto& lpvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(lavs1.at(0).attribute_vector);
  // cast down left prefix sum table
  const auto& lavsp = pleft->getAttributeVectors(0);
  const auto& lprefixvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(lavsp.at(0).attribute_vector);
  size_t lprefixvector_size = lprefixvector->size();

  // cast down right hash table
  const auto& ravs0 = hright->getAttributeVectors(0);
  const auto& rhvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(ravs0.at(0).attribute_vector);
  const auto& ravs1 = hright->getAttributeVectors(1);
  const auto& rpvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(ravs1.at(0).attribute_vector);
  // cast down right first prefix sum table
  const auto& ravsp = pright->getAttributeVectors(0);
  const auto& rprefixvector = std::dynamic_pointer_cast<FixedLengthVector<value_id_t>>(ravsp.at(0).attribute_vector);
  size_t rprefixvector_size = rprefixvector->size();

  // Prepare mask for right prefix sum 
  register auto mask1 = (1 << bits1()) - 1;
  register auto mask2 = ((1 << bits2()) - 1) << bits1();
  // join
  size_t left_begin = 0, left_end = 0, right_begin = 0, right_end = 0, partition = 0, rpart = 0;
  register uint32_t lhash;
  auto lpos_list = new pos_list_t;
  auto rpos_list = new pos_list_t;
  
  lpos_list->reserve(lhvector->size());
  rpos_list->reserve(lhvector->size());


  register auto multiplier = 1 << bits2();
  register auto shift_right = bits1();
  
  // iterate over partitions -> partition gives the offset into the prefix table
  // The number of partitions depends on the number of bits used for the partitioning
  for(size_t i = 0, partitions_count = _partitions.size(); i < partitions_count; i++){
    partition = _partitions[i];
    
    // get the corresponding rows for partition using the left prefix table
    left_begin = lprefixvector->get(0, partition);
    if((partition == lprefixvector_size - 1)) //|| (i+1) == _partitions.size())
      left_end = left_size;
    else
      left_end = lprefixvector->get(0,partition +1);
    
    // iterate over values in partition
    for (size_t left_row = left_begin; left_row < left_end; left_row++) {   
      // find corresponding values of the right relation using the left hash value
      lhash = lhvector->get(0, left_row);

      // determine partition in right prefix
      rpart = ((lhash & mask1) * (multiplier)) + ((lhash & mask2) >> shift_right);
      right_begin = rprefixvector->get(0, rpart);
      right_end = rpart < (rprefixvector_size - 1) ? rprefixvector->get(0,rpart +1) : right_size;
        
      // iterate over all potentially matching positions in right relation
      for (size_t right_row = right_begin; right_row < right_end; right_row++) {
	// compare hashes
        if (lhash == rhvector->get(0, right_row)) {
          // remeber matching positions
          lpos_list->push_back(lpvector->get(0, left_row));
          rpos_list->push_back(rpvector->get(0, right_row));
        }
      }
    }
  }

  
  // create PointerCalculator and pos_lists for output
  auto loutput = PointerCalculatorFactory::createPointerCalculator(left, nullptr, lpos_list);
  auto routput = PointerCalculatorFactory::createPointerCalculator(right, nullptr, rpos_list);

  // build output table
  std::vector<hyrise::storage::atable_ptr_t > vc;
  vc.push_back(loutput);
  vc.push_back(routput);

  hyrise::storage::atable_ptr_t result = std::make_shared<MutableVerticalTable>(vc);
  addResult(result);
}

}
}
