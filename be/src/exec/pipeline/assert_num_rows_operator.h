// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
class AssertNumRowsOperator final : public Operator {
public:
    AssertNumRowsOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int64_t& desired_num_rows,
                          const std::string& subquery_string, const TAssertion::type& assertion,
                          const std::vector<SlotDescriptor*>& slots)
            : Operator(factory, id, "assert_num_rows_sink", plan_node_id),
              _has_assert(false),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(std::move(assertion)),
              _slots(slots) {}

    ~AssertNumRowsOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool need_input() const override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void set_finishing(RuntimeState* state) override;
    bool is_finished() const override;

private:
    bool _has_assert;
    bool _input_finished = false;
    int64_t _actual_num_rows = 0;

    const int64_t& _desired_num_rows;
    const std::string& _subquery_string;
    const TAssertion::type& _assertion;
    vectorized::ChunkPtr _cur_chunk = nullptr;
    const std::vector<SlotDescriptor*>& _slots;
};

class AssertNumRowsOperatorFactory final : public OperatorFactory {
public:
    AssertNumRowsOperatorFactory(int32_t id, int32_t plan_node_id, int64_t desired_num_rows,
                                 const std::string& subquery_string, TAssertion::type&& assertion,
                                 const std::vector<SlotDescriptor*>& slots)
            : OperatorFactory(id, "assert_num_rows_sink", plan_node_id),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(std::move(assertion)),
              _slots(slots) {}

    ~AssertNumRowsOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AssertNumRowsOperator>(this, _id, _plan_node_id, _desired_num_rows, _subquery_string,
                                                       _assertion, _slots);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    int64_t _desired_num_rows;
    std::string _subquery_string;
    TAssertion::type _assertion;
    const std::vector<SlotDescriptor*>& _slots;
};

} // namespace pipeline
} // namespace starrocks
