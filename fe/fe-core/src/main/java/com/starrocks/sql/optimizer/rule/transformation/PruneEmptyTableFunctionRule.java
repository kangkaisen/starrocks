// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneEmptyTableFunctionRule extends TransformationRule{
    public PruneEmptyTableFunctionRule() {
        super(RuleType.TF_PRUNE_EMPTY_TABLE_FUNCTION, Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator tableOperator = (LogicalTableFunctionOperator) input.getOp();

        if (tableOperator.getFnResultColRefs().isEmpty()) {
            return input.getInputs();
        }

        return Collections.emptyList();
    }
}
