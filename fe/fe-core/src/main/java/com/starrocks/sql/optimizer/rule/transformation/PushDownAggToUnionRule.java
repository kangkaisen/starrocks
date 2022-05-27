// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PushDownAggToUnionRule extends TransformationRule {
    /* rewrite this rule.
        agg                    agg
         |                      |
         |                      |
       union      ----->      union
         |                      |
        / \                    / \
       /   \                  /   \
     olap  iceberg          agg   agg
                             |     |
                           olap  iceberg
    */
    public PushDownAggToUnionRule() {
        super(RuleType.TF_TESTXX, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOperator logicalOperator = (LogicalOperator) input.getOp();

        if (!(logicalOperator instanceof LogicalAggregationOperator)) {
            return Collections.emptyList(); 
        }
        return doWork(input, context.getColumnRefFactory());
    }

    public List<OptExpression> doWork(OptExpression input, ColumnRefFactory columnRefFactory) {
        LogicalAggregationOperator topAggOperator = (LogicalAggregationOperator) input.getOp();

        // union node
        OptExpression unionOptExpression = input.getInputs().get(0);
        LogicalUnionOperator oldUnionOperator = (LogicalUnionOperator) unionOptExpression.getOp();
        List<ColumnRefOperator> unionOutputColumns = oldUnionOperator.getOutputColumnRefOp();
        List<List<ColumnRefOperator>> childOutputColumns = oldUnionOperator.getChildOutputColumns();

        List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList();
        List<ColumnRefOperator> newChild1OutputColumns = Lists.newArrayList();
        List<ColumnRefOperator> newChild2OutputColumns = Lists.newArrayList();

        // olap, iceberg scan node
        OptExpression childOptExpression1 = unionOptExpression.getInputs().get(0);
        OptExpression childOptExpression2 = unionOptExpression.getInputs().get(1);

        // union's input column and input node.
        List<OptExpression> newInputs = Lists.newArrayList();

        // get children aggOp
        LogicalAggregationOperator child1AggOperator = getNewAggregationOperator(topAggOperator,
                unionOutputColumns, childOutputColumns.get(0), columnRefFactory, newChild1OutputColumns);

        Preconditions.checkState(unionOutputColumns.size() == newChild1OutputColumns.size());

        OptExpression newChildOptExpression1 = OptExpression.create(child1AggOperator, childOptExpression1);

        LogicalAggregationOperator child2AggOperator = getNewAggregationOperator(topAggOperator,
                unionOutputColumns, childOutputColumns.get(1), columnRefFactory, newChild2OutputColumns);
        OptExpression newChildOptExpression2 = OptExpression.create(child2AggOperator, childOptExpression2);

        Preconditions.checkState(unionOutputColumns.size() == newChild2OutputColumns.size());

        newInputs.add(newChildOptExpression1);
        newInputs.add(newChildOptExpression2);

        // for union input column
        newChildOutputColumns.add(newChild1OutputColumns);
        newChildOutputColumns.add(newChild2OutputColumns);

        LogicalUnionOperator newUnionOperator = new LogicalUnionOperator(oldUnionOperator.getOutputColumnRefOp(),
                newChildOutputColumns, true);

        OptExpression unionExpression = OptExpression.create(newUnionOperator, newInputs);
        return Lists.newArrayList(OptExpression.create(topAggOperator, unionExpression));
    }

    public LogicalAggregationOperator getNewAggregationOperator(LogicalAggregationOperator aggOp,
                                                                List<ColumnRefOperator> unionOutputColumns,
                                                                List<ColumnRefOperator> childOutputColumns,
                                                                ColumnRefFactory columnRefFactory,
                                                                List<ColumnRefOperator> newChildOutputColumns) {
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator fn = entry.getValue();
            
            if (fn.getChild(0) instanceof ColumnRefOperator) {
                ColumnRefOperator ref = (ColumnRefOperator) fn.getChild(0);
                int index = unionOutputColumns.indexOf(ref);
                Preconditions.checkState(index >= 0);

                ColumnRefOperator childColumn = columnRefFactory.getColumnRef(childOutputColumns.get(index).getId());

                CallOperator newFn = new CallOperator(fn.getFnName(), fn.getType(), Lists.newArrayList(childColumn), fn.getFunction(),
                            fn.isDistinct());

                ColumnRefOperator newColumn = columnRefFactory.create(ref.getName(),
                        entry.getKey().getType(), entry.getKey().isNullable());

                newAggregations.put(newColumn, newFn);
                if (!newChildOutputColumns.contains(newColumn)) {
                    newChildOutputColumns.add(index, newColumn);
                }
            } else {
                ColumnRefOperator ref = entry.getKey();
                ColumnRefOperator newColumn = columnRefFactory.create(ref.getName(),
                        entry.getKey().getType(), entry.getKey().isNullable());
                newAggregations.put(newColumn, entry.getValue());

                int index = unionOutputColumns.indexOf(ref);
                if (!newChildOutputColumns.contains(newColumn)) {
                    newChildOutputColumns.add(index, newColumn);
                }
            }
        }

        List<ColumnRefOperator> newGroupBy = Lists.newArrayList();
        for(ColumnRefOperator column: aggOp.getGroupingKeys()) {
            int index = unionOutputColumns.indexOf(column);
            Preconditions.checkState(index >= 0);
            ColumnRefOperator childColumn = columnRefFactory.getColumnRef(childOutputColumns.get(index).getId());
            newGroupBy.add(childColumn);
            if (!newChildOutputColumns.contains(childColumn)) {
                newChildOutputColumns.add(index, childColumn);
            }
        }

        List<ColumnRefOperator> newPartitions = Lists.newArrayList();
        for(ColumnRefOperator column: aggOp.getPartitionByColumns()) {
            int index = unionOutputColumns.indexOf(column);
            Preconditions.checkState(index >= 0);
            ColumnRefOperator childColumn = columnRefFactory.getColumnRef(childOutputColumns.get(index).getId());
            newPartitions.add(childColumn);
        }

        return new LogicalAggregationOperator(aggOp.getType(),
                newGroupBy,
                newPartitions,
                newAggregations,
                aggOp.isSplit(),
                aggOp.getSingleDistinctFunctionPos(),
                aggOp.getLimit(),
                aggOp.getPredicate());
    }
}