// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(TestRule.class);

    private static final List<String> supportFuncs = Arrays.asList("sum", "min", "max", "avg");


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
    public TestRule() {
        super(RuleType.TF_TESTXX, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                        .addChildren(Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOperator logicalOperator = (LogicalOperator) input.getOp();
        // return Collections.emptyList(); 

        if (!(logicalOperator instanceof LogicalAggregationOperator)) {
            return Collections.emptyList(); 
        }
        return xxx(input);
    }

    public class GenerateColumnId {
        public int nextColumnId;

        public GenerateColumnId(int startId) {
            nextColumnId = startId;
        }

        public int getNextColumnId() {
            nextColumnId += 1;
            return nextColumnId;
        }
    }

    public List<OptExpression> xxx(OptExpression input) {
        LogicalAggregationOperator topAggOperator = (LogicalAggregationOperator) input.getOp();

        // union node
        OptExpression unionOptExpression = input.getInputs().get(0);

        // olap, iceberg scan node
        OptExpression childOptExpression1 = unionOptExpression.getInputs().get(0);
        OptExpression childOptExpression2 = unionOptExpression.getInputs().get(1);
        // scan node output column
        List<ColumnRefOperator> originScanNode1Column = ((LogicalScanOperator) childOptExpression1.getOp()).getOutputColumns();
        List<ColumnRefOperator> originScanNode2Column = ((LogicalScanOperator) childOptExpression2.getOp()).getOutputColumns();

        GenerateColumnId generateColumnId = new GenerateColumnId(
                ((LogicalOlapScanOperator) childOptExpression1.getOp()).getColumnMetaToColRefMap().size() * 2);

        // union's input column and input node.
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        List<OptExpression> newInputs = Lists.newArrayList();

        // get children aggOp
        LogicalAggregationOperator child1AggOperator = getNewAggregationOperator(topAggOperator,
                                                        originScanNode1Column, generateColumnId);

        OptExpression newChildOptExpression1 = OptExpression.create(child1AggOperator, childOptExpression1);

        LogicalAggregationOperator child2AggOperator = getNewAggregationOperator(topAggOperator,
                                                            originScanNode2Column, generateColumnId);
        OptExpression newChildOptExpression2 = OptExpression.create(child2AggOperator, childOptExpression2);

        newInputs.add(newChildOptExpression1);
        newInputs.add(newChildOptExpression2);

        // for union input column
        childOutputColumns.add(getAggOutputColumn(topAggOperator, child1AggOperator, generateColumnId, originScanNode1Column));
        childOutputColumns.add(getAggOutputColumn(topAggOperator, child2AggOperator, generateColumnId, originScanNode2Column));

        List<ColumnRefOperator> newUnionOutput = Lists.newArrayList();
        // LogicalUnionOperator  unionOperator = (LogicalUnionOperator) unionOptExpression.getOp();
        // for (int i = 0; i < unionOperator.getOutputColumnRefOp().size(); i++) {

        //     List<ColumnRefOperator> tmp = childOutputColumns.get(0);
        //     for (int j = 0; j < tmp.size(); j++) {
        //         if (unionOperator.getOutputColumnRefOp().get(i).getName().equals(tmp.get(j).getName())) {
        //             newUnionOutput.add(unionOperator.getOutputColumnRefOp().get(i));
        //             break;
        //         }
        //     }
        // }

        for (List<ColumnRefOperator> colList : childOutputColumns) {
            for (ColumnRefOperator col : colList) {
                newUnionOutput.add(getNewColumnRefOperatorWithNewId(col, generateColumnId.getNextColumnId()));   
            }
            break;
        }

        LogicalUnionOperator newUnionOperator = new LogicalUnionOperator(newUnionOutput,
                                                        childOutputColumns, true);

        OptExpression unionExpression = OptExpression.create(newUnionOperator, newInputs);
        return Lists.newArrayList(OptExpression.create(topAggOperator, unionExpression));
    }

    public boolean isExist(List<ColumnRefOperator> col, String name) {
        for (ColumnRefOperator c : col) {
            if (c.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    public List<ColumnRefOperator> getAggOutputColumn(LogicalAggregationOperator aggOp, LogicalAggregationOperator newAgg,
                        GenerateColumnId generateColumnId, List<ColumnRefOperator> childColumnRef) {
        List<ColumnRefOperator> newOutput = new ArrayList();
        Projection projection = aggOp.getProjection();
        if (projection != null) {
            List<ColumnRefOperator> columnRefList = new ArrayList<>(projection.getColumnRefMap().keySet());
            for (ColumnRefOperator col : columnRefList) {
                boolean flag = false;
                for (ColumnRefOperator chCol : childColumnRef) {
                    if (col.getName().equals(chCol.getName())) {
                        newOutput.add(chCol);
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    // this may be a column belong to this agg, not child's column.
                    newOutput.add(getNewColumnRefOperatorWithNewId(col, generateColumnId.getNextColumnId()));
                }
            }
            return newOutput;
        }

        if (newAgg.getAggregations().size() > 0) {
            for (ColumnRefOperator col : new ArrayList<>(newAgg.getAggregations().keySet())) {
                newOutput.add(col);
            }

            // NOTE: which column to use, and remove repeat column.
            for (ColumnRefOperator col : newAgg.getGroupingKeys()) {
                if (!isExist(newOutput, col.getName())) {
                    newOutput.add(col);
                }
            }
            return newOutput;
        }
        return newOutput;
    }

    public List<ColumnRefOperator> getColumnRefOperator(
                Map<Column, ColumnRefOperator> originColumnMeta, GenerateColumnId generateColumnId) {
        List<ColumnRefOperator> columnRefOperator = Lists.newArrayList();
        for (Column column : originColumnMeta.keySet()) {
            columnRefOperator.add(new ColumnRefOperator(
                    generateColumnId.getNextColumnId(),
                    originColumnMeta.get(column).getType(),
                    originColumnMeta.get(column).getName(),
                    originColumnMeta.get(column).isNullable()));
        }
        return columnRefOperator;
    }

    public ColumnRefOperator getNewColumnRefOperatorWithNewId(ColumnRefOperator columnRef, int id) {
        return new ColumnRefOperator(id, columnRef.getType(), columnRef.getName(), columnRef.isNullable());
    }

    public LogicalAggregationOperator getNewAggregationOperator(LogicalAggregationOperator aggOp,
                                                    List<ColumnRefOperator> columnRefList,
                                                    GenerateColumnId generateColumnId) {
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator fn = entry.getValue();
            
            if (fn.getChild(0) instanceof ColumnRefOperator) {
                ColumnRefOperator ref = (ColumnRefOperator) fn.getChild(0);

                ColumnRefOperator newRef = new ColumnRefOperator(getChildColumnId(columnRefList, ref.getName()), ref.getType(),
                        ref.getName(), ref.isNullable());

                CallOperator newFn = new CallOperator(fn.getFnName(), fn.getType(), Lists.newArrayList(newRef), fn.getFunction(),
                            fn.isDistinct());

                ColumnRefOperator newName = new ColumnRefOperator(generateColumnId.getNextColumnId(), entry.getKey().getType(),
                        ref.getName(), entry.getKey().isNullable());
                newAggregations.put(newName, newFn);
            } else {
                ColumnRefOperator ref = entry.getKey();
                ColumnRefOperator newName = new ColumnRefOperator(generateColumnId.getNextColumnId(), entry.getKey().getType(),
                        ref.getName(), entry.getKey().isNullable());
                newAggregations.put(newName, entry.getValue());
            }
        }

        LogicalAggregationOperator result = new LogicalAggregationOperator(aggOp.getType(),
                aggOp.getGroupingKeys().size() != 0 ? columnRefList : aggOp.getGroupingKeys(),
                aggOp.getPartitionByColumns().size() != 0 ? columnRefList : aggOp.getPartitionByColumns(),
                newAggregations,
                aggOp.isSplit(),
                aggOp.getSingleDistinctFunctionPos(),
                aggOp.getLimit(),
                aggOp.getPredicate());
        return result;
    }

    public boolean isSupportPushDownFunc(String funcName) {
        for (String name : supportFuncs) {
            if (name.equals(funcName)) {
                return true;
            }
        }
        return false;
    }

    public int getChildColumnId(List<ColumnRefOperator> columnRef, String columnName) {
        for (int i = 0; i < columnRef.size(); i++) {
            if (columnRef.get(i).getName().equals(columnName)) {
                return columnRef.get(i).getId();
            }
        }
        return 0;
    }
}