// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ArrayElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitOlapToUnionWithIcebergRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(PlanFragmentBuilder.class);
    public static final SplitOlapToUnionWithIcebergRule OLAP_SCAN = new SplitOlapToUnionWithIcebergRule(
                                                                OperatorType.LOGICAL_OLAP_SCAN);

    public SplitOlapToUnionWithIcebergRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_SPLIT_OLAP_TO_UNION_WITH_ICEBERG, Pattern.create(logicalOperatorType));
    }

    public SplitOlapToUnionWithIcebergRule() {
        super(RuleType.TF_SPLIT_OLAP_TO_UNION_WITH_ICEBERG, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
        Table externalTable = getExternalTable(olapScanOperator, context);
        if (externalTable == null || scanOperator.isSplited() || !supportSplitWithIcebergScanNode(olapScanOperator)) {
            return Collections.emptyList();
        }

        Map<ColumnRefOperator, Column> originColumnRefMap = olapScanOperator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> originColumnMeta = olapScanOperator.getColumnMetaToColRefMap();
        // construct new olap node.
        Map<ColumnRefOperator, Column> olapColumnRef = new HashMap<ColumnRefOperator, Column>();
        Map<Column, ColumnRefOperator> olapColumnMeta = new HashMap<Column, ColumnRefOperator>();
        setColumnMetaToColRefMapForIceberg(originColumnMeta, originColumnRefMap, olapColumnMeta, olapColumnRef, false);
        // TODO: contruct new predicate for olap node.
        LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(olapScanOperator.getTable(),
                                        olapColumnRef, olapColumnMeta, olapScanOperator.getDistributionSpec(),
                                        olapScanOperator.getLimit(),
                                        constructOlapPredicate(olapScanOperator, olapColumnMeta, 
                                                context.getSessionVariable().isPreferComputeNode()),
                                        olapScanOperator.getSelectedIndexId(), olapScanOperator.getSelectedPartitionId(),
                                        olapScanOperator.getPartitionNames(), olapScanOperator.getSelectedTabletId(),
                                        olapScanOperator.getHintsTabletIds());

        // construct iceberg node, now only support split olap to union with olap+iceberg.
        Map<ColumnRefOperator, Column> icebergColumnRef = new HashMap<ColumnRefOperator, Column>();
        Map<Column, ColumnRefOperator> icebergColumnMeta = new HashMap<Column, ColumnRefOperator>();
        setColumnMetaToColRefMapForIceberg(originColumnMeta, originColumnRefMap, icebergColumnMeta, icebergColumnRef, true);
        LogicalIcebergScanOperator icebergScanOperator = new LogicalIcebergScanOperator(
                externalTable, externalTable.getType(), icebergColumnRef, icebergColumnMeta,
                olapScanOperator.getLimit(), 
                constructIcebergPredicate(olapScanOperator, icebergColumnMeta, 
                        context.getSessionVariable().isPreferComputeNode()));

        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        childOutputColumns.add(newScanOperator.getOutputColumns());
        childOutputColumns.add(icebergScanOperator.getOutputColumns());

        List<OptExpression> newInputs = Lists.newArrayList();
        // set flag for this olap scan node to avoid next split it.
        newScanOperator.setSplit(true);
        newInputs.add(new OptExpression(newScanOperator));
        newInputs.add(new OptExpression(icebergScanOperator));
        // NOTE: use origin outputColums for union node.
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(
                newScanOperator.getOutputColumns(), childOutputColumns, true);
        return Lists.newArrayList(OptExpression.create(unionOperator, newInputs));
    }

    public boolean supportSplitWithIcebergScanNode(LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        // TODO: use partitionColumn to check this condition.
        Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
        for (Long id : idToRange.keySet()) {
            PartitionKey key = idToRange.get(id).lowerEndpoint();
            List<LiteralExpr> keys = key.getKeys();
            // now only support partition have one column.
            if (keys.size() > 1) {
                return false;
            }
            // now only support rangePartitionInfo with date/datetime column
            if (!(keys.get(0) instanceof DateLiteral)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get linked external table, now only support iceberg table.
     * TODO: check table type.
     */
    public Table getExternalTable(LogicalOlapScanOperator olapScanOperator, OptimizerContext context) {
        String externalTable = ((OlapTable) olapScanOperator.getTable()).getExternalTable();
        if (externalTable == null || externalTable.equals("")) {
            return null;
        }
        String[] split = externalTable.split("\\.");
        Preconditions.checkState(split.length == 2);
        Database database = context.getCatalog().getDb(new StringBuilder(64)
                                                            .append("default_cluster:")
                                                            .append(split[0]).toString());
        Preconditions.checkState(database != null);
        Table icebergTable = database.getTable(split[1]);
        Preconditions.checkState(icebergTable != null);
        return icebergTable;
    }

    /**
     * Get olap's partiton column name, now only support column's type is datetime.
     */
    public String getPartitionName(LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        List<Column> partitionColumns = ((RangePartitionInfo) olapTable.getPartitionInfo()).getPartitionColumns();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getType().isDatetime()) {
                return partitionColumns.get(i).getName();
            }
        }
        return null;
    }

    /**
     * Construct iceberg predicate and add time predicate out of partition range.
     * for example:
     * p20220420 VALUES [('2022-04-20 00:00:00'), ('2022-04-21 00:00:00'))
     * p20220425 VALUES [('2022-04-25 00:00:00'), ('2022-04-26 00:00:00'))
     * p20220427 VALUES [('2022-04-27 00:00:00'), ('2022-04-28 00:00:00'))
     * iceberg's predicate must out of this range.
     */
    public ScalarOperator constructIcebergPredicate(LogicalOlapScanOperator olapScanOperator,
                                                    Map<Column, ColumnRefOperator> icebergColumnMeta,
                                                    boolean preferComputeNode) {
        // get partition column name, now only support datetime partition.
        String partitionColumnName = getPartitionName(olapScanOperator);
        Preconditions.checkState(partitionColumnName != null);
        int columnId = getColumnId(icebergColumnMeta, partitionColumnName);

        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
        List<TimeSequence> timeSequenceList = new ArrayList<TimeSequence>();
        for (Long id : idToRange.keySet()) {
            Partition partition = olapTable.getPartition(id);
            // this partition has been synced to remote storage and has preferComputeNode falg.
            if (partition.getVisibleVersionTime() <= rangePartitionInfo.getColdDownSyncedTimeMs(id) &&
                        rangePartitionInfo.getColdDownSyncedTimeMs(id) > 0 && preferComputeNode) {
                continue;
            }
            timeSequenceList.add(new TimeSequence(
                    getDatetimeFromPartitionInfo(idToRange.get(id).lowerEndpoint()),
                    getDatetimeFromPartitionInfo(idToRange.get(id).upperEndpoint())));
        }
        Collections.sort(timeSequenceList);
        List<ScalarOperator> predicateList = new ArrayList<ScalarOperator>();

        for (int i = 0; i < timeSequenceList.size(); i++) {
            if (i == 0) {
                BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                        getPredicateArguments(columnId, partitionColumnName, timeSequenceList.get(i).start));
                predicateList.add(leftPredicate);
            }
            String end = timeSequenceList.get(i).end;
            for (i++; i < timeSequenceList.size(); ) {
                String nextStart = timeSequenceList.get(i).start;
                if (end.compareTo(nextStart) >= 0) {
                    end = timeSequenceList.get(i).end;
                    i++;
                    continue;
                }
                predicateList.add(getAndPredicate(end, nextStart, columnId, partitionColumnName));
                break;
            }
            if (i == timeSequenceList.size()) {
                BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                        getPredicateArguments(columnId, partitionColumnName, end));
                predicateList.add(leftPredicate);
            }
        }
        // connect this predicates.
        ScalarOperator partitionOrPredicate = Utils.compoundOr(predicateList);
        ScalarOperator copyPredicate = null;
        if (olapScanOperator.getPredicate() != null) {
            copyPredicate = ((PredicateOperator) olapScanOperator.getPredicate()).clone();
        }
        modifyPredicateColumnId(copyPredicate, icebergColumnMeta);

        ScalarOperator predicates = Utils.compoundAnd(partitionOrPredicate, copyPredicate);
        return predicates;
    }

    /*
     * Construct olap predicate and add time predicate in of partition range which not synced to
     * remote storage.
    */
    public ScalarOperator constructOlapPredicate(LogicalOlapScanOperator olapScanOperator,
                                                    Map<Column, ColumnRefOperator> olapColumnMeta,
                                                    boolean preferComputeNode) {
        // get partition column name, now only support datetime partition.
        String partitionColumnName = getPartitionName(olapScanOperator);
        Preconditions.checkState(partitionColumnName != null);
        int columnId = getColumnId(olapColumnMeta, partitionColumnName);
        List<ScalarOperator> predicateList = new ArrayList<ScalarOperator>();

        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
        List<TimeSequence> timeSequenceList = new ArrayList<TimeSequence>();

        for (Long id : idToRange.keySet()) {
            Partition partition = olapTable.getPartition(id);
            // this partition has been synced to remote storage and has preferComputeNode falg.
            if (partition.getVisibleVersionTime() <= rangePartitionInfo.getColdDownSyncedTimeMs(id) &&
                        rangePartitionInfo.getColdDownSyncedTimeMs(id) > 0 && preferComputeNode) {
                continue;
            }
            timeSequenceList.add(new TimeSequence(
                    getDatetimeFromPartitionInfo(idToRange.get(id).lowerEndpoint()),
                    getDatetimeFromPartitionInfo(idToRange.get(id).upperEndpoint())));
        }
        Collections.sort(timeSequenceList);
        for (int i = 0; i < timeSequenceList.size(); ) {
            BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                        getPredicateArguments(columnId, partitionColumnName, timeSequenceList.get(i).start));

            String end = timeSequenceList.get(i).end;
            for (i++; i < timeSequenceList.size(); ) {
                String nextStart = timeSequenceList.get(i).start;
                if (end.compareTo(nextStart) >= 0) {
                    end = timeSequenceList.get(i).end;
                    i++;
                    continue;
                }
                BinaryPredicateOperator rightPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                        getPredicateArguments(columnId, partitionColumnName, end));

                predicateList.add(Utils.compoundAnd(leftPredicate, rightPredicate));
                break;
            }
            if (i == timeSequenceList.size()) {
                BinaryPredicateOperator rightPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                        getPredicateArguments(columnId, partitionColumnName, end));
                predicateList.add(Utils.compoundAnd(leftPredicate, rightPredicate));
            }
        }
        // connect this predicates.
        ScalarOperator partitionOrPredicate = Utils.compoundOr(predicateList);
        ScalarOperator predicates = Utils.compoundAnd(partitionOrPredicate, olapScanOperator.getPredicate());
        return predicates;
    }

    public ScalarOperator getAndPredicate(String left, String right, int columnId, String partitionColumnName) {
        BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                getPredicateArguments(columnId, partitionColumnName, left));
        BinaryPredicateOperator rightPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                getPredicateArguments(columnId, partitionColumnName, right));
        return Utils.compoundAnd(leftPredicate, rightPredicate);
    }

    public List<ScalarOperator> getPredicateArguments(int columnId, String partitionColumnName, String value) {
        List<ScalarOperator> newArguments = new ArrayList<ScalarOperator>();
        newArguments.add(new ColumnRefOperator(columnId, Type.DATETIME, partitionColumnName, false));
        newArguments.add(ConstantOperator.createDatetime(LocalDateTime.parse(value,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), Type.DATETIME));
        return newArguments;
    }

    public String getDatetimeFromPartitionInfo(PartitionKey key) {
        List<LiteralExpr> keys = key.getKeys();
        String strTime = null;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) instanceof DateLiteral) {
                strTime = ((DateLiteral) keys.get(i)).getStringValue();
                break;
            }
        }
        Preconditions.checkState(strTime != null);
        return strTime;
    }

    public class TimeSequence implements Comparable<TimeSequence> {
        public String start;
        public String end;

        public TimeSequence(String start, String end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(TimeSequence timeSequence) {
            if (this.start.compareTo(timeSequence.start) == 0) {
                return this.end.compareTo(timeSequence.end);
            }
            return this.start.compareTo(timeSequence.start);
        }
    }

    public void setColumnMetaToColRefMapForIceberg(
            Map<Column, ColumnRefOperator> originColumnMeta,
            Map<ColumnRefOperator, Column> originColumnRef,
            Map<Column, ColumnRefOperator> icebergColumnMeta,
            Map<ColumnRefOperator, Column> icebergColumnRef,
            boolean increaseColumnId) {
        // keep ColumnMeta variable have all column info, because the other rule is also like this.
        int mapSize = increaseColumnId ? originColumnMeta.size() : 0;
        for (Column column : originColumnMeta.keySet()) {
            ColumnRefOperator columnRef = originColumnMeta.get(column);
            icebergColumnMeta.put(column, new ColumnRefOperator(
                    originColumnMeta.get(column).getId() + mapSize,
                    originColumnMeta.get(column).getType(),
                    originColumnMeta.get(column).getName(),
                    originColumnMeta.get(column).isNullable()));

            icebergColumnRef.put(icebergColumnMeta.get(column), column);
        }
    }

    /**
     * Construct new scan node, it's column id in predicate must be change.
     */
    public void modifyPredicateColumnId(ScalarOperator copyPredicate, 
                        Map<Column, ColumnRefOperator> columnMeta) {
        if (copyPredicate == null) {
            return;
        }
        for (int i = 0; i < copyPredicate.getChildren().size(); i++) {
            ScalarOperator child = copyPredicate.getChild(i);
            if (child instanceof ColumnRefOperator) {
                ColumnRefOperator columnRefOperator = (ColumnRefOperator) copyPredicate.getChild(i);
                copyPredicate.setChild(i, new ColumnRefOperator(
                            getColumnId(columnMeta, columnRefOperator.getName()),
                            columnRefOperator.getType(),
                            columnRefOperator.getName(),
                            columnRefOperator.isNullable()));
            } else if (child instanceof PredicateOperator) {
                modifyPredicateColumnId(child, columnMeta);
            } else if (child instanceof CallOperator) {
                modifyPredicateColumnId(child, columnMeta);
            } else if (child instanceof ArrayElementOperator) {
                modifyPredicateColumnId(child, columnMeta);
            } else if (child instanceof ArrayOperator || child instanceof ArraySliceOperator) {
                modifyPredicateColumnId(child, columnMeta);
            }
        }
    }

    /**
     * Get new column id from columnMeta map depend on column name.
     * because column id in new scan node must difference with children of a common parent.
     */
    public int getColumnId(Map<Column, ColumnRefOperator> columnMeta, String name) {
        int columnId = 0;
        for (Column column : columnMeta.keySet()) {
            if (name.equals(columnMeta.get(column).getName())) {
                return columnMeta.get(column).getId();
            }
        }
        Preconditions.checkState(columnId > 0);
        return columnId;
    }
}