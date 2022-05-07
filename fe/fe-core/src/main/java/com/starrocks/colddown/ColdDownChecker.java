// This file is made available under Elastic License 2.0.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.colddown;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.ExportStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MetaObject;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.Config;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.external.iceberg.IcebergPartitionMgr;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.exportcallback.PartitionColdDownExportCallback;
import com.starrocks.qe.ConnectContext;
import com.starrocks.system.SystemInfoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ColdDownChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ColdDownChecker.class);

    private static final int MAX_JOB_NUM = 16;

    private static final Comparator<MetaObject> COMPARATOR =
            (first, second) -> Long.signum(first.getLastCheckTime() - second.getLastCheckTime());

    // tabletId -> job
    private Map<String, Boolean> jobs;

    /*
     * ATTN:
     *      lock order is:
     *       jobs lock
     *       CheckConsistencyJob's synchronized
     *       db lock
     *
     * if reversal is inevitable. use db.tryLock() instead to avoid dead lock
     */
    private final ReentrantReadWriteLock jobsLock;

    private int startTime;
    private int endTime;

    public ColdDownChecker() {
        super("cold down checker");

        jobs = Maps.newHashMap();
        jobsLock = new ReentrantReadWriteLock();

        if (!initWorkTime()) {
            LOG.error("failed to init time in ColdDownChecker. exit");
            System.exit(-1);
        }
    }

    private boolean initWorkTime() {
        Date startDate = TimeUtils.getTimeAsDate(Config.cold_down_check_start_time);
        Date endDate = TimeUtils.getTimeAsDate(Config.cold_down_check_end_time);

        if (startDate == null || endDate == null) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(startDate);
        startTime = calendar.get(Calendar.HOUR_OF_DAY);

        calendar.setTime(endDate);
        endTime = calendar.get(Calendar.HOUR_OF_DAY);
        return true;
    }

    @Override
    protected void runAfterCatalogReady() {
        // for each round. try chose enough new partitions to check
        // only add new job when it's old then visible time
        if (itsTime() && getJobNum() < MAX_JOB_NUM) {
            long remainJobs = MAX_JOB_NUM - getJobNum();
            // clear terminated jobs
            ExportMgr exportMgr = Catalog.getCurrentCatalog().getExportMgr();
            List<ExportJob> exportJobs1 = exportMgr.getExportJobs(ExportJob.JobState.PENDING);
            List<ExportJob> exportJobs2 = exportMgr.getExportJobs(ExportJob.JobState.EXPORTING);
            List<ExportJob> unfinishedJobs = Stream.
                    concat(exportJobs1.stream(), exportJobs2.stream()).
                    sorted((job1, job2) -> {
                        long l = job1.getFinishTimeMs() - job2.getFinishTimeMs();
                        return l == 0 ? 0 : l > 0 ? 1 : -1;
                    }).collect(Collectors.toList());
            jobsLock.writeLock().lock();
            try {
                Map<String, Boolean> unfinishedJobsMap = Maps.newHashMap();
                for (ExportJob job : unfinishedJobs) {
                    if (!job.getExportType().equalsIgnoreCase("partition_cold_down")) {
                        continue;
                    }
                    TableName tableName = job.getTableName();
                    List<String> partitions = job.getPartitions();
                    String key = String.format("%s-%s-%s", tableName.getDb(), tableName.getTbl(), partitions.get(0));
                    unfinishedJobsMap.put(key, true);
                }
                this.jobs = unfinishedJobsMap;
            } finally {
                jobsLock.writeLock().lock();
            }
            List<ChosenPartition> chosenPartitions = choosePartitions(remainJobs);
            for (ChosenPartition chosenPartition : chosenPartitions) {
                jobsLock.writeLock().lock();
                try {
                    ColdDownJob job = new ColdDownJob(
                            chosenPartition.getDatabase(),
                            chosenPartition.getTable(),
                            chosenPartition.getPartition(),
                            chosenPartition.getExternalTable()
                    );
                    boolean ok = addJob(job);
                    if (!ok) {
                        LOG.warn(String.format("add cold down job [%s] failed", job));
                    }
                } finally {
                    jobsLock.writeLock().unlock();
                }
            }
        }
    }

    /*
     * check if time comes
     */
    private boolean itsTime() {
        if (startTime == endTime) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        int currentTime = calendar.get(Calendar.HOUR_OF_DAY);

        boolean isTime = false;
        if (startTime < endTime) {
            if (currentTime >= startTime && currentTime <= endTime) {
                isTime = true;
            } else {
                isTime = false;
            }
        } else {
            // startTime > endTime (across the day)
            if (currentTime >= startTime || currentTime <= endTime) {
                isTime = true;
            } else {
                isTime = false;
            }
        }

        if (!isTime) {
            LOG.debug("current time is {}:00, waiting to {}:00 to {}:00",
                    currentTime, startTime, endTime);
        }

        return isTime;
    }

    private boolean addJob(ColdDownJob job) {
        this.jobsLock.writeLock().lock();
        try {
            if (jobs.containsKey(job.getKey())) {
                return false;
            } else {
                LOG.info("add tablet[{}] to check consistency", job.getKey());
                jobs.put(job.getKey(), true);
                addJobReal(job);
                return true;
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    private boolean addJobReal(ColdDownJob job) {
        List<String> nameList = Collections.singletonList(job.getPartition());
        PartitionNames partitionNames = new PartitionNames(false, nameList);
        TableName tableName = new TableName(job.getDatabase(), job.getTable());
        TableRef tableRef = new TableRef(tableName, null, partitionNames);
        String externalTable = job.getExternalTable();
        org.apache.iceberg.Table icebergTable = PartitionColdDownExportCallback.getIcebergTable(externalTable);
        if (icebergTable == null) {
            LOG.warn(String.format("get iceberg table for %s get null", externalTable));
            return false;
        }
        String location = icebergTable.location();
        String exportTo = String.format("%s/data/%s/", location, job.getPartition());
        try {
            Configuration conf = IcebergPartitionMgr.get_hadoop_conf();
            Map<String, String> brokerProperties = avro.shaded.com.google.common.collect.Maps.newHashMap();
            brokerProperties.put("fs.s3a.access.key", conf.get("fs.s3a.access.key"));
            brokerProperties.put("fs.s3a.secret.key", conf.get("fs.s3a.secret.key"));
            URI uri = new URI(conf.get("fs.s3a.endpoint"));
            brokerProperties.put("fs.s3a.endpoint", uri.getHost());
            brokerProperties.put("fs.s3a.etag.checksum", "false");
            BrokerDesc broker = new BrokerDesc("broker1", brokerProperties);
            Map<String, String> properties = avro.shaded.com.google.common.collect.Maps.newHashMap();
            properties.put("file_format", "orc");
            properties.put("export_type", "partition_cold_down");
            ExportStmt exportStmt = new ExportStmt(tableRef, null, exportTo, properties, broker);
            ConnectContext ctx = new ConnectContext();
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            ctx.setThreadLocalInfo();
            Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), ctx);
            exportStmt.analyze(analyzer);
            LOG.info(String.format("finish generate export job for partition cold down, job stmt:[%s]", exportStmt.toSql()));
            Catalog.getCurrentCatalog().getExportMgr().addExportJob(UUIDUtil.genUUID(), exportStmt);
        } catch (Exception e) {
            LOG.error(String.format("add cold down job %s %s failed, %s", tableName, partitionNames, e));
        }
        LOG.info(String.format("add cold down job for %s %s success", tableName, partitionNames));
        return true;
    }

    private int getJobNum() {
        this.jobsLock.readLock().lock();
        try {
            return jobs.size();
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    public static class ChosenPartition {
        private final String database;
        private final String table;
        private final String partition;
        private final String externalTable;

        public ChosenPartition(String database, String table, String partition, String externalTable) {
            this.database = database;
            this.table = table;
            this.partition = partition;
            this.externalTable = externalTable;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public String getPartition() {
            return partition;
        }

        public String getExternalTable() {
            return externalTable;
        }
    }

    /**
     * choose a tablet to check it's consistency
     * we use a priority queue to sort db/table/partition/index/tablet by 'lastCheckTime'.
     * chose a tablet which has the smallest 'lastCheckTime'.
     */
    private List<ChosenPartition> choosePartitions(long remainJobs) {
        Catalog catalog = Catalog.getCurrentCatalog();
        MetaObject chosenOne = null;

        List<ChosenPartition> chosenPartitions = Lists.newArrayList();

        // sort dbs
        List<Long> dbIds = catalog.getDbIds();
        if (dbIds.isEmpty()) {
            return chosenPartitions;
        }
        Queue<MetaObject> dbQueue = new PriorityQueue<>(Math.max(dbIds.size(), 1), COMPARATOR);
        for (Long dbId : dbIds) {
            if (dbId == 0L) {
                // skip 'information_schema' database
                continue;
            }
            Database db = catalog.getDb(dbId);
            if (db == null) {
                continue;
            }
            dbQueue.add(db);
        }

        // must lock jobsLock first to obey the lock order rule
        this.jobsLock.readLock().lock();
        try {
            while ((chosenOne = dbQueue.poll()) != null) {
                Database db = (Database) chosenOne;
                db.readLock();
                try {
                    // sort tables
                    List<Table> tables = db.getTables();
                    Queue<MetaObject> tableQueue = new PriorityQueue<>(Math.max(tables.size(), 1), COMPARATOR);
                    for (Table table : tables) {
                        if (table.getType() != TableType.OLAP) {
                            continue;
                        }
                        tableQueue.add(table);
                    }

                    while ((chosenOne = tableQueue.poll()) != null) {
                        OlapTable table = (OlapTable) chosenOne;

                        Map<String, String> tableProperties = table.getTableProperty().getProperties();
                        String externalTable = tableProperties.getOrDefault("external_table", null);
                        String coldDownWaitSeconds = tableProperties.getOrDefault("colddown_wait_seconds", null);
                        if (externalTable == null || externalTable.isEmpty()) {
                            LOG.debug("table[{}] has no `external_table` property. ignore", table.toString());
                            continue;
                        }
                        if (coldDownWaitSeconds == null || coldDownWaitSeconds.isEmpty()) {
                            LOG.debug("table[{}] has no `colddown_wait_seconds` property. ignore", table.toString());
                            continue;
                        }
                        long coldDownWaitSecondsLong;
                        try {
                            coldDownWaitSecondsLong = Long.parseLong(coldDownWaitSeconds, 10);
                        } catch (NumberFormatException e) {
                            LOG.warn(String.format("property `colddown_wait_seconds` value should be integer, %s", e));
                            continue;
                        }
                        long coldDownWaitMillis = coldDownWaitSecondsLong * 1000;

                        PartitionInfo partitionInfo = table.getPartitionInfo();
                        for (Partition partition : table.getPartitions()) {
                            // check if this partition has changed since last cold down
                            long partitionColdDownTimeMs = partitionInfo.getColdDownSyncedTimeMs(partition.getId());
                            if (partitionColdDownTimeMs >= partition.getVisibleVersionTime()) {
                                LOG.debug("partition[{}]'s version is {}. ignore", partition.getId(),
                                        Partition.PARTITION_INIT_VERSION);
                                continue;
                            }
                            long changedMillis = System.currentTimeMillis() - partition.getVisibleVersionTime();
                            if (changedMillis <= coldDownWaitMillis) {
                                LOG.debug("partition[{}]'s changed time hasn't reach " +
                                        "colddown_wait_seconds {}. ignore", partition.getId(),
                                        coldDownWaitSeconds);
                                continue;
                            }
                            if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION) {
                                LOG.debug("partition[{}]'s is init version. ignore", partition.getId());
                                continue;
                            }

                            LOG.info("chose partition[{}-{}-{}] to cold down", db.getId(),
                                    table.getId(), partition.getId());
                            ChosenPartition chosenPartition = new ChosenPartition(
                                    db.getFullName(),
                                    table.getName(),
                                    partition.getName(),
                                    externalTable);
                            chosenPartitions.add(chosenPartition);
                            remainJobs -= 1;
                            if (remainJobs <= 0) {
                                LOG.info("stop chose partition as no remain jobs");
                                break;
                            }
                        }
                    } // end while tableQueue
                } finally {
                    db.readUnlock();
                }
            } // end while dbQueue
        } finally {
            jobsLock.readLock().unlock();
        }

        return chosenPartitions;
    }

    // manually adding tablets to check
    public void addPartitionsToColdDown(List<ChosenPartition> chosenPartitions) {
        for (ChosenPartition chosenPartition : chosenPartitions) {
            ColdDownJob job = new ColdDownJob(
                    chosenPartition.getDatabase(),
                    chosenPartition.getTable(),
                    chosenPartition.getPartition(),
                    chosenPartition.getExternalTable()
                );
            addJob(job);
        }
    }
}
