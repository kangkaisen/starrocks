// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/consistency/CheckConsistencyJob.java

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ColdDownJob {
    private static final Logger LOG = LogManager.getLogger(ColdDownJob.class);

    public enum JobState {
        PENDING,
        RUNNING
    }

    private JobState state;
    private final String database;
    private final String table;
    private final String partition;
    private final String externalTable;

    private long createTime;
    private long timeoutMs;

    public ColdDownJob(String database, String table, String partition, String externalTable) {
        this.state = JobState.PENDING;
        this.database = database;
        this.table = table;
        this.partition = partition;
        this.externalTable = externalTable;

        this.createTime = System.currentTimeMillis();
        this.timeoutMs = 0L;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
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

    public String getKey() {
        return String.format("%s-%s-%s", database, table, partition);
    }

    private boolean isTimeout() {
        if (timeoutMs == 0 || System.currentTimeMillis() - createTime < timeoutMs) {
            return false;
        }
        return true;
    }
}

