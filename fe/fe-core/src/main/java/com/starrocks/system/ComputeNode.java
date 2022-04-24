// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/ComputeNode.java

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

package com.starrocks.system;

import com.starrocks.alter.DecommissionBackendJob;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComputeNode extends Backend {

    private static final Logger LOG = LogManager.getLogger(ComputeNode.class);

    public ComputeNode() {
    }

    public ComputeNode(long id, String host, int heartbeatPort) {
        super(id, host, heartbeatPort);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getId());
        Text.writeString(out, getHost());
        out.writeInt(getHeartbeatPort());
        out.writeInt(getBePort());
        out.writeInt(getHttpPort());
        out.writeInt(getBeRpcPort());
        out.writeBoolean(isAlive());
        out.writeBoolean(isDecommissioned());
        out.writeLong(getLastUpdateMs());

        out.writeLong(getLastStartTime());

        Text.writeString(out, getOwnerClusterName());
        out.writeInt(getBackendStateValue());
        out.writeInt(getDecommissionTypeValue());

        out.writeInt(getBrpcPort());
    }

    public static ComputeNode read(DataInput in) throws IOException {
        ComputeNode computeNode = new ComputeNode();
        computeNode.readFields(in);
        return computeNode;
    }

    public void readFields(DataInput in) throws IOException {
        setId(in.readLong());
        setHost(Text.readString(in));
        setHeartbeatPort(in.readInt());
        setBePort(in.readInt());
        setHttpPort(in.readInt());
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_31) {
            setBeRpcPort(in.readInt());
        }
        setAlive(in.readBoolean());

        if (Catalog.getCurrentCatalogJournalVersion() >= 5) {
            setDecommissioned(in.readBoolean());
        }

        setLastUpdateMs(in.readLong());

        if (Catalog.getCurrentCatalogJournalVersion() >= 2) {
            setLastStartTime(in.readLong());
        }
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            setOwnerClusterName(Text.readString(in));
            setBackendStateVlaue(in.readInt());
            setDecommissionTypeValue(in.readInt());
        } else {
            setOwnerClusterName(SystemInfoService.DEFAULT_CLUSTER);
            setBackendStateVlaue(BackendState.using.ordinal());
            setDecommissionTypeValue(DecommissionBackendJob.DecommissionType.SystemDecommission.ordinal());
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_40) {
            setBrpcPort(in.readInt());
        }
    }

}
