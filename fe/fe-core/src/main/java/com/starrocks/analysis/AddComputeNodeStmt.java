// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/com/starrocks/system/ComputeNode.java

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

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.system.SystemInfoService;

import java.util.ArrayList;
import java.util.List;

public class AddComputeNodeStmt extends AlterSystemStmtNew
{
    private boolean free;
    private List<StringLiteral> hostPorts;
    private List<Pair<String, Integer>> hostPortPairs;

    public AddComputeNodeStmt(boolean free,List<StringLiteral> hostPorts,String clusterName) {
        hostPortPairs = new ArrayList<>();
        this.free = free;
        this.hostPorts = hostPorts;
        setClusterName(clusterName);
    }

    @Override
    public void analyze(ConnectContext context) throws AnalysisException, UserException {
        super.analyze(context);

        for (StringLiteral hostPort : hostPorts) {
            Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort.getStringValue());
            hostPortPairs.add(pair);
        }
    }

    @Override
    public synchronized void handle(ConnectContext context) throws UserException {
        final String destClusterName = getClusterName();
        if ((!Strings.isNullOrEmpty(destClusterName) || free) &&
                Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "ADD COMPUTOR NODE TO CLUSTER");
        }
        if (!Strings.isNullOrEmpty(destClusterName)
                && Catalog.getCurrentCatalog().getCluster(destClusterName) == null) {
            throw new DdlException("Cluster: " + destClusterName + " does not exist.");
        }
        Catalog.getCurrentSystemInfo().addComputeNodes(hostPortPairs, free, destClusterName);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddComputeNode(this, context);
    }

    public boolean isFree()
    {
        return free;
    }

    public List<Pair<String, Integer>> getHostPortPairs()
    {
        return hostPortPairs;
    }
}
