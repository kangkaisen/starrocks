package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.system.SystemInfoService;

import java.util.ArrayList;
import java.util.List;

public class DropComputeNodeStmt
        extends AlterSystemStmtNew
{
    private List<StringLiteral> hostPorts;
    private List<Pair<String, Integer>> hostPortPairs;

    public DropComputeNodeStmt(List<StringLiteral> hostPorts) {
        this.hostPorts = hostPorts;
        hostPortPairs = new ArrayList<>();
    }

    @Override
    public void analyze(ConnectContext context)
            throws AnalysisException, UserException {
        super.analyze(context);

        for (StringLiteral hostPort : hostPorts) {
            Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort.getStringValue());
            hostPortPairs.add(pair);
        }

        Preconditions.checkState(!hostPortPairs.isEmpty());
    }

    @Override
    public synchronized void handle(ConnectContext context)
            throws Exception {
        Catalog.getCurrentSystemInfo().dropComputeNodes(hostPortPairs);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropComputeNode(this, context);
    }

}
