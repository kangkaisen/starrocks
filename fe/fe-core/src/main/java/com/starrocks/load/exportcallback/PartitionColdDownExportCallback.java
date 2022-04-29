// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.exportcallback;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Status;
import com.starrocks.external.iceberg.IcebergHiveCatalog;
import com.starrocks.external.iceberg.IcebergPartitionMgr;
import com.starrocks.load.ExportJob;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.system.SystemInfoService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PartitionColdDownExportCallback implements ExportCallback {

    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    PartitionColdDownExportCallback() {}

    private static class IcebergTableInfo {
        public String database;
        public String table;
        public String catalogType;
        public String hiveMetastoreURIs;
    }

    public static IcebergTableInfo getIcebergTableInfo(String tableName) {
        IcebergTableInfo info = new IcebergTableInfo();

        LOG.info(String.format("getIcebergTableInfo: %s", tableName));
        String[] parts = tableName.split("\\.");
        LOG.info(String.format("getIcebergTableInfo: %s", String.join("+", parts)));
        String fullDatabaseName = String.format("%s:%s", SystemInfoService.DEFAULT_CLUSTER, parts[0]);
        Database database = Catalog.getCurrentCatalog().getDb(fullDatabaseName);
        if (database == null) {
            return null;
        }
        Table table = database.getTable(parts[1]);
        IcebergTable icebergTable = (IcebergTable) table;
        String resourceName = icebergTable.getResourceName();
        info.table = icebergTable.getTable();
        info.database = icebergTable.getDb();
        Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            LOG.error(String.format("invalid resource name %s", resourceName));
            return null;
        }
        if (resource.getType() != Resource.ResourceType.ICEBERG) {
            LOG.error(String.format("resource type should be %s, get %s", Resource.ResourceType.ICEBERG, resourceName));
        }
        IcebergResource icebergResource = (IcebergResource) resource;
        info.catalogType = icebergResource.getCatalogType().toString();
        info.hiveMetastoreURIs = icebergResource.getHiveMetastoreURIs();
        return info;
    }

    public static org.apache.iceberg.Table getIcebergTable(String externalTable) {
        String[] parts = externalTable.split("\\.");
        String fullDatabaseName = String.format("%s:%s", SystemInfoService.DEFAULT_CLUSTER, parts[0]);
        Database database = Catalog.getCurrentCatalog().getDb(fullDatabaseName);
        if (database == null) {
            return null;
        }
        Table table = database.getTable(parts[1]);
        if (table == null) {
            return null;
        }
        IcebergTable icebergTable = (IcebergTable) table;
        return icebergTable.getIcebergTable();
    }

    public OlapTable getJobRelatedTable(ExportJob job) {
        Database database = Catalog.getCurrentCatalog().getDb(job.getDbId());
        Table table = database.getTable(job.getTableId());
        if (!(table instanceof OlapTable)) {
            return null;
        }

        return (OlapTable) table;
    }

    @Override
    public Status runCallback(ExportJob job) {
        Database db = Catalog.getCurrentCatalog().getDb(job.getDbId());
        OlapTable olapTable = getJobRelatedTable(job);
        olapTable.getPartitionColumnNames();
        TableProperty property = olapTable.getTableProperty();
        Map<String, String> properties = property.getProperties();
        String externalTable = properties.get("external_table");
        if (externalTable == null || externalTable.isEmpty()) {
            LOG.warn(String.format("table %s has no external table property", olapTable.getName()));
            return Status.OK;
        }
        IcebergTableInfo icebergTableInfo = getIcebergTableInfo(externalTable);
        if (icebergTableInfo == null) {
            LOG.warn(String.format("table %s related iceberg table not found", externalTable));
            return Status.OK;
        }
        LOG.debug(String.format("begin new IcebergPartitionMgr with externalTable: %s", externalTable));

        IcebergPartitionMgr icebergPartitionMgr = new IcebergPartitionMgr(
                icebergTableInfo.database,
                icebergTableInfo.table,
                icebergTableInfo.catalogType,
                icebergTableInfo.hiveMetastoreURIs
        );
        HashMap<String, Set<String>> partitionFiles = new HashMap<>();
        String partitionName = job.getPartitions().get(0);
        partitionFiles.put(partitionName, job.getExportedFile());
        Partition partition = olapTable.getPartition(partitionName);
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Status status = icebergPartitionMgr.add_file(partitionFiles, true);
        if (!status.ok()) {
            return status;
        }
        long coldDownTimeMs = job.getStartTimeMs();
        db.writeLock();
        try {
            partitionInfo.setColdDownSyncedTimeMs(partition.getId(), coldDownTimeMs);
        } finally {
            db.writeUnlock();
        }
        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(), partition.getId(),
                DataProperty.DEFAULT_DATA_PROPERTY, (short) -1, partitionInfo.getIsInMemory(partition.getId()),
                coldDownTimeMs);
        Catalog.getCurrentCatalog().getEditLog().logModifyPartition(info);
        LOG.info("modify partition[{}-{}-{}] cold down time to {}", db.getFullName(), olapTable.getName(),
                partition.getName(), coldDownTimeMs);

        // refresh hive metastore cache
        IcebergHiveCatalog hiveCatalog = IcebergHiveCatalog.getInstance(icebergTableInfo.hiveMetastoreURIs);
        TableIdentifier tableIdentifier = TableIdentifier.of(icebergTableInfo.database, icebergTableInfo.table);
        hiveCatalog.newTableOps(tableIdentifier).refresh();

        return status;
    }
}
