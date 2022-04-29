// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import avro.shaded.com.google.common.collect.Maps;
import com.starrocks.common.Status;
import com.starrocks.load.ExportJob;
import com.starrocks.thrift.TStatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergPartitionMgr {

    static Configuration conf = new Configuration();
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);
    private final String database;
    private final String table;
    private String catalogType;
    private String hiveMetaStoreUris;

    public IcebergPartitionMgr(String database, String table, String catalogType, String hiveMetaStoreUris) {
        this.database = database;
        this.table = table;
        this.catalogType = catalogType;
        this.hiveMetaStoreUris = hiveMetaStoreUris;
    }

    public static Configuration get_hadoop_conf() {
        return conf;
    }

    public HiveCatalog get_catalog() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", hiveMetaStoreUris);

        HiveCatalog catalog = new HiveCatalog();
        Configuration conf = get_hadoop_conf();
        catalog.setConf(conf);
        catalog.initialize(catalogType, properties);

        return catalog;
    }

    public synchronized Status add_file(Map<String, Set<String>> partitionFiles, Boolean reset) {
        // IcebergHiveCatalog catalog = IcebergHiveCatalog.getInstance(hiveMetaStoreUris);
        HiveCatalog catalog = get_catalog();

        LOG.debug("begin get tableIdentifier");
        TableIdentifier tableIdentifier = TableIdentifier.of(database, table);

        Table icebergTable = catalog.loadTable(tableIdentifier);
        Schema schema = icebergTable.schema();

        PartitionSpec partitionSpec = icebergTable.spec();
        SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat partitionFormat = new SimpleDateFormat("yyyy-MM-dd");

        HashMap<String, String> partitionMap = new HashMap<>();
        NameMapping nameMapping = MappingUtil.create(schema);
        Configuration conf = get_hadoop_conf();
        LOG.debug(String.format("begin get add part newAppend loop, %d", partitionFiles.size()));
        for (Map.Entry<String, Set<String>> entry : partitionFiles.entrySet()) {
            String partition = entry.getKey();
            Date date;
            try {

                date = dateParser.parse(partition.replace("p", ""));
            } catch (ParseException e) {
                String msg = String.format("parse partition key to date failed, key: %s", partition);
                LOG.error(msg);
                return new Status(TStatusCode.CANCELLED, msg);
            }

            Set<String> files = entry.getValue();
            LOG.debug(String.format("file count: %d", files.size()));
            ReplacePartitions replacePartitions = icebergTable.newReplacePartitions();
            for (String filePath : files) {
                for (PartitionField field : partitionSpec.fields()) {
                    if (field.toString().contains("bucket[")) {
                        partitionMap.put(field.name(), filePath.split("_")[2]);
                    } else {
                        partitionMap.put(field.name(), partitionFormat.format(date));
                    }
                }
                String partitionKey = partitionSpec.fields().stream()
                        .map(PartitionField::name)
                        .map(name -> String.format("%s=%s", name, partitionMap.get(name)))
                        .collect(Collectors.joining("/"));
                LOG.debug(String.format("partitionPath, %s, file count: %d", partitionKey, files.size()));

                LOG.debug(String.format("filePath, %s", filePath));
                HadoopInputFile inputFile = HadoopInputFile.fromLocation(filePath, conf);
                Metrics metrics1 = OrcMetrics.fromInputFile(inputFile, MetricsConfig.getDefault(), nameMapping);
                DataFile file = DataFiles.builder(partitionSpec)
                        .withPath(filePath)
                        .withFormat(FileFormat.fromFileName(filePath))
                        .withMetrics(metrics1)
                        .withFileSizeInBytes(inputFile.getLength())
                        .withPartitionPath(partitionKey)
                        .build();
                replacePartitions.addFile(file);
            }
            LOG.debug(String.format("begin commit iceberg partition replace, partition:%s", partition));
            try {
                replacePartitions.commit();
            } catch (CommitFailedException e) {
                LOG.error("commit iceberg partition replace [{}] failed", partition, e);
                return new Status(TStatusCode.CANCELLED, String.format("iceberg commit failed, %s", e));
            }
            LOG.debug(String.format("finished commit iceberg partition replace, partition: %s", partition));
        }
        return new Status();
    }
}
