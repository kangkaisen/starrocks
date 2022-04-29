// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.exportcallback;

public class ExportCallbackFactory {
    public enum ExportType {
        DEFAULT,
        PARTITION_COLD_DOWN,
    }

    public ExportCallback create(String jobType) {
        if (ExportType.PARTITION_COLD_DOWN.name().toLowerCase().contentEquals(jobType)) {
            return create(ExportType.PARTITION_COLD_DOWN);
        }
        return create(ExportType.DEFAULT);
    }

    public ExportCallback create(ExportType jobType) {
        if (jobType == ExportType.PARTITION_COLD_DOWN) {
            return new PartitionColdDownExportCallback();
        }
        return new NoopExportCallback();
    }
}
