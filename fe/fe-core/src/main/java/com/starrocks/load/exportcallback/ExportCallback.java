// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.exportcallback;

import com.starrocks.common.Status;
import com.starrocks.load.ExportJob;

public interface ExportCallback {
    Status runCallback(ExportJob job);
}
