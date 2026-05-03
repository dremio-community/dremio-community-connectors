/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.influxdb;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.influxdb.InfluxDBSubScan.InfluxDBScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class InfluxDBGroupScan extends AbstractGroupScan {

    private final StoragePluginId pluginId;
    private final InfluxDBScanSpec scanSpec;

    public InfluxDBGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            InfluxDBScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        return new InfluxDBSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(scanSpec.getMeasurement())),
                getColumns(),
                pluginId,
                scanSpec);
    }

    @Override public int getMaxParallelizationWidth() { return 1; }
    @Override public int getOperatorType() { return 0; }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public InfluxDBScanSpec getScanSpec() { return scanSpec; }
}
