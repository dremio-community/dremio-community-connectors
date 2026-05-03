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

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.influxdb.InfluxDBSubScan.InfluxDBScanSpec;

import java.util.List;

public class InfluxDBScanRule extends SourceLogicalConverter {

    public static final InfluxDBScanRule INSTANCE = new InfluxDBScanRule();

    private InfluxDBScanRule() {
        super(InfluxDBConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String measurement = qualifiedName.get(qualifiedName.size() - 1);
        InfluxDBScanSpec spec = new InfluxDBScanSpec(measurement);

        return new InfluxDBScanDrel(
                scan.getCluster(),
                scan.getTraitSet().replace(Rel.LOGICAL),
                scan.getTable(), scan.getPluginId(),
                scan.getTableMetadata(), scan.getProjectedColumns(),
                scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
    }
}
