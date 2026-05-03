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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.influxdb.InfluxDBSubScan.InfluxDBScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InfluxDBScanPrel extends ScanPrelBase implements OldScanPrelBase {

    private final InfluxDBGroupScan groupScan;
    private final InfluxDBScanSpec scanSpec;

    public InfluxDBScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
            StoragePluginId pluginId, TableMetadata tableMetadata,
            List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
            List<RelHint> hints, InfluxDBGroupScan groupScan, InfluxDBScanSpec scanSpec) {
        super(cluster, traitSet, table, pluginId, tableMetadata,
                projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
        this.groupScan = groupScan;
        this.scanSpec = scanSpec;
    }

    @Override public GroupScan getGroupScan() { return groupScan; }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
        return new InfluxDBGroupScan(
                creator.props(this, null, getTableMetadata().getSchema()),
                getTableMetadata(), getProjectedColumns(), getPluginId(), scanSpec);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new InfluxDBScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
                getTableMetadata(), getProjectedColumns(),
                getCostAdjustmentFactor(), getHints(), groupScan, scanSpec);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
        return new InfluxDBScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
                getTableMetadata(), columns,
                getCostAdjustmentFactor(), getHints(), groupScan, scanSpec);
    }
}
