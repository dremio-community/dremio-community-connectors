/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class DataverseScanDrel extends ScanRelBase implements Rel {

    private final DataverseScanSpec scanSpec;

    public DataverseScanDrel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            StoragePluginId pluginId,
            TableMetadata tableMetadata,
            List<SchemaPath> projectedColumns,
            double observedRowcountAdjustment,
            List<RelHint> hints,
            DataverseScanSpec scanSpec) {
        super(cluster, traitSet, table, pluginId, tableMetadata,
                projectedColumns, observedRowcountAdjustment, hints);
        this.scanSpec = scanSpec;
    }

    public DataverseScanSpec getScanSpec() { return scanSpec; }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DataverseScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
                getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(), getHints(), scanSpec);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
        return new DataverseScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
                getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
    }
}
