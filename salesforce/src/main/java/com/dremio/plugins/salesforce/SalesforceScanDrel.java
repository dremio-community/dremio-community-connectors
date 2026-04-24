/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

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

/**
 * Logical plan node for a Salesforce table scan.
 *
 * <p>CRITICAL: Must carry {@link Rel#LOGICAL} convention so that
 * {@link SalesforceScanPrule} can fire in the PHYSICAL phase.
 */
public class SalesforceScanDrel extends ScanRelBase implements Rel {

    private final SalesforceScanSpec scanSpec;

    public SalesforceScanDrel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            StoragePluginId pluginId,
            TableMetadata tableMetadata,
            List<SchemaPath> projectedColumns,
            double observedRowcountAdjustment,
            List<RelHint> hints,
            SalesforceScanSpec scanSpec) {
        super(cluster, traitSet, table, pluginId, tableMetadata,
                projectedColumns, observedRowcountAdjustment, hints);
        this.scanSpec = scanSpec;
    }

    public SalesforceScanSpec getScanSpec() {
        return scanSpec;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SalesforceScanDrel(
                getCluster(), traitSet, getTable(), getPluginId(),
                getTableMetadata(), getProjectedColumns(),
                getCostAdjustmentFactor(), getHints(), scanSpec);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
        return new SalesforceScanDrel(
                getCluster(), getTraitSet(), getTable(), getPluginId(),
                getTableMetadata(), columns,
                getCostAdjustmentFactor(), getHints(), scanSpec);
    }
}
