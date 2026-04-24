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

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts a generic {@link ScanCrel} backed by a {@link SalesforceConf} source into a
 * {@link SalesforceScanDrel} during the LOGICAL planning phase.
 *
 * <p>Uses {@link SourceLogicalConverter} which matches on the plugin config class and
 * calls {@link #convertScan} with the already-matched ScanCrel.
 *
 * <p><strong>CRITICAL</strong>: {@link #convertScan} calls
 * {@code scan.getTraitSet().replace(Rel.LOGICAL)} — without this the produced node has
 * {@code NONE} convention and the physical rule {@link SalesforceScanPrule} will never fire,
 * resulting in a "cannot be planned" error.
 */
public class SalesforceScanRule extends SourceLogicalConverter {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceScanRule.class);

    public static final SalesforceScanRule INSTANCE = new SalesforceScanRule();

    private SalesforceScanRule() {
        super(SalesforceConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String objectName = qualifiedName.get(qualifiedName.size() - 1);

        // Build a base SOQL — no WHERE, no LIMIT/OFFSET; splits add those at execution time.
        // The SOQL here is used for the initial group scan cost estimation.
        // The actual per-split SOQL (with LIMIT+OFFSET) is encoded in the partition chunks
        // written by SalesforceStoragePlugin.listPartitionChunks().
        String baseSoql = "SELECT * FROM " + objectName;

        // Estimate row count from table metadata (may be 0 on first plan)
        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats() != null
                                ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                                : 10_000L
                        : 10_000L);

        SalesforceScanSpec spec = new SalesforceScanSpec(objectName, baseSoql, estimatedRows, 0, 1);

        // CRITICAL: replace(Rel.LOGICAL) so the physical rule fires
        return new SalesforceScanDrel(
                scan.getCluster(),
                scan.getTraitSet().replace(Rel.LOGICAL),
                scan.getTable(),
                scan.getPluginId(),
                scan.getTableMetadata(),
                scan.getProjectedColumns(),
                scan.getObservedRowcountAdjustment(),
                scan.getHints(),
                spec);
    }
}
