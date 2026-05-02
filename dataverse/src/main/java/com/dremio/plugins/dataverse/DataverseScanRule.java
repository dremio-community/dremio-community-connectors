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

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;

import java.util.List;

/**
 * Converts a generic ScanCrel backed by DataverseConf into a DataverseScanDrel during LOGICAL planning.
 *
 * <p>CRITICAL: replace(Rel.LOGICAL) must be called on the trait set so DataverseScanPrule fires.
 */
public class DataverseScanRule extends SourceLogicalConverter {

    public static final DataverseScanRule INSTANCE = new DataverseScanRule();

    private DataverseScanRule() {
        super(DataverseConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String entityLogicalName = qualifiedName.get(qualifiedName.size() - 1);

        // Base URL placeholder — the real URL with $select is built in listPartitionChunks.
        // ScanSpec here is used only for cost estimation; actual query URL comes from the split.
        String baseUrl = entityLogicalName + "|placeholder";

        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        && scan.getTableMetadata().getReadDefinition().getScanStats() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                        : 10_000L);

        // entitySetName unknown here — filled in from the split at execution time
        DataverseScanSpec spec = new DataverseScanSpec(entityLogicalName, entityLogicalName + "s",
                baseUrl, estimatedRows);

        return new DataverseScanDrel(
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
