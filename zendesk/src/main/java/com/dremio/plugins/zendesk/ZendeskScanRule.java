/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.zendesk;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;

import java.util.List;

/**
 * Converts a generic ScanCrel backed by a ZendeskConf source into a ZendeskScanDrel.
 */
public class ZendeskScanRule extends SourceLogicalConverter {

    public static final ZendeskScanRule INSTANCE = new ZendeskScanRule();

    private ZendeskScanRule() {
        super(ZendeskConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String tableName = qualifiedName.get(qualifiedName.size() - 1).toLowerCase();

        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        && scan.getTableMetadata().getReadDefinition().getScanStats() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                        : 10_000L);

        ZendeskScanSpec spec = new ZendeskScanSpec(tableName, estimatedRows);

        return new ZendeskScanDrel(
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
