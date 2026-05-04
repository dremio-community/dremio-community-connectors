/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.pinecone;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.pinecone.PineconeSubScan.PineconeScanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts a generic {@link ScanCrel} backed by a {@link PineconeConf} source into a
 * {@link PineconeScanDrel} during the LOGICAL planning phase.
 */
public class PineconeScanRule extends SourceLogicalConverter {

    private static final Logger logger = LoggerFactory.getLogger(PineconeScanRule.class);

    public static final PineconeScanRule INSTANCE = new PineconeScanRule();

    private PineconeScanRule() {
        super(PineconeConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String table = qualifiedName.get(qualifiedName.size() - 1);

        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        && scan.getTableMetadata().getReadDefinition().getScanStats() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                        : 100L);

        // Host is unknown at planning time; the executor resolves it via the plugin
        PineconeScanSpec spec = new PineconeScanSpec(table, "", estimatedRows);

        return new PineconeScanDrel(
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
