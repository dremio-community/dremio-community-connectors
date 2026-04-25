/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.hubspot;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Converts a generic {@link ScanCrel} backed by a {@link HubSpotConf} source into a
 * {@link HubSpotScanDrel} during the LOGICAL planning phase.
 *
 * <p>CRITICAL: Must call {@code scan.getTraitSet().replace(Rel.LOGICAL)} so that
 * {@link HubSpotScanPrule} fires in the PHYSICAL phase.
 */
public class HubSpotScanRule extends SourceLogicalConverter {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotScanRule.class);

    public static final HubSpotScanRule INSTANCE = new HubSpotScanRule();

    private HubSpotScanRule() {
        super(HubSpotConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String objectType = qualifiedName.get(qualifiedName.size() - 1);

        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        && scan.getTableMetadata().getReadDefinition().getScanStats() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                        : 1000L);

        // Property names will be filled in from partition chunk metadata at execution time.
        // Use empty list here — the real list is encoded in the DatasetSplit bytes.
        HubSpotScanSpec spec = new HubSpotScanSpec(objectType, Collections.emptyList(), estimatedRows);

        return new HubSpotScanDrel(
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
