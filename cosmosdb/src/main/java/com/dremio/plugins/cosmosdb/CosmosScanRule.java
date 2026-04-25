/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.cosmosdb;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;

import java.util.List;

public class CosmosScanRule extends SourceLogicalConverter {

    public static final CosmosScanRule INSTANCE = new CosmosScanRule();

    private CosmosScanRule() {
        super(CosmosConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String container = qualifiedName.get(qualifiedName.size() - 1);
        String sql = "SELECT * FROM c";

        CosmosScanSpec spec = new CosmosScanSpec(container, sql);

        return new CosmosScanDrel(
                scan.getCluster(),
                scan.getTraitSet().replace(Rel.LOGICAL),
                scan.getTable(), scan.getPluginId(),
                scan.getTableMetadata(), scan.getProjectedColumns(),
                scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
    }
}
