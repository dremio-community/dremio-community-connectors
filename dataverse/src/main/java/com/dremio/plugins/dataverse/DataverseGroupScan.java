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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DataverseGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(DataverseGroupScan.class);

    private final StoragePluginId pluginId;
    private final DataverseScanSpec scanSpec;
    private final TableMetadata tableMetadata;

    public DataverseGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            DataverseScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        List<DataverseScanSpec> specs = new ArrayList<>();
        for (SplitWork sw : work) {
            DataverseScanSpec decoded = decodeSplit(sw);
            if (decoded != null) specs.add(decoded);
        }
        if (specs.isEmpty()) specs.add(scanSpec);

        return new DataverseSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(scanSpec.getEntityLogicalName())),
                getColumns(),
                pluginId,
                specs);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getOperatorType() {
        return 0;
    }

    public DataverseGroupScan withFilter(String odataFilter) {
        if (odataFilter == null || odataFilter.isBlank()) return null;
        DataverseScanSpec updated = scanSpec.withFilter(odataFilter);
        return new DataverseGroupScan(getProps(), tableMetadata, getColumns(), pluginId, updated);
    }

    private DataverseScanSpec decodeSplit(SplitWork sw) {
        try {
            byte[] bytes = null;
            try {
                Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
                if (extProp != null) {
                    bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
                }
            } catch (Exception ignore) {}

            if (bytes != null && bytes.length > 0) {
                // Format: "logicalName|entitySetName|queryUrl"
                String spec = new String(bytes, StandardCharsets.UTF_8);
                String[] parts = spec.split("\\|", 3);
                if (parts.length == 3) {
                    return new DataverseScanSpec(parts[0], parts[1], parts[2], scanSpec.getEstimatedRowCount());
                }
            }
        } catch (Exception e) {
            logger.warn("Could not decode Dataverse split spec, using fallback: {}", e.getMessage());
        }
        return null;
    }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public DataverseScanSpec getScanSpec() { return scanSpec; }
}
