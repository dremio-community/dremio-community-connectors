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

/**
 * Physical group scan for Salesforce.
 *
 * <p>Each split carries a SalesforceScanSpec with its own SOQL (LIMIT+OFFSET).
 * On a single-node Dremio cluster all splits may be assigned to one fragment.
 */
public class SalesforceGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceGroupScan.class);

    private final StoragePluginId pluginId;
    private final SalesforceScanSpec scanSpec;
    private final TableMetadata tableMetadata;

    public SalesforceGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            SalesforceScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        // Decode all assigned splits; each carries an encoded SalesforceScanSpec.
        List<SalesforceScanSpec> specs = new ArrayList<>();
        for (SplitWork sw : work) {
            SalesforceScanSpec decoded = decodeSplit(sw);
            if (decoded != null) {
                specs.add(decoded);
            }
        }
        // Fallback: use the base scan spec (no offset)
        if (specs.isEmpty()) {
            specs.add(scanSpec);
        }

        return new SalesforceSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(scanSpec.getObjectName())),
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
        return 0; // UNKNOWN
    }

    // -------------------------------------------------------------------------
    // Filter pushdown helper
    // -------------------------------------------------------------------------

    /**
     * Returns a new SalesforceGroupScan with a WHERE clause appended to the SOQL.
     * Returns null if the where clause is empty/null (no pushdown to perform).
     */
    public SalesforceGroupScan withWhereClause(String whereClause) {
        if (whereClause == null || whereClause.isBlank()) {
            return null;
        }
        SalesforceScanSpec updated = scanSpec.withWhereClause(whereClause);
        return new SalesforceGroupScan(getProps(), tableMetadata, getColumns(), pluginId, updated);
    }

    // -------------------------------------------------------------------------
    // Split decoding
    // -------------------------------------------------------------------------

    /**
     * Decodes the split's extended property bytes into a SalesforceScanSpec.
     * Format: "objectName|splitIndex|splitCount|soql"
     */
    private SalesforceScanSpec decodeSplit(SplitWork sw) {
        try {
            byte[] bytes = null;
            try {
                Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
                if (extProp != null) {
                    bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
                }
            } catch (Exception ignore) {
                // Method may not exist on all versions
            }

            if (bytes != null && bytes.length > 0) {
                // Format: "objectName|splitIndex|splitCount|soql"
                String spec = new String(bytes, StandardCharsets.UTF_8);
                String[] parts = spec.split("\\|", 4);
                if (parts.length == 4) {
                    return new SalesforceScanSpec(
                            parts[0],                            // objectName
                            parts[3],                            // soql
                            scanSpec.getEstimatedRowCount(),
                            Integer.parseInt(parts[1]),          // splitIndex
                            Integer.parseInt(parts[2]));         // splitCount
                }
            }
        } catch (Exception e) {
            logger.warn("Could not decode Salesforce split spec, using fallback: {}", e.getMessage());
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public SalesforceScanSpec getScanSpec() { return scanSpec; }
}
