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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.hubspot.HubSpotSubScan.HubSpotScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Physical group scan for HubSpot.
 *
 * <p>HubSpot uses cursor-based pagination, so there is always a single split per object.
 * The split bytes carry the full JSON-encoded HubSpotScanSpec (including the resolved
 * property list from the Properties API), which is decoded here and forwarded to the SubScan.
 */
public class HubSpotGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotGroupScan.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final StoragePluginId pluginId;
    private final HubSpotScanSpec scanSpec;
    private final TableMetadata tableMetadata;

    public HubSpotGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            HubSpotScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        // Attempt to decode the full spec from split bytes (has the resolved property list).
        HubSpotScanSpec resolvedSpec = scanSpec;
        for (SplitWork sw : work) {
            HubSpotScanSpec decoded = decodeSplit(sw);
            if (decoded != null) {
                resolvedSpec = decoded;
                break;
            }
        }

        return new HubSpotSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(resolvedSpec.getObjectType())),
                getColumns(),
                pluginId,
                resolvedSpec);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1; // cursor-based pagination — single reader per object
    }

    @Override
    public int getOperatorType() {
        return 0;
    }

    private HubSpotScanSpec decodeSplit(SplitWork sw) {
        try {
            byte[] bytes = null;
            try {
                Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
                if (extProp != null) {
                    bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
                }
            } catch (Exception ignore) {
                // Method may not exist on all Dremio versions
            }

            if (bytes != null && bytes.length > 0) {
                String json = new String(bytes, StandardCharsets.UTF_8);
                return MAPPER.readValue(json, HubSpotScanSpec.class);
            }
        } catch (Exception e) {
            logger.warn("Could not decode HubSpot split spec, using base spec: {}", e.getMessage());
        }
        return null;
    }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public HubSpotScanSpec getScanSpec()  { return scanSpec; }
}
