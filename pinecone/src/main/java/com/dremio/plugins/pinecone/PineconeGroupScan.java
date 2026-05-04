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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.pinecone.PineconeSubScan.PineconeScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PineconeGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(PineconeGroupScan.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final StoragePluginId pluginId;
    private final PineconeScanSpec scanSpec;

    public PineconeGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            PineconeScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        PineconeScanSpec resolvedSpec = scanSpec;
        for (SplitWork sw : work) {
            PineconeScanSpec decoded = decodeSplit(sw);
            if (decoded != null) {
                resolvedSpec = decoded;
                break;
            }
        }

        return new PineconeSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(resolvedSpec.getTable())),
                getColumns(),
                pluginId,
                resolvedSpec);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1; // pagination-based — single reader per index
    }

    @Override
    public int getOperatorType() {
        return 0;
    }

    private PineconeScanSpec decodeSplit(SplitWork sw) {
        try {
            byte[] bytes = null;
            try {
                Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
                if (extProp != null) {
                    bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
                }
            } catch (Exception ignore) {}

            if (bytes != null && bytes.length > 0) {
                String json = new String(bytes, StandardCharsets.UTF_8);
                return MAPPER.readValue(json, PineconeScanSpec.class);
            }
        } catch (Exception e) {
            logger.warn("Could not decode Pinecone split spec, using base spec: {}", e.getMessage());
        }
        return null;
    }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public PineconeScanSpec getScanSpec() { return scanSpec; }
}
