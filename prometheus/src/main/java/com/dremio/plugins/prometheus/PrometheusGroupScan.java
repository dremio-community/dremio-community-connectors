/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.prometheus;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.prometheus.PrometheusSubScan.PrometheusScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PrometheusGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusGroupScan.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final StoragePluginId pluginId;
    private final PrometheusScanSpec scanSpec;

    public PrometheusGroupScan(OpProps props, TableMetadata tableMetadata,
            List<SchemaPath> columns, StoragePluginId pluginId, PrometheusScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId  = pluginId;
        this.scanSpec  = scanSpec;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        PrometheusScanSpec resolved = scanSpec;
        for (SplitWork sw : work) {
            PrometheusScanSpec decoded = decodeSplit(sw);
            if (decoded != null) { resolved = decoded; break; }
        }
        return new PrometheusSubScan(getProps(), getFullSchema(),
                List.of(List.of(resolved.getTableName())),
                getColumns(), pluginId, resolved);
    }

    @Override
    public int getMaxParallelizationWidth() { return 1; }

    @Override
    public int getOperatorType() { return 0; }

    private PrometheusScanSpec decodeSplit(SplitWork sw) {
        try {
            byte[] bytes = null;
            try {
                Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
                if (extProp != null) {
                    bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
                }
            } catch (Exception ignore) {}
            if (bytes != null && bytes.length > 0) {
                return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), PrometheusScanSpec.class);
            }
        } catch (Exception e) {
            logger.warn("Could not decode Prometheus split spec: {}", e.getMessage());
        }
        return null;
    }

    @JsonIgnore public StoragePluginId getPluginId()    { return pluginId; }
    @JsonIgnore public PrometheusScanSpec getScanSpec() { return scanSpec; }
}
