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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.List;

/**
 * Physical group scan for Zendesk. Single split per table.
 */
public class ZendeskGroupScan extends AbstractGroupScan {

    private final StoragePluginId pluginId;
    private final ZendeskScanSpec scanSpec;
    private final TableMetadata tableMetadata;

    public ZendeskGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            ZendeskScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        return new ZendeskSubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(scanSpec.getTableName())),
                getColumns(),
                pluginId,
                scanSpec);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1; // Zendesk has no offset-based splits
    }

    @Override
    public int getOperatorType() {
        return 0;
    }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public ZendeskScanSpec getScanSpec() { return scanSpec; }
}
