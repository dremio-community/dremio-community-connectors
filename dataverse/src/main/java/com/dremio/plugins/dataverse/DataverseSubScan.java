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
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonTypeName("dataverse-sub-scan")
public class DataverseSubScan extends SubScanWithProjection {

    public static final int OPERATOR_TYPE = 5002;

    private final StoragePluginId pluginId;
    private final List<DataverseScanSpec> scanSpecs;

    @JsonCreator
    public DataverseSubScan(
            @JsonProperty("props")      OpProps                    props,
            @JsonProperty("schema")     BatchSchema                schema,
            @JsonProperty("tables")     Collection<List<String>>   tables,
            @JsonProperty("columns")    List<SchemaPath>           columns,
            @JsonProperty("pluginId")   StoragePluginId            pluginId,
            @JsonProperty("scanSpecs")  List<DataverseScanSpec>    scanSpecs) {
        super(props, schema, tables, columns);
        this.pluginId  = pluginId;
        this.scanSpecs = scanSpecs != null ? scanSpecs : Collections.emptyList();
    }

    @JsonProperty("pluginId")
    public StoragePluginId getPluginId() { return pluginId; }

    @JsonProperty("scanSpecs")
    public List<DataverseScanSpec> getScanSpecs() { return scanSpecs; }

    @JsonIgnore
    public DataverseScanSpec getScanSpec() {
        return scanSpecs.isEmpty() ? null : scanSpecs.get(0);
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
        return visitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        return new DataverseSubScan(getProps(), getFullSchema(), getReferencedTables(),
                getColumns(), pluginId, scanSpecs);
    }

    @Override
    public int getOperatorType() { return OPERATOR_TYPE; }
}
