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
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Collection;
import java.util.List;

/** Serializable unit of work sent from coordinator to executor fragments. */
@JsonTypeName("pinecone-sub-scan")
public class PineconeSubScan extends SubScanWithProjection {

    public static final int OPERATOR_TYPE = 5005;

    /** Describes a Pinecone index scan — one spec per index. */
    public static class PineconeScanSpec {

        private final String table;
        private final String host;
        private final long estimatedRowCount;

        @JsonCreator
        public PineconeScanSpec(
                @JsonProperty("table")             String table,
                @JsonProperty("host")              String host,
                @JsonProperty("estimatedRowCount") long   estimatedRowCount) {
            this.table             = table;
            this.host              = host;
            this.estimatedRowCount = estimatedRowCount;
        }

        @JsonProperty("table")
        public String getTable()             { return table; }

        @JsonProperty("host")
        public String getHost()              { return host; }

        @JsonProperty("estimatedRowCount")
        public long getEstimatedRowCount()   { return estimatedRowCount; }

        @Override
        public String toString() {
            return "PineconeScanSpec{table='" + table + "', host='" + host
                    + "', estimatedRows=" + estimatedRowCount + '}';
        }
    }

    private final StoragePluginId pluginId;
    private final PineconeScanSpec scanSpec;

    @JsonCreator
    public PineconeSubScan(
            @JsonProperty("props")    OpProps                  props,
            @JsonProperty("schema")   BatchSchema              schema,
            @JsonProperty("tables")   Collection<List<String>> tables,
            @JsonProperty("columns")  List<SchemaPath>         columns,
            @JsonProperty("pluginId") StoragePluginId          pluginId,
            @JsonProperty("scanSpec") PineconeScanSpec         scanSpec) {
        super(props, schema, tables, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @JsonProperty("pluginId")
    public StoragePluginId getPluginId() { return pluginId; }

    @JsonProperty("scanSpec")
    public PineconeScanSpec getScanSpec() { return scanSpec; }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
        return visitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        return new PineconeSubScan(getProps(), getFullSchema(), getReferencedTables(),
                getColumns(), pluginId, scanSpec);
    }

    @Override
    public int getOperatorType() { return OPERATOR_TYPE; }
}
