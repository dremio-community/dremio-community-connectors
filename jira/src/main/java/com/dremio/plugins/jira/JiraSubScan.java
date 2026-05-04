/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.jira;

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

@JsonTypeName("jira-sub-scan")
public class JiraSubScan extends SubScanWithProjection {

    public static final int OPERATOR_TYPE = 5004;

    public static class JiraScanSpec {

        private final String tableName;
        private final long estimatedRowCount;

        @JsonCreator
        public JiraScanSpec(
                @JsonProperty("tableName")        String tableName,
                @JsonProperty("estimatedRowCount") long   estimatedRowCount) {
            this.tableName        = tableName;
            this.estimatedRowCount = estimatedRowCount;
        }

        @JsonProperty("tableName")
        public String getTableName() {
            return tableName;
        }

        @JsonProperty("estimatedRowCount")
        public long getEstimatedRowCount() {
            return estimatedRowCount;
        }

        @Override
        public String toString() {
            return "JiraScanSpec{table='" + tableName + "', rows=" + estimatedRowCount + '}';
        }
    }

    private final StoragePluginId pluginId;
    private final JiraScanSpec scanSpec;

    @JsonCreator
    public JiraSubScan(
            @JsonProperty("props")    OpProps                  props,
            @JsonProperty("schema")   BatchSchema              schema,
            @JsonProperty("tables")   Collection<List<String>> tables,
            @JsonProperty("columns")  List<SchemaPath>         columns,
            @JsonProperty("pluginId") StoragePluginId          pluginId,
            @JsonProperty("scanSpec") JiraScanSpec             scanSpec) {
        super(props, schema, tables, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @JsonProperty("pluginId")
    public StoragePluginId getPluginId() {
        return pluginId;
    }

    @JsonProperty("scanSpec")
    public JiraScanSpec getScanSpec() {
        return scanSpec;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
        return visitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        return new JiraSubScan(getProps(), getFullSchema(), getReferencedTables(),
                getColumns(), pluginId, scanSpec);
    }

    @Override
    public int getOperatorType() {
        return OPERATOR_TYPE;
    }
}
