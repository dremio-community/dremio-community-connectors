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

/**
 * Serializable unit of work sent from coordinator to executor fragments.
 */
@JsonTypeName("hubspot-sub-scan")
public class HubSpotSubScan extends SubScanWithProjection {

    public static final int OPERATOR_TYPE = 5003;

    /**
     * Describes a HubSpot table scan — one spec per object type.
     *
     * <p>Pagination is handled cursor-based inside {@link HubSpotRecordReader};
     * there is no LIMIT/OFFSET splitting like Salesforce.
     */
    public static class HubSpotScanSpec {

        /** HubSpot CRM object type, e.g. "contacts", "deals", "owners". */
        private final String objectType;

        /**
         * Property names to fetch from the Properties API.
         * Empty for "owners" (schema is hardcoded).
         */
        private final List<String> propertyNames;

        /** Estimated total row count for cost estimation. */
        private final long estimatedRowCount;

        @JsonCreator
        public HubSpotScanSpec(
                @JsonProperty("objectType")      String       objectType,
                @JsonProperty("propertyNames")   List<String> propertyNames,
                @JsonProperty("estimatedRowCount") long       estimatedRowCount) {
            this.objectType       = objectType;
            this.propertyNames    = propertyNames;
            this.estimatedRowCount = estimatedRowCount;
        }

        @JsonProperty("objectType")
        public String getObjectType() {
            return objectType;
        }

        @JsonProperty("propertyNames")
        public List<String> getPropertyNames() {
            return propertyNames;
        }

        @JsonProperty("estimatedRowCount")
        public long getEstimatedRowCount() {
            return estimatedRowCount;
        }

        @Override
        public String toString() {
            return "HubSpotScanSpec{objectType='" + objectType
                    + "', properties=" + (propertyNames == null ? 0 : propertyNames.size())
                    + ", estimatedRows=" + estimatedRowCount + '}';
        }
    }

    private final StoragePluginId pluginId;
    private final HubSpotScanSpec scanSpec;

    @JsonCreator
    public HubSpotSubScan(
            @JsonProperty("props")     OpProps                  props,
            @JsonProperty("schema")    BatchSchema              schema,
            @JsonProperty("tables")    Collection<List<String>> tables,
            @JsonProperty("columns")   List<SchemaPath>         columns,
            @JsonProperty("pluginId")  StoragePluginId          pluginId,
            @JsonProperty("scanSpec")  HubSpotScanSpec          scanSpec) {
        super(props, schema, tables, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @JsonProperty("pluginId")
    public StoragePluginId getPluginId() { return pluginId; }

    @JsonProperty("scanSpec")
    public HubSpotScanSpec getScanSpec() { return scanSpec; }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
        return visitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        return new HubSpotSubScan(getProps(), getFullSchema(), getReferencedTables(),
                getColumns(), pluginId, scanSpec);
    }

    @Override
    public int getOperatorType() { return OPERATOR_TYPE; }
}
