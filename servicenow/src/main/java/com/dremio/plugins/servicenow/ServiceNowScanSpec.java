/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.servicenow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a ServiceNow table scan.
 *
 * <p>Unlike Salesforce, ServiceNow does not support OFFSET-based splits, so each
 * table is read as a single logical scan that pages through the full cursor chain.
 */
public class ServiceNowScanSpec {

    /** Logical table name, e.g. "tickets", "users". */
    private final String tableName;

    /** Estimated row count (used for cost estimation; may be approximate). */
    private final long estimatedRowCount;

    @JsonCreator
    public ServiceNowScanSpec(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("estimatedRowCount") long estimatedRowCount) {
        this.tableName = tableName;
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
        return "ServiceNowScanSpec{tableName='" + tableName + "', estimatedRowCount=" + estimatedRowCount + '}';
    }
}
