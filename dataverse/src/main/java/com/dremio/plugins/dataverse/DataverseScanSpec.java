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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes one scan chunk of a Dataverse entity query.
 *
 * <p>Carries the initial OData query URL (including $select and optional $filter).
 * The record reader follows @odata.nextLink for pagination until exhausted.
 */
public class DataverseScanSpec {

    /** Dataverse entity logical name, e.g. "account". */
    private final String entityLogicalName;

    /** OData entity set name (plural), e.g. "accounts". */
    private final String entitySetName;

    /**
     * Full OData query URL for this scan, e.g.:
     * https://org.api.crm.dynamics.com/api/data/v9.2/accounts?$select=name,accountid&$top=5000
     */
    private final String queryUrl;

    /** Estimated total row count for cost estimation. */
    private final long estimatedRowCount;

    @JsonCreator
    public DataverseScanSpec(
            @JsonProperty("entityLogicalName") String entityLogicalName,
            @JsonProperty("entitySetName")     String entitySetName,
            @JsonProperty("queryUrl")          String queryUrl,
            @JsonProperty("estimatedRowCount") long   estimatedRowCount) {
        this.entityLogicalName = entityLogicalName;
        this.entitySetName     = entitySetName;
        this.queryUrl          = queryUrl;
        this.estimatedRowCount = estimatedRowCount;
    }

    /**
     * Returns a new DataverseScanSpec with an OData $filter appended (or ANDed) into the query URL.
     */
    public DataverseScanSpec withFilter(String odataFilter) {
        if (odataFilter == null || odataFilter.isBlank()) {
            return this;
        }

        String url = queryUrl;
        if (url.contains("$filter=")) {
            // AND the new condition into the existing $filter value
            int idx = url.indexOf("$filter=") + "$filter=".length();
            // Find end of filter value (next & or end of string)
            int end = url.indexOf('&', idx);
            String existingFilter = (end < 0) ? url.substring(idx) : url.substring(idx, end);
            String newFilter = "(" + existingFilter + ") and (" + odataFilter + ")";
            url = url.substring(0, idx) + newFilter + (end < 0 ? "" : url.substring(end));
        } else if (url.contains("?")) {
            url = url + "&$filter=" + odataFilter;
        } else {
            url = url + "?$filter=" + odataFilter;
        }

        return new DataverseScanSpec(entityLogicalName, entitySetName, url, estimatedRowCount);
    }

    @JsonProperty("entityLogicalName")
    public String getEntityLogicalName() { return entityLogicalName; }

    @JsonProperty("entitySetName")
    public String getEntitySetName() { return entitySetName; }

    @JsonProperty("queryUrl")
    public String getQueryUrl() { return queryUrl; }

    @JsonProperty("estimatedRowCount")
    public long getEstimatedRowCount() { return estimatedRowCount; }

    @Override
    public String toString() {
        return "DataverseScanSpec{entity='" + entityLogicalName + "', url='" + queryUrl + "'}";
    }
}
