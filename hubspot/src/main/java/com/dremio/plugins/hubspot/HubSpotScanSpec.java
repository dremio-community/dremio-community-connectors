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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Describes a HubSpot table scan — one spec per object type.
 *
 * <p>Pagination is handled cursor-based inside {@link HubSpotRecordReader};
 * there is no LIMIT/OFFSET splitting like Salesforce.
 */
public class HubSpotScanSpec {

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
