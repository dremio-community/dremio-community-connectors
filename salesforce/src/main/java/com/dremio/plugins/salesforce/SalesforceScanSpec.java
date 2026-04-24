/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes one parallel chunk of a Salesforce table scan.
 *
 * <p>Each split carries its own SOQL (with LIMIT + OFFSET) so readers can execute
 * independently without coordination.
 */
public class SalesforceScanSpec {

    /** Salesforce SObject API name, e.g. "Account", "Contact". */
    private final String objectName;

    /**
     * Full SOQL query for this split, including LIMIT and OFFSET.
     * Example: SELECT Id, Name FROM Account LIMIT 500 OFFSET 0
     */
    private final String soqlQuery;

    /** Estimated total row count for the object (used for cost estimation). */
    private final long estimatedRowCount;

    /** Zero-based index of this split within the total splits for this scan. */
    private final int splitIndex;

    /** Total number of splits for this scan. */
    private final int splitCount;

    @JsonCreator
    public SalesforceScanSpec(
            @JsonProperty("objectName") String objectName,
            @JsonProperty("soqlQuery") String soqlQuery,
            @JsonProperty("estimatedRowCount") long estimatedRowCount,
            @JsonProperty("splitIndex") int splitIndex,
            @JsonProperty("splitCount") int splitCount) {
        this.objectName = objectName;
        this.soqlQuery = soqlQuery;
        this.estimatedRowCount = estimatedRowCount;
        this.splitIndex = splitIndex;
        this.splitCount = splitCount;
    }

    /**
     * Returns a new SalesforceScanSpec with a WHERE clause appended (or ANDed) to the SOQL.
     */
    public SalesforceScanSpec withWhereClause(String whereClause) {
        if (whereClause == null || whereClause.isBlank()) {
            return this;
        }

        String updatedSoql;
        String upperSoql = soqlQuery.toUpperCase();

        if (upperSoql.contains(" WHERE ")) {
            // AND the new condition into existing WHERE
            // Insert before LIMIT/OFFSET if present
            int limitIdx = upperSoql.indexOf(" LIMIT ");
            int offsetIdx = upperSoql.indexOf(" OFFSET ");
            int insertAt = -1;
            if (limitIdx > 0) insertAt = limitIdx;
            if (offsetIdx > 0 && (insertAt < 0 || offsetIdx < insertAt)) insertAt = offsetIdx;

            if (insertAt > 0) {
                updatedSoql = soqlQuery.substring(0, insertAt)
                        + " AND (" + whereClause + ")"
                        + soqlQuery.substring(insertAt);
            } else {
                updatedSoql = soqlQuery + " AND (" + whereClause + ")";
            }
        } else {
            // Insert WHERE before LIMIT/OFFSET if present
            int limitIdx = upperSoql.indexOf(" LIMIT ");
            int offsetIdx = upperSoql.indexOf(" OFFSET ");
            int insertAt = -1;
            if (limitIdx > 0) insertAt = limitIdx;
            if (offsetIdx > 0 && (insertAt < 0 || offsetIdx < insertAt)) insertAt = offsetIdx;

            if (insertAt > 0) {
                updatedSoql = soqlQuery.substring(0, insertAt)
                        + " WHERE " + whereClause
                        + soqlQuery.substring(insertAt);
            } else {
                updatedSoql = soqlQuery + " WHERE " + whereClause;
            }
        }

        return new SalesforceScanSpec(objectName, updatedSoql, estimatedRowCount, splitIndex, splitCount);
    }

    @JsonProperty("objectName")
    public String getObjectName() {
        return objectName;
    }

    @JsonProperty("soqlQuery")
    public String getSoqlQuery() {
        return soqlQuery;
    }

    @JsonProperty("estimatedRowCount")
    public long getEstimatedRowCount() {
        return estimatedRowCount;
    }

    @JsonProperty("splitIndex")
    public int getSplitIndex() {
        return splitIndex;
    }

    @JsonProperty("splitCount")
    public int getSplitCount() {
        return splitCount;
    }

    @Override
    public String toString() {
        return "SalesforceScanSpec{"
                + "objectName='" + objectName + '\''
                + ", splitIndex=" + splitIndex
                + ", splitCount=" + splitCount
                + ", estimatedRowCount=" + estimatedRowCount
                + ", soql='" + soqlQuery + '\''
                + '}';
    }
}
