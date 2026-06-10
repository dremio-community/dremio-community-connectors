package com.dremio.plugins.pagerduty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Describes a PagerDuty table scan.
 */
public class PagerDutyScanSpec {

    private final String tableName;
    private final long estimatedRowCount;

    @JsonCreator
    public PagerDutyScanSpec(
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
        return "PagerDutyScanSpec{tableName='" + tableName + "', estimatedRowCount=" + estimatedRowCount + '}';
    }
}
