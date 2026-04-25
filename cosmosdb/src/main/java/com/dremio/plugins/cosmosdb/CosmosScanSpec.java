/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.cosmosdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Serializable descriptor for a Cosmos DB container scan.
 *
 * <p>Carries the container name plus an optional partition key value for
 * single-partition routing (set when a WHERE clause equality on the partition
 * key field is detected at planning time).
 */
public class CosmosScanSpec {

    private final String containerName;
    private final String sql;

    @JsonCreator
    public CosmosScanSpec(
            @JsonProperty("containerName") String containerName,
            @JsonProperty("sql")           String sql) {
        this.containerName = containerName;
        this.sql = sql;
    }

    @JsonProperty("containerName")
    public String getContainerName() { return containerName; }

    @JsonProperty("sql")
    public String getSql() { return sql; }
}
