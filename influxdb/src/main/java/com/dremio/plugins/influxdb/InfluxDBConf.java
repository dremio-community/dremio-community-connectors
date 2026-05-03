/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.influxdb;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * InfluxDB 3 source configuration.
 *
 * <p>Connects via the InfluxDB 3 HTTP SQL API using Bearer token auth.
 * Each measurement in the configured database is exposed as a Dremio table.
 * Schema is derived from information_schema.columns at metadata refresh time.
 */
@SourceType(value = "INFLUXDB", label = "InfluxDB", uiConfig = "influxdb-layout.json")
public class InfluxDBConf extends ConnectionConf<InfluxDBConf, InfluxDBStoragePlugin> {

    /** InfluxDB host URL including scheme and port, e.g. http://localhost:8181 */
    @Tag(1)
    public String host = "http://localhost:8181";

    /** Database name to expose. Each measurement becomes a table. */
    @Tag(2)
    public String database;

    /** InfluxDB API token for Bearer authentication. */
    @Tag(3)
    @Secret
    public String token = "";

    /** Rows fetched per SQL page via LIMIT/OFFSET. */
    @Tag(4)
    @NotMetadataImpacting
    public int pageSize = 1000;

    /** HTTP request timeout in seconds. */
    @Tag(5)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    @Override
    public InfluxDBStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new InfluxDBStoragePlugin(this, context, name);
    }
}
