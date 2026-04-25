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

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Azure Cosmos DB (NoSQL API) source configuration.
 *
 * <p>Connects via the Cosmos DB REST API using HMAC-SHA256 master key auth.
 * Each container in the configured database is exposed as a Dremio table.
 * Schema is inferred by sampling documents at metadata refresh time.
 */
@SourceType(value = "COSMOS_DB", label = "Azure Cosmos DB", uiConfig = "cosmosdb-layout.json")
public class CosmosConf extends ConnectionConf<CosmosConf, CosmosStoragePlugin> {

    /** Cosmos DB account endpoint, e.g. https://myaccount.documents.azure.com:443 */
    @Tag(1)
    public String endpoint;

    /** Database name to expose. Each container in this database becomes a table. */
    @Tag(2)
    public String database;

    /** Primary or secondary master key from the Azure portal. */
    @Tag(3)
    @Secret
    public String masterKey = "";

    /** Documents per API page (max 1000 per Cosmos DB limits). */
    @Tag(4)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** Number of documents sampled per container to infer schema. */
    @Tag(5)
    @NotMetadataImpacting
    public int schemaSampleSize = 100;

    /** HTTP request timeout in seconds. */
    @Tag(6)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    @Override
    public CosmosStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new CosmosStoragePlugin(this, context, name);
    }
}
