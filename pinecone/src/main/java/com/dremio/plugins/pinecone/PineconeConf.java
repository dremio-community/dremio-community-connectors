/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.pinecone;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Pinecone source configuration.
 *
 * <p>Authenticates via a Pinecone API key.
 * Dynamically discovers indexes from the control plane and exposes each as a SQL table.
 */
@SourceType(value = "PINECONE", label = "Pinecone", uiConfig = "pinecone-layout.json")
public class PineconeConf extends ConnectionConf<PineconeConf, PineconeStoragePlugin> {

    /** Pinecone API key. */
    @Tag(1)
    @Secret
    public String apiKey;

    /** Pinecone control plane URL. Override for local mock testing. */
    @Tag(2)
    @NotMetadataImpacting
    public String controlPlaneUrl = "https://api.pinecone.io";

    /** Namespace to query within each index. Empty string = default namespace. */
    @Tag(3)
    @NotMetadataImpacting
    public String namespace = "";

    /** Number of vector IDs to fetch per list call. */
    @Tag(4)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** Number of vectors to sample for schema inference. */
    @Tag(5)
    @NotMetadataImpacting
    public int sampleSize = 20;

    /** HTTP timeout in seconds for calls to Pinecone. */
    @Tag(6)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 60;

    @Override
    public PineconeStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new PineconeStoragePlugin(this, context, name);
    }
}
