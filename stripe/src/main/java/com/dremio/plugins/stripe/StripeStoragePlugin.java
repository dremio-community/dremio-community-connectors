/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.stripe;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.stripe.StripeSubScan.StripeScanSpec;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class StripeStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(StripeStoragePlugin.class);

    private final StripeConf conf;
    private final PluginSabotContext context;
    private final String name;
    private StripeConnection connection;

    public StripeStoragePlugin(StripeConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new StripeConnection(conf);
        connection.verify();
        logger.info("StripeStoragePlugin '{}' started", name);
    }

    @Override
    public void close() throws Exception {
        logger.info("StripeStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Stripe connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.verify();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Stripe health check failed: {}", e.getMessage());
            return SourceState.badState("Stripe connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return StripeRulesFactory.class;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
        return true;
    }

    // -------------------------------------------------------------------------
    // Dataset listing
    // -------------------------------------------------------------------------

    @Override
    public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) throws ConnectorException {
        List<DatasetHandle> handles = new ArrayList<>();
        for (String table : StripeConnection.TABLES) {
            EntityPath path = new EntityPath(Arrays.asList(name, table));
            handles.add(new StripeDatasetHandle(path, table));
        }
        logger.debug("Listed {} Stripe tables from source '{}'", handles.size(), name);
        return () -> handles.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) return Optional.empty();
        String table = components.get(components.size() - 1);
        return Optional.of(new StripeDatasetHandle(datasetPath, table));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        StripeDatasetHandle stripeHandle = (StripeDatasetHandle) handle;
        BatchSchema schema = StripeTypeConverter.schemaFor(stripeHandle.getTable());
        long estimatedRows = connection.estimateCount(stripeHandle.getTable());
        return DatasetMetadata.of(DatasetStats.of(estimatedRows, 1.0), schema);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        StripeDatasetHandle stripeHandle = (StripeDatasetHandle) handle;
        String table = stripeHandle.getTable();
        long estimatedRows = connection.estimateCount(table);
        long sizeEstimate = Math.max(1024L, estimatedRows * 200L);

        StripeScanSpec spec = new StripeScanSpec(table, estimatedRows);
        byte[] specBytes;
        try {
            specBytes = new ObjectMapper().writeValueAsString(spec).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConnectorException("Failed to serialize scan spec for " + table, e);
        }

        DatasetSplit split = DatasetSplit.of(
                Collections.emptyList(), sizeEstimate, estimatedRows,
                os -> os.write(specBytes));
        List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
        return () -> chunks.iterator();
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) {
        return false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public StripeConf getConf()             { return conf; }
    public StripeConnection getConnection() { return connection; }

    // -------------------------------------------------------------------------
    // Inner handle
    // -------------------------------------------------------------------------

    public static class StripeDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String table;

        public StripeDatasetHandle(EntityPath entityPath, String table) {
            this.entityPath = entityPath;
            this.table = table;
        }

        @Override
        public EntityPath getDatasetPath() { return entityPath; }
        public String getTable()           { return table; }
    }
}
