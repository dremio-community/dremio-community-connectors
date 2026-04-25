/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.zendesk;

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
import com.dremio.plugins.zendesk.ZendeskConnection.ZendeskTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Dremio StoragePlugin for Zendesk.
 *
 * <p>Exposes six fixed tables: tickets, users, organizations, groups,
 * ticket_metrics, satisfaction_ratings.
 */
public class ZendeskStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(ZendeskStoragePlugin.class);

    private final ZendeskConf conf;
    private final PluginSabotContext context;
    private final String name;
    private ZendeskConnection connection;

    public ZendeskStoragePlugin(ZendeskConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new ZendeskConnection(conf);
        try {
            connection.testConnection();
        } catch (Exception e) {
            logger.warn("Zendesk connection test failed during start (non-fatal): {}", e.getMessage());
        }
        logger.info("ZendeskStoragePlugin '{}' started for subdomain '{}'", name, conf.subdomain);
    }

    @Override
    public void close() throws Exception {
        logger.info("ZendeskStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Zendesk connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.testConnection();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Zendesk health check failed: {}", e.getMessage());
            return SourceState.badState("Zendesk connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return ZendeskRulesFactory.class;
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
        for (String tableName : ZendeskConnection.getTables().keySet()) {
            EntityPath path = new EntityPath(Arrays.asList(name, tableName));
            handles.add(new ZendeskDatasetHandle(path, tableName));
        }
        logger.debug("Listed {} Zendesk tables from source '{}'", handles.size(), name);
        return () -> handles.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) {
            return Optional.empty();
        }
        String tableName = components.get(components.size() - 1).toLowerCase();
        if (!ZendeskConnection.getTables().containsKey(tableName)) {
            return Optional.empty();
        }
        return Optional.of(new ZendeskDatasetHandle(datasetPath, tableName));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        ZendeskDatasetHandle zdHandle = (ZendeskDatasetHandle) handle;
        ZendeskTable table = ZendeskConnection.getTable(zdHandle.getTableName());

        List<Field> arrowFields = new ArrayList<>();
        for (ZendeskConnection.ZendeskField f : table.fields) {
            arrowFields.add(f.toArrowField());
        }
        BatchSchema schema = BatchSchema.newBuilder().addFields(arrowFields).build();

        return DatasetMetadata.of(DatasetStats.of(10_000L, 1.0), schema);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        ZendeskDatasetHandle zdHandle = (ZendeskDatasetHandle) handle;
        String tableName = zdHandle.getTableName();

        // Single split per table — encode table name as split bytes
        byte[] specBytes = tableName.getBytes(StandardCharsets.UTF_8);
        long estimatedSize = 10_000L * 200L; // ~200 bytes/row estimate

        DatasetSplit split = DatasetSplit.of(
                Collections.emptyList(),
                estimatedSize, 10_000L,
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

    public ZendeskConf getConf() {
        return conf;
    }

    public ZendeskConnection getConnection() {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Inner handle class
    // -------------------------------------------------------------------------

    public static class ZendeskDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String tableName;

        public ZendeskDatasetHandle(EntityPath entityPath, String tableName) {
            this.entityPath = entityPath;
            this.tableName = tableName;
        }

        @Override
        public EntityPath getDatasetPath() {
            return entityPath;
        }

        public String getTableName() {
            return tableName;
        }
    }
}
