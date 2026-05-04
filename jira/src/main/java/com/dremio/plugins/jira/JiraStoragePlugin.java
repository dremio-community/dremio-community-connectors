/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.jira;

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
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.jira.JiraSubScan.JiraScanSpec;
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

public class JiraStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(JiraStoragePlugin.class);

    private final JiraConf conf;
    private final PluginSabotContext context;
    private final String name;
    private JiraConnection connection;

    public JiraStoragePlugin(JiraConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    @Override
    public void start() throws IOException {
        connection = new JiraConnection(conf);
        connection.verifyConnection();
        logger.info("JiraStoragePlugin '{}' started", name);
    }

    @Override
    public void close() throws Exception {
        logger.info("JiraStoragePlugin '{}' closed", name);
    }

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Jira connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.verifyConnection();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Jira health check failed: {}", e.getMessage());
            return SourceState.badState("Jira connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return JiraRulesFactory.class;
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
        for (String tableName : JiraTableDef.ALL_TABLES) {
            EntityPath path = new EntityPath(Arrays.asList(name, tableName));
            handles.add(new JiraDatasetHandle(path, tableName));
        }
        return () -> handles.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) return Optional.empty();
        String tableName = components.get(components.size() - 1);
        if (!JiraTableDef.ALL_TABLES.contains(tableName)) return Optional.empty();
        return Optional.of(new JiraDatasetHandle(datasetPath, tableName));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(DatasetHandle handle, PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        JiraDatasetHandle jHandle = (JiraDatasetHandle) handle;
        JiraTableDef def = JiraTableDef.fromName(jHandle.getTableName())
                .orElseThrow(() -> new ConnectorException("Unknown table: " + jHandle.getTableName()));
        long estimatedRows = connection.estimateCount(jHandle.getTableName());
        return DatasetMetadata.of(DatasetStats.of(estimatedRows, 1.0), def.getSchema());
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        JiraDatasetHandle jHandle = (JiraDatasetHandle) handle;
        long estimatedRows = connection.estimateCount(jHandle.getTableName());
        long sizeEstimate = Math.max(1024L, estimatedRows * 200L);

        JiraScanSpec spec = new JiraScanSpec(jHandle.getTableName(), estimatedRows);
        try {
            String specJson = new ObjectMapper().writeValueAsString(spec);
            byte[] specBytes = specJson.getBytes(StandardCharsets.UTF_8);
            DatasetSplit split = DatasetSplit.of(
                    Collections.emptyList(), sizeEstimate, estimatedRows,
                    os -> os.write(specBytes));
            List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
            return () -> chunks.iterator();
        } catch (Exception e) {
            throw new ConnectorException("Failed to create partition chunk for " + jHandle.getTableName(), e);
        }
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) {
        return false;
    }

    public JiraConf getConf() {
        return conf;
    }

    public JiraConnection getConnection() {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Dataset handle
    // -------------------------------------------------------------------------

    public static class JiraDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String tableName;

        public JiraDatasetHandle(EntityPath entityPath, String tableName) {
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
