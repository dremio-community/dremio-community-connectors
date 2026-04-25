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
import com.dremio.plugins.cosmosdb.CosmosConnection.CosmosField;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Dremio StoragePlugin for Azure Cosmos DB (NoSQL API).
 *
 * <p>Each container in the configured database is listed as a dataset.
 * Schema is inferred by sampling documents at metadata refresh time.
 */
public class CosmosStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(CosmosStoragePlugin.class);

    private final CosmosConf conf;
    private final String name;
    private CosmosConnection connection;

    public CosmosStoragePlugin(CosmosConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.name = name;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void start() throws IOException {
        connection = new CosmosConnection(conf);
        connection.testConnection();
        logger.info("CosmosStoragePlugin '{}' started — database={}", name, conf.database);
    }

    @Override
    public void close() throws Exception {
        logger.info("CosmosStoragePlugin '{}' closed", name);
    }

    // ── Status ────────────────────────────────────────────────────────────────

    @Override
    public SourceState getState() {
        if (connection == null) return SourceState.badState("Not initialized",
                new RuntimeException("Plugin not started"));
        try {
            connection.testConnection();
            return SourceState.goodState();
        } catch (Exception e) {
            return SourceState.badState("Cosmos DB connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() { return SourceCapabilities.NONE; }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return CosmosRulesFactory.class;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig cfg) {
        return true;
    }

    // ── Dataset listing ───────────────────────────────────────────────────────

    @Override
    public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
            throws ConnectorException {
        try {
            List<String> containers = connection.listContainers();
            List<DatasetHandle> handles = containers.stream()
                    .map(c -> new CosmosDatasetHandle(new EntityPath(Arrays.asList(name, c)), c))
                    .collect(Collectors.toList());
            logger.debug("Listed {} containers in database '{}'", handles.size(), conf.database);
            return () -> handles.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list Cosmos DB containers", e);
        }
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
            throws ConnectorException {
        List<String> parts = path.getComponents();
        if (parts.isEmpty()) return Optional.empty();
        String container = parts.get(parts.size() - 1);
        return Optional.of(new CosmosDatasetHandle(path, container));
    }

    // ── Metadata (schema inference) ───────────────────────────────────────────

    @Override
    public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
            PartitionChunkListing chunkListing, GetMetadataOption... options)
            throws ConnectorException {
        CosmosDatasetHandle ch = (CosmosDatasetHandle) handle;
        try {
            List<CosmosField> fields = connection.inferSchema(ch.getContainer());
            BatchSchema schema = toBatchSchema(fields);
            return DatasetMetadata.of(DatasetStats.of(10_000L, 1.0), schema);
        } catch (IOException e) {
            throw new ConnectorException("Failed to infer schema for container " + ch.getContainer(), e);
        }
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
            ListPartitionChunkOption... options) throws ConnectorException {
        CosmosDatasetHandle ch = (CosmosDatasetHandle) handle;
        try {
            List<CosmosField> fields = connection.inferSchema(ch.getContainer());
            String selectFields = fields.isEmpty() ? "*" : fields.stream()
                    .map(f -> "c." + toCosmosPath(f.name))
                    .collect(Collectors.joining(", "));
            String sql = "SELECT " + selectFields + " FROM c";

            String specStr = ch.getContainer() + "|" + sql;
            byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);
            DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1024L * 1024L, 10_000L,
                    os -> os.write(specBytes));
            return () -> Collections.singletonList(PartitionChunk.of(split)).iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list partition chunks for " + ch.getContainer(), e);
        }
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) { return false; }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public CosmosConf getConf() { return conf; }
    public CosmosConnection getConnection() { return connection; }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static BatchSchema toBatchSchema(List<CosmosField> fields) {
        List<Field> arrowFields = new ArrayList<>();
        for (CosmosField f : fields) {
            arrowFields.add(f.toArrowField());
        }
        return BatchSchema.newBuilder().addFields(arrowFields).build();
    }

    /**
     * Converts a flattened column name back to a Cosmos DB SQL path.
     * "contact_email" stays as is in SELECT — Cosmos DB can't navigate nested
     * paths in the field selector this way; we extract them in the RecordReader instead.
     * We SELECT * FROM c and flatten in Java.
     */
    static String toCosmosPath(String flatName) {
        return flatName;
    }

    // ── Inner dataset handle ──────────────────────────────────────────────────

    public static class CosmosDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String container;

        public CosmosDatasetHandle(EntityPath entityPath, String container) {
            this.entityPath = entityPath;
            this.container = container;
        }

        @Override public EntityPath getDatasetPath() { return entityPath; }
        public String getContainer() { return container; }
    }
}
