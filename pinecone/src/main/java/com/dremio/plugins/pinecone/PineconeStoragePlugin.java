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
import com.dremio.plugins.pinecone.PineconeConnection.IndexInfo;
import com.dremio.plugins.pinecone.PineconeSubScan.PineconeScanSpec;
import com.dremio.plugins.pinecone.PineconeTypeConverter.PineconeColumn;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PineconeStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(PineconeStoragePlugin.class);

    private final PineconeConf conf;
    private final PluginSabotContext context;
    private final String name;
    private PineconeConnection connection;

    /** Cached index list: name → IndexInfo. Populated at start(). */
    private Map<String, IndexInfo> indexCache = new HashMap<>();

    public PineconeStoragePlugin(PineconeConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new PineconeConnection(conf);
        connection.verify();

        // Cache index list
        List<IndexInfo> indexes = connection.listIndexes();
        indexCache = new HashMap<>();
        for (IndexInfo idx : indexes) {
            indexCache.put(idx.name, idx);
        }
        logger.info("PineconeStoragePlugin '{}' started with {} index(es): {}",
                name, indexes.size(), indexCache.keySet());
    }

    @Override
    public void close() throws Exception {
        logger.info("PineconeStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Pinecone connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.verify();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Pinecone health check failed: {}", e.getMessage());
            return SourceState.badState("Pinecone connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return PineconeRulesFactory.class;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
        return true;
    }

    // -------------------------------------------------------------------------
    // Dataset listing
    // -------------------------------------------------------------------------

    @Override
    public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
            throws ConnectorException {
        // Refresh index cache on each listing
        try {
            List<IndexInfo> indexes = connection.listIndexes();
            indexCache = new HashMap<>();
            for (IndexInfo idx : indexes) {
                indexCache.put(idx.name, idx);
            }
        } catch (IOException e) {
            throw new ConnectorException("Failed to list Pinecone indexes", e);
        }

        List<DatasetHandle> handles = new ArrayList<>();
        for (IndexInfo idx : indexCache.values()) {
            EntityPath path = new EntityPath(Arrays.asList(name, idx.name));
            handles.add(new PineconeDatasetHandle(path, idx.name, idx.host));
        }
        logger.debug("Listed {} Pinecone indexes from source '{}'", handles.size(), name);
        return () -> handles.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath,
            GetDatasetOption... options) throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) return Optional.empty();
        String indexName = components.get(components.size() - 1);

        IndexInfo idx = indexCache.get(indexName);
        if (idx == null) {
            // Try refreshing
            try {
                List<IndexInfo> indexes = connection.listIndexes();
                for (IndexInfo i : indexes) {
                    if (i.name.equals(indexName)) {
                        idx = i;
                        indexCache.put(i.name, i);
                        break;
                    }
                }
            } catch (IOException e) {
                logger.warn("Failed to refresh index cache for '{}': {}", indexName, e.getMessage());
            }
        }

        if (idx == null) {
            return Optional.of(new PineconeDatasetHandle(datasetPath, indexName, null));
        }
        return Optional.of(new PineconeDatasetHandle(datasetPath, indexName, idx.host));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        PineconeDatasetHandle pineconeHandle = (PineconeDatasetHandle) handle;
        String indexName = pineconeHandle.getIndexName();
        String host = pineconeHandle.getHost();

        List<PineconeColumn> columns;
        long estimatedRows = 100L;
        try {
            if (host != null) {
                List<JsonNode> sample = connection.sampleVectors(host, conf.namespace,
                        conf.sampleSize, indexName);
                columns = PineconeTypeConverter.inferColumns(sample);
                estimatedRows = estimateRowCount(host);
            } else {
                columns = PineconeTypeConverter.inferColumns(Collections.emptyList());
            }
        } catch (IOException e) {
            logger.warn("Schema inference failed for index '{}': {}", indexName, e.getMessage());
            columns = PineconeTypeConverter.inferColumns(Collections.emptyList());
        }

        BatchSchema schema = PineconeTypeConverter.buildSchema(columns);
        return DatasetMetadata.of(DatasetStats.of(estimatedRows, 1.0), schema);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
            ListPartitionChunkOption... options) throws ConnectorException {
        PineconeDatasetHandle pineconeHandle = (PineconeDatasetHandle) handle;
        String indexName = pineconeHandle.getIndexName();
        String host = pineconeHandle.getHost();

        long estimatedRows = 100L;
        try {
            if (host != null) {
                estimatedRows = estimateRowCount(host);
            }
        } catch (Exception e) {
            logger.warn("Row count estimate failed for '{}': {}", indexName, e.getMessage());
        }

        long sizeEstimate = Math.max(1024L, estimatedRows * 200L);
        PineconeScanSpec spec = new PineconeScanSpec(indexName, host != null ? host : "", estimatedRows);
        byte[] specBytes;
        try {
            specBytes = new ObjectMapper().writeValueAsString(spec)
                    .getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConnectorException("Failed to serialize scan spec for " + indexName, e);
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

    public PineconeConf getConf()               { return conf; }
    public PineconeConnection getConnection()   { return connection; }

    /** Returns cached host for the given index name, or null. */
    public String getIndexHost(String indexName) {
        IndexInfo idx = indexCache.get(indexName);
        return idx != null ? idx.host : null;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private long estimateRowCount(String host) {
        try {
            List<String> ids = connection.listVectorIds(host, conf.namespace, 1, null);
            String nextToken = connection.fetchNextToken(host, conf.namespace, 1, null);
            if (nextToken == null) {
                // Only one page — fetch all IDs for exact count
                List<String> all = connection.listVectorIds(host, conf.namespace, 100, null);
                return all.size();
            }
            return 1000L; // Has multiple pages — conservative estimate
        } catch (Exception e) {
            return 100L;
        }
    }

    // -------------------------------------------------------------------------
    // Inner handle
    // -------------------------------------------------------------------------

    public static class PineconeDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String indexName;
        private final String host;

        public PineconeDatasetHandle(EntityPath entityPath, String indexName, String host) {
            this.entityPath = entityPath;
            this.indexName  = indexName;
            this.host       = host;
        }

        @Override
        public EntityPath getDatasetPath() { return entityPath; }
        public String getIndexName()       { return indexName; }
        public String getHost()            { return host; }
    }
}
