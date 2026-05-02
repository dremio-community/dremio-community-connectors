/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

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
import com.dremio.plugins.dataverse.DataverseConnection.DataverseEntity;
import com.dremio.plugins.dataverse.DataverseConnection.DataverseField;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Dremio StoragePlugin for Microsoft Dataverse.
 *
 * <p>Connects via Azure AD OAuth2 client credentials and exposes Dataverse entities as datasets.
 */
public class DataverseStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(DataverseStoragePlugin.class);

    private final DataverseConf conf;
    private final PluginSabotContext context;
    private final String name;
    private DataverseConnection connection;

    public DataverseStoragePlugin(DataverseConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new DataverseConnection(conf);
        connection.ensureAuthenticated();
        logger.info("DataverseStoragePlugin '{}' started for org: {}", name, conf.organizationUrl);
    }

    @Override
    public void close() throws Exception {
        logger.info("DataverseStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Dataverse connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.listEntities();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Dataverse health check failed: {}", e.getMessage());
            try {
                connection.authenticate();
                return SourceState.goodState();
            } catch (Exception ex) {
                return SourceState.badState("Dataverse connection error: " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return DataverseRulesFactory.class;
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
        try {
            List<DataverseEntity> entities = connection.listEntities();
            Set<String> excluded = buildExcludedSet();

            List<DatasetHandle> handles = new ArrayList<>();
            for (DataverseEntity entity : entities) {
                if (!excluded.contains(entity.logicalName.toLowerCase())) {
                    EntityPath path = new EntityPath(Arrays.asList(name, entity.logicalName));
                    handles.add(new DataverseDatasetHandle(path, entity));
                }
            }

            logger.debug("Listed {} Dataverse entities from source '{}'", handles.size(), name);
            return () -> handles.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list Dataverse entities", e);
        }
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) {
            return Optional.empty();
        }
        String logicalName = components.get(components.size() - 1);
        // Look up the entity set name for this logical name
        try {
            List<DataverseEntity> entities = connection.listEntities();
            for (DataverseEntity entity : entities) {
                if (entity.logicalName.equalsIgnoreCase(logicalName)) {
                    return Optional.of(new DataverseDatasetHandle(datasetPath, entity));
                }
            }
        } catch (IOException e) {
            throw new ConnectorException("Failed to resolve entity: " + logicalName, e);
        }
        // Fallback: assume entity set name is logical name + "s"
        DataverseEntity fallback = new DataverseEntity();
        fallback.logicalName = logicalName;
        fallback.entitySetName = logicalName + "s";
        fallback.displayName = logicalName;
        return Optional.of(new DataverseDatasetHandle(datasetPath, fallback));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        DataverseDatasetHandle dvHandle = (DataverseDatasetHandle) handle;
        try {
            List<DataverseField> fields = connection.describeEntity(dvHandle.getLogicalName());
            BatchSchema schema = DataverseTypeConverter.toBatchSchema(fields);
            return DatasetMetadata.of(DatasetStats.of(10_000L, 1.0), schema);
        } catch (IOException e) {
            throw new ConnectorException("Failed to get metadata for " + dvHandle.getLogicalName(), e);
        }
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        DataverseDatasetHandle dvHandle = (DataverseDatasetHandle) handle;
        try {
            List<DataverseField> fields = connection.describeEntity(dvHandle.getLogicalName());

            // Build $select list from field names
            StringJoiner selectJoiner = new StringJoiner(",");
            for (DataverseField f : fields) {
                selectJoiner.add(f.logicalName);
            }
            String selectClause = selectJoiner.length() > 0 ? selectJoiner.toString() : "*";

            String queryUrl = connection.getBaseUrl()
                    + dvHandle.getEntitySetName()
                    + "?$select=" + selectClause
                    + "&$top=" + conf.recordsPerPage;

            DataverseScanSpec spec = new DataverseScanSpec(
                    dvHandle.getLogicalName(),
                    dvHandle.getEntitySetName(),
                    queryUrl,
                    10_000L);

            // Encode spec as bytes for the split
            String specStr = dvHandle.getLogicalName() + "|" + dvHandle.getEntitySetName() + "|" + queryUrl;
            byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);

            DatasetSplit split = DatasetSplit.of(
                    Collections.emptyList(),
                    1024L * 1024L, // estimated size
                    10_000L,       // estimated rows
                    os -> os.write(specBytes));

            List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
            return () -> chunks.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list partition chunks for " + dvHandle.getLogicalName(), e);
        }
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) {
        return false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public DataverseConf getConf() { return conf; }
    public DataverseConnection getConnection() { return connection; }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Set<String> buildExcludedSet() {
        Set<String> excluded = new HashSet<>();
        if (conf.excludedEntities != null && !conf.excludedEntities.isBlank()) {
            for (String part : conf.excludedEntities.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    excluded.add(trimmed.toLowerCase());
                }
            }
        }
        return excluded;
    }

    // -------------------------------------------------------------------------
    // Inner handle class
    // -------------------------------------------------------------------------

    public static class DataverseDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final DataverseEntity entity;

        public DataverseDatasetHandle(EntityPath entityPath, DataverseEntity entity) {
            this.entityPath = entityPath;
            this.entity = entity;
        }

        @Override
        public EntityPath getDatasetPath() { return entityPath; }

        public String getLogicalName() { return entity.logicalName; }
        public String getEntitySetName() { return entity.entitySetName; }
        public String getDisplayName() { return entity.displayName; }
    }
}
