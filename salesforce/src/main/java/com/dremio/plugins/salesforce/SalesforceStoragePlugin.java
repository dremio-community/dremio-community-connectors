/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

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
import com.dremio.plugins.salesforce.SalesforceConnection.SalesforceField;
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

/**
 * Dremio StoragePlugin for Salesforce.
 *
 * <p>Connects via OAuth2 password flow and exposes queryable SObjects as Dremio datasets.
 */
public class SalesforceStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceStoragePlugin.class);

    private final SalesforceConf conf;
    private final PluginSabotContext context;
    private final String name;
    private SalesforceConnection connection;

    public SalesforceStoragePlugin(SalesforceConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new SalesforceConnection(conf);
        connection.ensureAuthenticated();
        logger.info("SalesforceStoragePlugin '{}' started", name);
    }

    @Override
    public void close() throws Exception {
        logger.info("SalesforceStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("Salesforce connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.listSObjects();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("Salesforce health check failed: {}", e.getMessage());
            try {
                connection.authenticate();
                return SourceState.goodState();
            } catch (Exception ex) {
                return SourceState.badState("Salesforce connection error: " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return SalesforceRulesFactory.class;
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
            List<String> sobjects = connection.listSObjects();
            Set<String> excluded = buildExcludedSet();

            List<DatasetHandle> handles = new ArrayList<>();
            for (String objectName : sobjects) {
                if (!excluded.contains(objectName.toLowerCase())) {
                    EntityPath path = new EntityPath(Arrays.asList(name, objectName));
                    handles.add(new SalesforceDatasetHandle(path, objectName));
                }
            }

            logger.debug("Listed {} queryable Salesforce SObjects from source '{}'", handles.size(), name);
            return () -> handles.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list Salesforce SObjects", e);
        }
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) {
            return Optional.empty();
        }
        String objectName = components.get(components.size() - 1);
        return Optional.of(new SalesforceDatasetHandle(datasetPath, objectName));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        SalesforceDatasetHandle sfHandle = (SalesforceDatasetHandle) handle;
        try {
            List<SalesforceField> fields = connection.describeSObject(sfHandle.getObjectName());
            BatchSchema schema = SalesforceTypeConverter.toBatchSchema(fields);

            long estimatedRows;
            try {
                estimatedRows = connection.countQuery(sfHandle.getObjectName(), "");
            } catch (Exception e) {
                logger.warn("Could not count rows for {}: {}", sfHandle.getObjectName(), e.getMessage());
                estimatedRows = 10_000L;
            }

            final long finalEstimatedRows = estimatedRows;
            return DatasetMetadata.of(
                    DatasetStats.of(finalEstimatedRows, 1.0),
                    schema);
        } catch (IOException e) {
            throw new ConnectorException("Failed to get metadata for " + sfHandle.getObjectName(), e);
        }
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        SalesforceDatasetHandle sfHandle = (SalesforceDatasetHandle) handle;
        try {
            List<SalesforceField> fields = connection.describeSObject(sfHandle.getObjectName());

            long estimatedRows;
            try {
                estimatedRows = connection.countQuery(sfHandle.getObjectName(), "");
            } catch (Exception e) {
                logger.warn("Could not count rows for {}, defaulting to 10000", sfHandle.getObjectName());
                estimatedRows = 10_000L;
            }

            String objectName = sfHandle.getObjectName();
            int splitCount = conf.splitParallelism;
            long rowsPerSplit = Math.max(1, estimatedRows / splitCount);
            long sizePerSplit = Math.max(1024L, rowsPerSplit * 200L); // ~200 bytes/row estimate

            // Build SELECT field list
            StringBuilder selectFields = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) selectFields.append(", ");
                selectFields.append(fields.get(i).name);
            }
            String selectFieldsStr = selectFields.length() > 0 ? selectFields.toString() : "Id";

            List<PartitionChunk> chunks = new ArrayList<>(splitCount);
            for (int seg = 0; seg < splitCount; seg++) {
                long offset = (long) seg * rowsPerSplit;
                long limit = (seg == splitCount - 1)
                        ? Math.max(rowsPerSplit, estimatedRows - offset)
                        : rowsPerSplit;

                String soql = "SELECT " + selectFieldsStr + " FROM " + objectName
                        + " LIMIT " + limit + " OFFSET " + offset;

                // Encode: "objectName|segment|totalSegments|soql"
                String specStr = objectName + "|" + seg + "|" + splitCount + "|" + soql;
                byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);

                DatasetSplit split = DatasetSplit.of(
                        Collections.emptyList(),
                        sizePerSplit, rowsPerSplit,
                        os -> os.write(specBytes));
                chunks.add(PartitionChunk.of(split));
            }
            return () -> chunks.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list partition chunks for " + sfHandle.getObjectName(), e);
        }
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) {
        return false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public SalesforceConf getConf() {
        return conf;
    }

    public SalesforceConnection getConnection() {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Set<String> buildExcludedSet() {
        Set<String> excluded = new HashSet<>();
        if (conf.excludedObjects != null && !conf.excludedObjects.isBlank()) {
            for (String part : conf.excludedObjects.split(",")) {
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

    /**
     * Lightweight dataset handle that carries the SObject name.
     */
    public static class SalesforceDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String objectName;

        public SalesforceDatasetHandle(EntityPath entityPath, String objectName) {
            this.entityPath = entityPath;
            this.objectName = objectName;
        }

        @Override
        public EntityPath getDatasetPath() {
            return entityPath;
        }

        public String getObjectName() {
            return objectName;
        }
    }
}
