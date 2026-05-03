/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.hubspot;

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
import com.dremio.plugins.hubspot.HubSpotConnection.HubSpotProperty;
import com.dremio.plugins.hubspot.HubSpotSubScan.HubSpotScanSpec;
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
import java.util.List;
import java.util.Optional;

public class HubSpotStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotStoragePlugin.class);

    private final HubSpotConf conf;
    private final PluginSabotContext context;
    private final String name;
    private HubSpotConnection connection;

    public HubSpotStoragePlugin(HubSpotConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new HubSpotConnection(conf);
        connection.verifyToken();
        logger.info("HubSpotStoragePlugin '{}' started", name);
    }

    @Override
    public void close() throws Exception {
        logger.info("HubSpotStoragePlugin '{}' closed", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("HubSpot connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.verifyToken();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("HubSpot health check failed: {}", e.getMessage());
            return SourceState.badState("HubSpot connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return HubSpotRulesFactory.class;
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
        List<String> objectTypes = new ArrayList<>(HubSpotConnection.STANDARD_OBJECTS);
        objectTypes.add(HubSpotConnection.OWNERS_OBJECT);

        List<DatasetHandle> handles = new ArrayList<>();
        for (String objectType : objectTypes) {
            EntityPath path = new EntityPath(Arrays.asList(name, objectType));
            handles.add(new HubSpotDatasetHandle(path, objectType));
        }
        logger.debug("Listed {} HubSpot object types from source '{}'", handles.size(), name);
        return () -> handles.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options)
            throws ConnectorException {
        List<String> components = datasetPath.getComponents();
        if (components.isEmpty()) return Optional.empty();
        String objectType = components.get(components.size() - 1);
        return Optional.of(new HubSpotDatasetHandle(datasetPath, objectType));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        HubSpotDatasetHandle hsHandle = (HubSpotDatasetHandle) handle;
        try {
            BatchSchema schema = buildSchema(hsHandle.getObjectType());
            long estimatedRows = connection.estimateCount(hsHandle.getObjectType());
            return DatasetMetadata.of(DatasetStats.of(estimatedRows, 1.0), schema);
        } catch (IOException e) {
            throw new ConnectorException("Failed to get metadata for " + hsHandle.getObjectType(), e);
        }
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        HubSpotDatasetHandle hsHandle = (HubSpotDatasetHandle) handle;
        try {
            String objectType = hsHandle.getObjectType();
            long estimatedRows = connection.estimateCount(objectType);
            long sizeEstimate = Math.max(1024L, estimatedRows * 250L);

            List<String> propertyNames;
            if (HubSpotConnection.OWNERS_OBJECT.equals(objectType)) {
                propertyNames = Collections.emptyList();
            } else {
                List<HubSpotProperty> props = connection.getProperties(objectType);
                propertyNames = new ArrayList<>(props.size());
                for (HubSpotProperty p : props) {
                    propertyNames.add(p.name);
                }
            }

            HubSpotScanSpec spec = new HubSpotScanSpec(objectType, propertyNames, estimatedRows);
            String specJson = toJson(spec);
            byte[] specBytes = specJson.getBytes(StandardCharsets.UTF_8);

            DatasetSplit split = DatasetSplit.of(
                    Collections.emptyList(), sizeEstimate, estimatedRows,
                    os -> os.write(specBytes));
            List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
            return () -> chunks.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list partition chunks for " + hsHandle.getObjectType(), e);
        }
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) {
        return false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public HubSpotConf getConf() {
        return conf;
    }

    public HubSpotConnection getConnection() {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private BatchSchema buildSchema(String objectType) throws IOException {
        if (HubSpotConnection.OWNERS_OBJECT.equals(objectType)) {
            return HubSpotTypeConverter.toOwnerSchema();
        }
        List<HubSpotProperty> props = connection.getProperties(objectType);
        return HubSpotTypeConverter.toSchema(props);
    }

    private String toJson(HubSpotScanSpec spec) throws IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.writeValueAsString(spec);
    }

    // -------------------------------------------------------------------------
    // Inner handle class
    // -------------------------------------------------------------------------

    public static class HubSpotDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String objectType;

        public HubSpotDatasetHandle(EntityPath entityPath, String objectType) {
            this.entityPath = entityPath;
            this.objectType = objectType;
        }

        @Override
        public EntityPath getDatasetPath() {
            return entityPath;
        }

        public String getObjectType() {
            return objectType;
        }
    }
}
