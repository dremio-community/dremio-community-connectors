/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.influxdb;

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
import com.dremio.plugins.influxdb.InfluxDBConnection.InfluxField;
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
import java.util.stream.Collectors;

/**
 * Dremio StoragePlugin for InfluxDB 3.
 *
 * <p>Each measurement in the configured database is listed as a dataset.
 * Schema is derived from information_schema.columns at metadata refresh time.
 */
public class InfluxDBStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBStoragePlugin.class);

    private final InfluxDBConf conf;
    private final String name;
    private InfluxDBConnection connection;

    public InfluxDBStoragePlugin(InfluxDBConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.name = name;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void start() throws IOException {
        connection = new InfluxDBConnection(conf);
        connection.testConnection();
        logger.info("InfluxDBStoragePlugin '{}' started — host={} database={}", name, conf.host, conf.database);
    }

    @Override
    public void close() throws Exception {
        logger.info("InfluxDBStoragePlugin '{}' closed", name);
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
            return SourceState.badState("InfluxDB connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() { return SourceCapabilities.NONE; }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return InfluxDBRulesFactory.class;
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
            List<String> measurements = connection.listMeasurements();
            List<DatasetHandle> handles = measurements.stream()
                    .map(m -> new InfluxDBDatasetHandle(new EntityPath(Arrays.asList(name, m)), m))
                    .collect(Collectors.toList());
            logger.debug("Listed {} measurements in database '{}'", handles.size(), conf.database);
            return () -> handles.iterator();
        } catch (IOException e) {
            throw new ConnectorException("Failed to list InfluxDB measurements", e);
        }
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
            throws ConnectorException {
        List<String> parts = path.getComponents();
        if (parts.isEmpty()) return Optional.empty();
        String measurement = parts.get(parts.size() - 1);
        return Optional.of(new InfluxDBDatasetHandle(path, measurement));
    }

    // ── Metadata (schema) ─────────────────────────────────────────────────────

    @Override
    public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
            PartitionChunkListing chunkListing, GetMetadataOption... options)
            throws ConnectorException {
        InfluxDBDatasetHandle dh = (InfluxDBDatasetHandle) handle;
        try {
            List<InfluxField> fields = connection.inferSchema(dh.getMeasurement());
            BatchSchema schema = toBatchSchema(fields);
            return DatasetMetadata.of(DatasetStats.of(10_000L, 1.0), schema);
        } catch (IOException e) {
            throw new ConnectorException("Failed to infer schema for measurement " + dh.getMeasurement(), e);
        }
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
            ListPartitionChunkOption... options) throws ConnectorException {
        InfluxDBDatasetHandle dh = (InfluxDBDatasetHandle) handle;
        byte[] specBytes = dh.getMeasurement().getBytes(StandardCharsets.UTF_8);
        DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1024L * 1024L, 10_000L,
                os -> os.write(specBytes));
        return () -> Collections.singletonList(PartitionChunk.of(split)).iterator();
    }

    @Override
    public boolean containerExists(EntityPath path, GetMetadataOption... options) { return false; }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public InfluxDBConf getConf() { return conf; }
    public InfluxDBConnection getConnection() { return connection; }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static BatchSchema toBatchSchema(List<InfluxField> fields) {
        List<Field> arrowFields = new ArrayList<>();
        for (InfluxField f : fields) {
            arrowFields.add(f.toArrowField());
        }
        return BatchSchema.newBuilder().addFields(arrowFields).build();
    }

    // ── Inner dataset handle ──────────────────────────────────────────────────

    public static class InfluxDBDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String measurement;

        public InfluxDBDatasetHandle(EntityPath entityPath, String measurement) {
            this.entityPath = entityPath;
            this.measurement = measurement;
        }

        @Override public EntityPath getDatasetPath() { return entityPath; }
        public String getMeasurement() { return measurement; }
    }
}
