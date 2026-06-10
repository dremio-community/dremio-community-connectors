package com.dremio.plugins.pagerduty;

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
import com.dremio.plugins.pagerduty.PagerDutyConnection.PagerDutyTable;
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
 * Dremio StoragePlugin for PagerDuty.
 */
public class PagerDutyStoragePlugin implements StoragePlugin, SupportsListingDatasets {

    private static final Logger logger = LoggerFactory.getLogger(PagerDutyStoragePlugin.class);

    private final PagerDutyConf conf;
    private final PluginSabotContext context;
    private final String name;
    private PagerDutyConnection connection;

    public PagerDutyStoragePlugin(PagerDutyConf conf, PluginSabotContext context, String name) {
        this.conf = conf;
        this.context = context;
        this.name = name;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws IOException {
        connection = new PagerDutyConnection(conf);
        try {
            connection.testConnection();
        } catch (Exception e) {
            logger.warn("PagerDuty connection test failed during start (non-fatal): {}", e.getMessage());
        }
        logger.info("PagerDutyStoragePlugin '{}' started.", name);
    }

    @Override
    public void close() throws Exception {
        logger.info("PagerDutyStoragePlugin '{}' closed.", name);
    }

    // -------------------------------------------------------------------------
    // Status + capabilities
    // -------------------------------------------------------------------------

    @Override
    public SourceState getState() {
        if (connection == null) {
            return SourceState.badState("PagerDuty connection not initialized",
                    new RuntimeException("Plugin not started"));
        }
        try {
            connection.testConnection();
            return SourceState.goodState();
        } catch (Exception e) {
            logger.warn("PagerDuty health check failed: {}", e.getMessage());
            return SourceState.badState("PagerDuty connection error: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return SourceCapabilities.NONE;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return PagerDutyRulesFactory.class;
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
        for (String tableName : PagerDutyConnection.getTables().keySet()) {
            EntityPath path = new EntityPath(Arrays.asList(name, tableName));
            handles.add(new PagerDutyDatasetHandle(path, tableName));
        }
        logger.debug("Listed {} PagerDuty tables from source '{}'", handles.size(), name);
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
        if (!PagerDutyConnection.getTables().containsKey(tableName)) {
            return Optional.empty();
        }
        return Optional.of(new PagerDutyDatasetHandle(datasetPath, tableName));
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatasetMetadata getDatasetMetadata(
            DatasetHandle handle,
            PartitionChunkListing chunkListing,
            GetMetadataOption... options) throws ConnectorException {
        PagerDutyDatasetHandle pdHandle = (PagerDutyDatasetHandle) handle;
        PagerDutyTable table = PagerDutyConnection.getTable(pdHandle.getTableName());

        List<Field> arrowFields = new ArrayList<>();
        for (PagerDutyConnection.PagerDutyField f : table.fields) {
            arrowFields.add(f.toArrowField());
        }
        BatchSchema schema = BatchSchema.newBuilder().addFields(arrowFields).build();

        return DatasetMetadata.of(DatasetStats.of(10_000L, 1.0), schema);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle handle, ListPartitionChunkOption... options)
            throws ConnectorException {
        PagerDutyDatasetHandle pdHandle = (PagerDutyDatasetHandle) handle;
        String tableName = pdHandle.getTableName();

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

    public PagerDutyConf getConf() {
        return conf;
    }

    public PagerDutyConnection getConnection() {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Inner handle class
    // -------------------------------------------------------------------------

    public static class PagerDutyDatasetHandle implements DatasetHandle {
        private final EntityPath entityPath;
        private final String tableName;

        public PagerDutyDatasetHandle(EntityPath entityPath, String tableName) {
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
