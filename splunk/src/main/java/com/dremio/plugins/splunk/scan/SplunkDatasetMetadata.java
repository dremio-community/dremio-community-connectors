package com.dremio.plugins.splunk.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

/**
 * Metadata for a Splunk index: Arrow schema + event count estimate.
 *
 * The schema always contains metadata fields (_time, _raw, _index, _sourcetype,
 * _source, _host) plus inferred payload fields from schema sampling.
 */
public class SplunkDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String indexName;

  public SplunkDatasetMetadata(BatchSchema schema, DatasetStats stats, String indexName) {
    this.schema    = schema;
    this.stats     = stats;
    this.indexName = indexName;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return schema;
  }

  public String getIndexName() {
    return indexName;
  }
}
