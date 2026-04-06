package com.dremio.plugins.kafka.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;

/**
 * Carries all parameters needed to execute one Kafka partition scan.
 *
 * One KafkaScanSpec is created per Kafka partition at planning time.
 * The offsets are frozen at plan time — later arriving messages are never
 * included in the same query (bounded snapshot semantics).
 *
 * Filter pushdown fields (all optional; -1 means "no filter"):
 *
 *   partitionFilter    — skip all partitions except this one (_partition = N)
 *   offsetStartFilter  — seek start: max(startOffset, offsetStartFilter)
 *   offsetEndFilter    — seek end:   min(endOffset,   offsetEndFilter)
 *   timestampStartMs   — resolved at execution via offsetsForTimes(); seek start
 *   timestampEndMs     — resolved at execution via offsetsForTimes(); seek end
 *
 * Timestamp filters are pre-adjusted by KafkaFilterRule for operator semantics:
 *   _timestamp >= X  → timestampStartMs = X
 *   _timestamp >  X  → timestampStartMs = X + 1
 *   _timestamp <  Y  → timestampEndMs   = Y
 *   _timestamp <= Y  → timestampEndMs   = Y + 1
 *
 * KafkaRecordReader calls KafkaConsumer.offsetsForTimes() at setup() time to
 * convert these millisecond timestamps into actual partition offsets, then seeks
 * directly — skipping the entire non-matching prefix/suffix with zero polling.
 *
 * Serialized as JSON (Jackson) when the SubScan is distributed to executor fragments.
 */
public class KafkaScanSpec {

  private final String topic;
  private final int    partition;
  private final long   startOffset;       // inclusive (base, from listPartitionChunks)
  private final long   endOffset;         // exclusive (= latestOffset at plan time)
  private final String schemaMode;        // "RAW" or "JSON"

  // Filter pushdown — set by KafkaFilterRule; -1 means "no filter"
  private final int  partitionFilter;    // _partition = N  pushdown
  private final long offsetStartFilter; // _offset >= X    pushdown
  private final long offsetEndFilter;   // _offset <  Y    pushdown
  private final long timestampStartMs;  // _timestamp >= X pushdown (epoch ms, pre-adjusted)
  private final long timestampEndMs;    // _timestamp <  Y pushdown (epoch ms, pre-adjusted)

  /** Full constructor — used by Jackson and KafkaFilterRule. */
  @JsonCreator
  public KafkaScanSpec(
      @JsonProperty("topic")             String topic,
      @JsonProperty("partition")         int    partition,
      @JsonProperty("startOffset")       long   startOffset,
      @JsonProperty("endOffset")         long   endOffset,
      @JsonProperty("schemaMode")        String schemaMode,
      @JsonProperty("partitionFilter")   int    partitionFilter,
      @JsonProperty("offsetStartFilter") long   offsetStartFilter,
      @JsonProperty("offsetEndFilter")   long   offsetEndFilter,
      @JsonProperty("timestampStartMs")  long   timestampStartMs,
      @JsonProperty("timestampEndMs")    long   timestampEndMs) {
    this.topic             = topic;
    this.partition         = partition;
    this.startOffset       = startOffset;
    this.endOffset         = endOffset;
    this.schemaMode        = (schemaMode != null) ? schemaMode : "JSON";
    this.partitionFilter   = partitionFilter;
    this.offsetStartFilter = offsetStartFilter;
    this.offsetEndFilter   = offsetEndFilter;
    this.timestampStartMs  = timestampStartMs;
    this.timestampEndMs    = timestampEndMs;
  }

  /** Convenience constructor without filter fields (no pushdown). */
  public KafkaScanSpec(String topic, int partition, long startOffset, long endOffset,
                       String schemaMode) {
    this(topic, partition, startOffset, endOffset, schemaMode, -1, -1L, -1L, -1L, -1L);
  }

  @JsonProperty("topic")
  public String getTopic() { return topic; }

  @JsonProperty("partition")
  public int getPartition() { return partition; }

  @JsonProperty("startOffset")
  public long getStartOffset() { return startOffset; }

  @JsonProperty("endOffset")
  public long getEndOffset() { return endOffset; }

  @JsonProperty("schemaMode")
  public String getSchemaMode() { return schemaMode; }

  @JsonProperty("partitionFilter")
  public int getPartitionFilter() { return partitionFilter; }

  @JsonProperty("offsetStartFilter")
  public long getOffsetStartFilter() { return offsetStartFilter; }

  @JsonProperty("offsetEndFilter")
  public long getOffsetEndFilter() { return offsetEndFilter; }

  @JsonProperty("timestampStartMs")
  public long getTimestampStartMs() { return timestampStartMs; }

  @JsonProperty("timestampEndMs")
  public long getTimestampEndMs() { return timestampEndMs; }

  /** True if any filter-pushdown field is set. */
  @JsonIgnore
  public boolean hasFilterPushdown() {
    return partitionFilter >= 0 || offsetStartFilter >= 0 || offsetEndFilter >= 0
        || timestampStartMs >= 0 || timestampEndMs >= 0;
  }

  /**
   * Effective start offset after applying the offsetStartFilter override.
   * Note: timestamp filters are resolved at execution time in KafkaRecordReader
   * and override this value further — they are NOT reflected here.
   */
  @JsonIgnore
  public long effectiveStartOffset() {
    if (offsetStartFilter >= 0) {
      return Math.max(startOffset, offsetStartFilter);
    }
    return startOffset;
  }

  /**
   * Effective end offset after applying the offsetEndFilter override.
   * Note: timestamp filters are resolved at execution time in KafkaRecordReader
   * and may narrow this value further — they are NOT reflected here.
   */
  @JsonIgnore
  public long effectiveEndOffset() {
    if (offsetEndFilter >= 0) {
      return Math.min(endOffset, offsetEndFilter);
    }
    return endOffset;
  }

  /**
   * True when this spec should be skipped entirely based on partition filter
   * or known-empty offset window. Timestamp skipping is done at execution time.
   */
  @JsonIgnore
  public boolean shouldSkip() {
    if (partitionFilter >= 0 && partition != partitionFilter) {
      return true;
    }
    return effectiveStartOffset() >= effectiveEndOffset();
  }

  /** True when there are no records in this partition window (before pushdown). */
  @JsonIgnore
  public boolean isEmpty() {
    return startOffset >= endOffset;
  }

  /** Returns the estimated record count for this split (used for cost estimates). */
  @JsonIgnore
  public long estimatedRecordCount() {
    return Math.max(0, effectiveEndOffset() - effectiveStartOffset());
  }

  /**
   * Returns a new spec with all filter pushdown fields set.
   * Used by KafkaFilterRule to annotate the scan spec in one pass.
   */
  public KafkaScanSpec withFilters(int newPartitionFilter,
                                    long newOffsetStartFilter,
                                    long newOffsetEndFilter,
                                    long newTimestampStartMs,
                                    long newTimestampEndMs) {
    return new KafkaScanSpec(
        topic, partition, startOffset, endOffset, schemaMode,
        newPartitionFilter, newOffsetStartFilter, newOffsetEndFilter,
        newTimestampStartMs, newTimestampEndMs);
  }

  /**
   * Encodes this spec as a compact pipe-delimited string for split storage.
   * Format: topic|partition|startOffset|endOffset|schemaMode|
   *         partitionFilter|offsetStartFilter|offsetEndFilter|
   *         timestampStartMs|timestampEndMs
   */
  public String toExtendedProperty() {
    return topic + "|" + partition + "|" + startOffset + "|" + endOffset + "|" + schemaMode
        + "|" + partitionFilter + "|" + offsetStartFilter + "|" + offsetEndFilter
        + "|" + timestampStartMs + "|" + timestampEndMs;
  }

  /**
   * Decodes a split extended property string back into a KafkaScanSpec.
   * Handles legacy 5-field, 8-field, and current 10-field formats.
   */
  public static KafkaScanSpec fromExtendedProperty(String encoded) {
    String[] parts = encoded.split("\\|", 10);
    if (parts.length >= 5) {
      try {
        int  partitionFilter   = parts.length >= 6  ? Integer.parseInt(parts[5]) : -1;
        long offsetStartFilter = parts.length >= 7  ? Long.parseLong(parts[6])   : -1L;
        long offsetEndFilter   = parts.length >= 8  ? Long.parseLong(parts[7])   : -1L;
        long timestampStartMs  = parts.length >= 9  ? Long.parseLong(parts[8])   : -1L;
        long timestampEndMs    = parts.length >= 10 ? Long.parseLong(parts[9])   : -1L;
        return new KafkaScanSpec(
            parts[0],
            Integer.parseInt(parts[1]),
            Long.parseLong(parts[2]),
            Long.parseLong(parts[3]),
            parts[4],
            partitionFilter,
            offsetStartFilter,
            offsetEndFilter,
            timestampStartMs,
            timestampEndMs);
      } catch (NumberFormatException ignore) { }
    }
    return null;
  }

  /** Returns the table path as a List for SubScanWithProjection. */
  public List<String> toTablePath() {
    return Arrays.asList(topic);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(topic).append("[p=").append(partition)
      .append(", ").append(startOffset).append("..").append(endOffset)
      .append(", ").append(schemaMode);
    if (hasFilterPushdown()) {
      sb.append(", pushdown:");
      if (partitionFilter >= 0)  sb.append(" _partition=").append(partitionFilter);
      if (offsetStartFilter >= 0) sb.append(" _offset>=").append(offsetStartFilter);
      if (offsetEndFilter >= 0)   sb.append(" _offset<").append(offsetEndFilter);
      if (timestampStartMs >= 0)  sb.append(" _ts>=").append(timestampStartMs);
      if (timestampEndMs >= 0)    sb.append(" _ts<").append(timestampEndMs);
    }
    sb.append("]");
    return sb.toString();
  }
}
