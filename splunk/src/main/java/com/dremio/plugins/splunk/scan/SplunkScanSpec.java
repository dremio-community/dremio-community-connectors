package com.dremio.plugins.splunk.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Carries all parameters needed to execute one Splunk index scan.
 *
 * Created by SplunkScanRule at planning time with defaults, then optionally
 * enriched by SplunkFilterRule with pushed-down time bounds and field predicates.
 *
 * The SPL string is assembled lazily by {@link #toSpl(String)} which is called
 * by SplunkRecordReader during execution.
 *
 * Serialized as JSON (Jackson) when the SubScan is distributed to executor fragments.
 */
public class SplunkScanSpec {

  private static final DateTimeFormatter ISO_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

  private final String       indexName;

  // Time bounds — Splunk relative modifiers OR ISO-8601 strings
  private final String       earliest;          // default from config (e.g. "-24h")
  private final String       latest;            // default "now"

  // Pushed-down epoch-ms time bounds (from SplunkFilterRule); -1 = not set
  private final long         earliestEpochMs;
  private final long         latestEpochMs;

  // Additional SPL filter clause (field=value pairs from WHERE pushdown)
  private final String       splFilter;

  // LIMIT pushdown
  private final int          maxEvents;

  // SELECT projection pushdown (empty = all fields)
  private final List<String> fieldList;

  @JsonCreator
  public SplunkScanSpec(
      @JsonProperty("indexName")       String       indexName,
      @JsonProperty("earliest")        String       earliest,
      @JsonProperty("latest")          String       latest,
      @JsonProperty("earliestEpochMs") long         earliestEpochMs,
      @JsonProperty("latestEpochMs")   long         latestEpochMs,
      @JsonProperty("splFilter")       String       splFilter,
      @JsonProperty("maxEvents")       int          maxEvents,
      @JsonProperty("fieldList")       List<String> fieldList) {
    this.indexName      = indexName;
    this.earliest       = earliest;
    this.latest         = latest;
    this.earliestEpochMs = earliestEpochMs;
    this.latestEpochMs   = latestEpochMs;
    this.splFilter      = (splFilter != null) ? splFilter : "";
    this.maxEvents      = maxEvents;
    this.fieldList      = (fieldList != null) ? fieldList : Collections.emptyList();
  }

  // -----------------------------------------------------------------------
  // Getters (Jackson needs these)
  // -----------------------------------------------------------------------

  @JsonProperty("indexName")
  public String getIndexName() { return indexName; }

  @JsonProperty("earliest")
  public String getEarliest() { return earliest; }

  @JsonProperty("latest")
  public String getLatest() { return latest; }

  @JsonProperty("earliestEpochMs")
  public long getEarliestEpochMs() { return earliestEpochMs; }

  @JsonProperty("latestEpochMs")
  public long getLatestEpochMs() { return latestEpochMs; }

  @JsonProperty("splFilter")
  public String getSplFilter() { return splFilter; }

  @JsonProperty("maxEvents")
  public int getMaxEvents() { return maxEvents; }

  @JsonProperty("fieldList")
  public List<String> getFieldList() { return fieldList; }

  // -----------------------------------------------------------------------
  // Derived properties
  // -----------------------------------------------------------------------

  /**
   * True if any filter pushdown fields are set. Used by SplunkFilterRule to
   * avoid re-firing on already-processed scans.
   */
  @JsonIgnore
  public boolean hasFilterPushdown() {
    return earliestEpochMs >= 0 || latestEpochMs >= 0 || !splFilter.isEmpty();
  }

  /**
   * Returns the effective earliest time string for the Splunk job.
   * Epoch-ms value takes priority over the default relative modifier.
   */
  @JsonIgnore
  public String effectiveEarliest() {
    if (earliestEpochMs >= 0) {
      return ISO_FMT.format(Instant.ofEpochMilli(earliestEpochMs));
    }
    return (earliest != null && !earliest.isBlank()) ? earliest : "-24h";
  }

  /**
   * Returns the effective latest time string for the Splunk job.
   */
  @JsonIgnore
  public String effectiveLatest() {
    if (latestEpochMs >= 0) {
      return ISO_FMT.format(Instant.ofEpochMilli(latestEpochMs));
    }
    return (latest != null && !latest.isBlank()) ? latest : "now";
  }

  /**
   * Builds the full SPL string to submit to Splunk.
   *
   * Format:
   *   search index=<name> <splFilter> | head <maxEvents> [| fields <f1> <f2> ...]
   *
   * Time bounds are passed as separate parameters to createSearchJob(), not
   * embedded in the SPL string itself — this is the recommended Splunk pattern.
   */
  public String toSpl() {
    StringBuilder sb = new StringBuilder("search index=");
    sb.append(indexName);

    if (!splFilter.isEmpty()) {
      sb.append(" ").append(splFilter);
    }

    sb.append(" | head ").append(maxEvents);

    if (!fieldList.isEmpty()) {
      sb.append(" | fields ");
      sb.append(fieldList.stream().collect(Collectors.joining(" ")));
    }

    return sb.toString();
  }

  /**
   * Returns a table path list for SubScan construction.
   */
  @JsonIgnore
  public List<String> toTablePath() {
    List<String> path = new ArrayList<>();
    path.add(indexName);
    return path;
  }

  // -----------------------------------------------------------------------
  // Filter pushdown — returns new spec with updated fields
  // -----------------------------------------------------------------------

  public SplunkScanSpec withTimeFilter(long newEarliestEpochMs, long newLatestEpochMs) {
    return new SplunkScanSpec(indexName, earliest, latest,
        newEarliestEpochMs, newLatestEpochMs, splFilter, maxEvents, fieldList);
  }

  public SplunkScanSpec withSplFilter(String newSplFilter) {
    return new SplunkScanSpec(indexName, earliest, latest,
        earliestEpochMs, latestEpochMs, newSplFilter, maxEvents, fieldList);
  }

  public SplunkScanSpec withMaxEvents(int newMaxEvents) {
    return new SplunkScanSpec(indexName, earliest, latest,
        earliestEpochMs, latestEpochMs, splFilter, newMaxEvents, fieldList);
  }

  public SplunkScanSpec withFieldList(List<String> newFieldList) {
    return new SplunkScanSpec(indexName, earliest, latest,
        earliestEpochMs, latestEpochMs, splFilter, maxEvents, newFieldList);
  }

  public SplunkScanSpec withAllFilters(long newEarliestEpochMs, long newLatestEpochMs,
                                        String newSplFilter, int newMaxEvents) {
    return new SplunkScanSpec(indexName, earliest, latest,
        newEarliestEpochMs, newLatestEpochMs, newSplFilter, newMaxEvents, fieldList);
  }

  // -----------------------------------------------------------------------
  // Serialization for DatasetSplit extended property
  // -----------------------------------------------------------------------

  /**
   * Encodes this spec as a pipe-delimited string for split storage.
   * Format: indexName|earliest|latest|earliestEpochMs|latestEpochMs|splFilter|maxEvents
   */
  public String toExtendedProperty() {
    return indexName
        + "|" + nullToEmpty(earliest)
        + "|" + nullToEmpty(latest)
        + "|" + earliestEpochMs
        + "|" + latestEpochMs
        + "|" + nullToEmpty(splFilter)
        + "|" + maxEvents;
  }

  /** Decodes a split extended property string back into a SplunkScanSpec. */
  public static SplunkScanSpec fromExtendedProperty(String encoded) {
    if (encoded == null || encoded.isEmpty()) return null;
    try {
      String[] parts = encoded.split("\\|", -1);
      if (parts.length < 7) return null;
      return new SplunkScanSpec(
          parts[0],                        // indexName
          parts[1],                        // earliest
          parts[2],                        // latest
          Long.parseLong(parts[3]),        // earliestEpochMs
          Long.parseLong(parts[4]),        // latestEpochMs
          parts[5],                        // splFilter
          Integer.parseInt(parts[6]),      // maxEvents
          Collections.emptyList()          // fieldList (projection applied by Dremio)
      );
    } catch (Exception e) {
      return null;
    }
  }

  private static String nullToEmpty(String s) {
    return (s != null) ? s : "";
  }

  @Override
  public String toString() {
    return "SplunkScanSpec{index='" + indexName + "', spl='" + toSpl()
        + "', earliest='" + effectiveEarliest() + "', latest='" + effectiveLatest() + "'}";
  }
}
