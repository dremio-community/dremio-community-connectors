package com.dremio.plugins.splunk;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Infers an Arrow schema for a Splunk index by sampling recent events.
 *
 * Splunk is schemaless — each event can have different fields. This class
 * samples up to {@code sampleCount} recent events from an index, walks all
 * field values, and promotes types conservatively:
 *   BOOLEAN < BIGINT < DOUBLE < VARCHAR
 * (more specific types are widened to less specific when values conflict).
 *
 * Always-present metadata columns are prepended to the inferred schema:
 *   _time       TIMESTAMP(ms, UTC)
 *   _raw        VARCHAR
 *   _index      VARCHAR
 *   _sourcetype VARCHAR
 *   _source     VARCHAR
 *   _host       VARCHAR
 *
 * Splunk internal fields (starting with "_") are excluded from the inferred
 * payload section to avoid polluting the schema with Splunk bookkeeping fields.
 */
public class SplunkSchemaInferrer {

  private static final Logger logger = LoggerFactory.getLogger(SplunkSchemaInferrer.class);

  // Metadata columns always present in every Splunk event
  public static final String COL_TIME       = "_time";
  public static final String COL_RAW        = "_raw";
  public static final String COL_INDEX      = "_index";
  public static final String COL_SOURCETYPE = "_sourcetype";
  public static final String COL_SOURCE     = "_source";
  public static final String COL_HOST       = "_host";

  private static final List<Field> METADATA_FIELDS;

  static {
    METADATA_FIELDS = new ArrayList<>();
    // _time as TIMESTAMP(MILLISECOND, UTC) — parsed from ISO-8601 string
    METADATA_FIELDS.add(new Field(COL_TIME,
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null));
    METADATA_FIELDS.add(new Field(COL_RAW,        FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    METADATA_FIELDS.add(new Field(COL_INDEX,      FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    METADATA_FIELDS.add(new Field(COL_SOURCETYPE, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    METADATA_FIELDS.add(new Field(COL_SOURCE,     FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    METADATA_FIELDS.add(new Field(COL_HOST,       FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
  }

  public static List<Field> getMetadataFields() {
    return METADATA_FIELDS;
  }

  private final SplunkClient client;
  private final int sampleCount;

  // Type rank: higher = less specific. VARCHAR beats everything.
  private static final int RANK_BOOLEAN = 0;
  private static final int RANK_BIGINT  = 1;
  private static final int RANK_DOUBLE  = 2;
  private static final int RANK_VARCHAR = 3;

  public SplunkSchemaInferrer(SplunkClient client, int sampleCount) {
    this.client      = client;
    this.sampleCount = sampleCount;
  }

  /**
   * Infers the schema for the given index.
   * Returns metadata fields + inferred payload fields.
   */
  public List<Field> inferFields(String indexName) throws Exception {
    // Sample recent events (last 15 minutes is fast; fall back to -1h if empty)
    String spl = "search index=" + indexName + " | head " + sampleCount;
    List<JsonNode> events = client.runSearch(spl, "-15m", "now", sampleCount);
    if (events.isEmpty()) {
      logger.debug("No events in last 15m for index '{}', trying -1h", indexName);
      events = client.runSearch(spl, "-1h", "now", sampleCount);
    }
    if (events.isEmpty()) {
      logger.debug("No events found for index '{}', using metadata-only schema", indexName);
      return new ArrayList<>(METADATA_FIELDS);
    }

    // Walk all events, accumulate field → type rank
    // Use LinkedHashMap to preserve field discovery order
    Map<String, Integer> fieldRanks = new LinkedHashMap<>();

    for (JsonNode event : events) {
      event.fieldNames().forEachRemaining(fieldName -> {
        // Skip Splunk internal fields (they're covered by METADATA_FIELDS)
        if (fieldName.startsWith("_")) return;
        // Skip punct (Splunk internal punctuation pattern field)
        if ("punct".equals(fieldName)) return;

        String value = event.path(fieldName).asText(null);
        int rank = rankValue(value);

        fieldRanks.merge(fieldName, rank, Math::max);
      });
    }

    // Build the final field list
    List<Field> fields = new ArrayList<>(METADATA_FIELDS);
    for (Map.Entry<String, Integer> entry : fieldRanks.entrySet()) {
      String  name = entry.getKey();
      int     rank = entry.getValue();
      // Skip if it duplicates a metadata column name
      if (isMetadataField(name)) continue;
      fields.add(rankToField(name, rank));
    }

    logger.debug("Inferred {} fields ({} payload) for Splunk index '{}'",
        fields.size(), fields.size() - METADATA_FIELDS.size(), indexName);
    return fields;
  }

  // -----------------------------------------------------------------------
  // Type inference
  // -----------------------------------------------------------------------

  /**
   * Returns the type rank for a given string value.
   * Rank 0 = BOOLEAN (most specific), rank 3 = VARCHAR (least specific).
   */
  static int rankValue(String value) {
    if (value == null || value.isEmpty()) {
      // Null/empty — don't widen, keep existing type (return lowest rank)
      return RANK_BOOLEAN;
    }

    // Boolean?
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return RANK_BOOLEAN;
    }

    // Integer (BIGINT)?
    try {
      Long.parseLong(value);
      return RANK_BIGINT;
    } catch (NumberFormatException ignored) {}

    // Floating point (DOUBLE)?
    try {
      Double.parseDouble(value);
      return RANK_DOUBLE;
    } catch (NumberFormatException ignored) {}

    // Everything else → VARCHAR
    return RANK_VARCHAR;
  }

  private static Field rankToField(String name, int rank) {
    ArrowType type;
    switch (rank) {
      case RANK_BOOLEAN:
        type = ArrowType.Bool.INSTANCE;
        break;
      case RANK_BIGINT:
        type = new ArrowType.Int(64, true);
        break;
      case RANK_DOUBLE:
        type = new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        break;
      default:
        type = ArrowType.Utf8.INSTANCE;
        break;
    }
    return new Field(name, FieldType.nullable(type), null);
  }

  private static boolean isMetadataField(String name) {
    return COL_TIME.equals(name)
        || COL_RAW.equals(name)
        || COL_INDEX.equals(name)
        || COL_SOURCETYPE.equals(name)
        || COL_SOURCE.equals(name)
        || COL_HOST.equals(name);
  }

  /**
   * Parses a Splunk _time string (ISO-8601 with offset) to epoch milliseconds.
   * Returns -1 if the string cannot be parsed.
   */
  public static long parseTime(String timeStr) {
    if (timeStr == null || timeStr.isEmpty()) return -1L;
    try {
      return OffsetDateTime.parse(timeStr).toInstant().toEpochMilli();
    } catch (DateTimeParseException e) {
      // Some Splunk versions use Unix epoch float (e.g. "1705315200.000")
      try {
        double epochSeconds = Double.parseDouble(timeStr);
        return (long) (epochSeconds * 1000L);
      } catch (NumberFormatException ignored) {}
      return -1L;
    }
  }
}
