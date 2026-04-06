package com.dremio.plugins.kafka.scan;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaScanSpec logic: filter pushdown, offset resolution,
 * serialization round-trips, and backward-compatible deserialization.
 */
class KafkaScanSpecTest {

  // ---------------------------------------------------------------------------
  // shouldSkip() — partition filter
  // ---------------------------------------------------------------------------

  @Test
  void shouldSkip_noFilter_doesNotSkip() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON");
    assertFalse(spec.shouldSkip());
  }

  @Test
  void shouldSkip_partitionFilterMatches_doesNotSkip() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 2, 0L, 100L, "JSON",
        2, -1L, -1L, -1L, -1L);
    assertFalse(spec.shouldSkip());
  }

  @Test
  void shouldSkip_partitionFilterMismatches_skips() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 1, 0L, 100L, "JSON",
        0, -1L, -1L, -1L, -1L);
    assertTrue(spec.shouldSkip());
  }

  @Test
  void shouldSkip_emptyOffsetWindow_skips() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 50L, 50L, "JSON");
    assertTrue(spec.shouldSkip());
  }

  @Test
  void shouldSkip_offsetFilterCreatesEmptyWindow_skips() {
    // startOffset=50, offsetStartFilter=200 → effectiveStart=200 >= effectiveEnd=100
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 50L, 100L, "JSON",
        -1, 200L, -1L, -1L, -1L);
    assertTrue(spec.shouldSkip());
  }

  // ---------------------------------------------------------------------------
  // effectiveStartOffset() / effectiveEndOffset()
  // ---------------------------------------------------------------------------

  @Test
  void effectiveStart_noFilter_returnsBaseStart() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 10L, 200L, "JSON");
    assertEquals(10L, spec.effectiveStartOffset());
  }

  @Test
  void effectiveStart_filterHigherThanBase_returnsFilter() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 10L, 200L, "JSON",
        -1, 50L, -1L, -1L, -1L);
    assertEquals(50L, spec.effectiveStartOffset());
  }

  @Test
  void effectiveStart_filterLowerThanBase_returnsBase() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 30L, 200L, "JSON",
        -1, 5L, -1L, -1L, -1L);
    assertEquals(30L, spec.effectiveStartOffset());
  }

  @Test
  void effectiveEnd_noFilter_returnsBaseEnd() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON");
    assertEquals(100L, spec.effectiveEndOffset());
  }

  @Test
  void effectiveEnd_filterLowerThanBase_returnsFilter() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON",
        -1, -1L, 60L, -1L, -1L);
    assertEquals(60L, spec.effectiveEndOffset());
  }

  @Test
  void effectiveEnd_filterHigherThanBase_returnsBase() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON",
        -1, -1L, 150L, -1L, -1L);
    assertEquals(100L, spec.effectiveEndOffset());
  }

  // ---------------------------------------------------------------------------
  // estimatedRecordCount()
  // ---------------------------------------------------------------------------

  @Test
  void estimatedRecordCount_noFilter() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 1000L, "JSON");
    assertEquals(1000L, spec.estimatedRecordCount());
  }

  @Test
  void estimatedRecordCount_withOffsetFilters() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 1000L, "JSON",
        -1, 100L, 500L, -1L, -1L);
    assertEquals(400L, spec.estimatedRecordCount());
  }

  // ---------------------------------------------------------------------------
  // hasFilterPushdown()
  // ---------------------------------------------------------------------------

  @Test
  void hasFilterPushdown_noPushdown_false() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON");
    assertFalse(spec.hasFilterPushdown());
  }

  @Test
  void hasFilterPushdown_partitionSet_true() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON",
        1, -1L, -1L, -1L, -1L);
    assertTrue(spec.hasFilterPushdown());
  }

  @Test
  void hasFilterPushdown_timestampSet_true() {
    KafkaScanSpec spec = new KafkaScanSpec("topic", 0, 0L, 100L, "JSON",
        -1, -1L, -1L, 1700000000000L, -1L);
    assertTrue(spec.hasFilterPushdown());
  }

  // ---------------------------------------------------------------------------
  // withFilters()
  // ---------------------------------------------------------------------------

  @Test
  void withFilters_copiesBaseFields() {
    KafkaScanSpec base = new KafkaScanSpec("my-topic", 3, 10L, 500L, "AVRO");
    KafkaScanSpec filtered = base.withFilters(3, 50L, 400L, -1L, -1L);

    assertEquals("my-topic", filtered.getTopic());
    assertEquals(3, filtered.getPartition());
    assertEquals(10L, filtered.getStartOffset());
    assertEquals(500L, filtered.getEndOffset());
    assertEquals("AVRO", filtered.getSchemaMode());
    assertEquals(3, filtered.getPartitionFilter());
    assertEquals(50L, filtered.getOffsetStartFilter());
    assertEquals(400L, filtered.getOffsetEndFilter());
  }

  // ---------------------------------------------------------------------------
  // toExtendedProperty() / fromExtendedProperty() round-trip
  // ---------------------------------------------------------------------------

  @Test
  void serialization_roundTrip_allFields() {
    KafkaScanSpec original = new KafkaScanSpec("orders", 2, 100L, 999L, "JSON",
        2, 150L, 900L, 1700000000000L, 1700001000000L);

    String encoded = original.toExtendedProperty();
    KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty(encoded);

    assertNotNull(decoded);
    assertEquals("orders", decoded.getTopic());
    assertEquals(2, decoded.getPartition());
    assertEquals(100L, decoded.getStartOffset());
    assertEquals(999L, decoded.getEndOffset());
    assertEquals("JSON", decoded.getSchemaMode());
    assertEquals(2, decoded.getPartitionFilter());
    assertEquals(150L, decoded.getOffsetStartFilter());
    assertEquals(900L, decoded.getOffsetEndFilter());
    assertEquals(1700000000000L, decoded.getTimestampStartMs());
    assertEquals(1700001000000L, decoded.getTimestampEndMs());
  }

  @Test
  void serialization_roundTrip_noFilters() {
    KafkaScanSpec original = new KafkaScanSpec("events", 0, 0L, 50L, "RAW");
    KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty(original.toExtendedProperty());

    assertNotNull(decoded);
    assertEquals("events", decoded.getTopic());
    assertEquals(0, decoded.getPartition());
    assertEquals(0L, decoded.getStartOffset());
    assertEquals(50L, decoded.getEndOffset());
    assertEquals("RAW", decoded.getSchemaMode());
    assertFalse(decoded.hasFilterPushdown());
  }

  @Test
  void serialization_backwardCompat_5Fields() {
    // Legacy 5-field format: topic|partition|start|end|schemaMode
    KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty("legacy-topic|1|0|200|JSON");

    assertNotNull(decoded);
    assertEquals("legacy-topic", decoded.getTopic());
    assertEquals(1, decoded.getPartition());
    assertEquals(0L, decoded.getStartOffset());
    assertEquals(200L, decoded.getEndOffset());
    assertEquals("JSON", decoded.getSchemaMode());
    assertFalse(decoded.hasFilterPushdown());
    assertEquals(-1, decoded.getPartitionFilter());
    assertEquals(-1L, decoded.getOffsetStartFilter());
    assertEquals(-1L, decoded.getTimestampStartMs());
  }

  @Test
  void serialization_backwardCompat_8Fields() {
    // 8-field format (pre-timestamp): topic|p|start|end|mode|pFilter|offStart|offEnd
    KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty("topic|0|10|100|JSON|2|20|80");

    assertNotNull(decoded);
    assertEquals(2, decoded.getPartitionFilter());
    assertEquals(20L, decoded.getOffsetStartFilter());
    assertEquals(80L, decoded.getOffsetEndFilter());
    assertEquals(-1L, decoded.getTimestampStartMs());
    assertEquals(-1L, decoded.getTimestampEndMs());
  }

  @Test
  void serialization_topicWithSpecialChars() {
    // Topic names can contain dots and underscores
    KafkaScanSpec original = new KafkaScanSpec("my.topic_v2", 0, 0L, 100L, "JSON");
    KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty(original.toExtendedProperty());

    assertNotNull(decoded);
    assertEquals("my.topic_v2", decoded.getTopic());
  }

  @Test
  void serialization_invalidEncoding_returnsNull() {
    assertNull(KafkaScanSpec.fromExtendedProperty("bad|data"));
    assertNull(KafkaScanSpec.fromExtendedProperty(""));
    assertNull(KafkaScanSpec.fromExtendedProperty("a|b|c|not-a-number|JSON"));
  }

  // ---------------------------------------------------------------------------
  // toString()
  // ---------------------------------------------------------------------------

  @Test
  void toString_withPushdown_containsFilterInfo() {
    KafkaScanSpec spec = new KafkaScanSpec("orders", 0, 0L, 100L, "JSON",
        0, 10L, 80L, -1L, -1L);
    String s = spec.toString();
    assertTrue(s.contains("orders"));
    assertTrue(s.contains("pushdown"));
    assertTrue(s.contains("_partition=0"));
    assertTrue(s.contains("_offset>=10"));
  }

  @Test
  void toString_noPushdown_noPushdownSection() {
    KafkaScanSpec spec = new KafkaScanSpec("orders", 1, 0L, 100L, "JSON");
    assertFalse(spec.toString().contains("pushdown"));
  }
}
