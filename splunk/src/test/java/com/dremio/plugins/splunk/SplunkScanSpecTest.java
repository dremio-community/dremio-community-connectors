package com.dremio.plugins.splunk;

import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SplunkScanSpec SPL generation and serialization.
 */
public class SplunkScanSpecTest {

  @Test
  public void testBasicSpl() {
    SplunkScanSpec spec = new SplunkScanSpec(
        "main", "-24h", "now", -1L, -1L, "", 50000, Collections.emptyList());
    String spl = spec.toSpl();
    assertTrue(spl.startsWith("search index=main"), "SPL must start with 'search index=main'");
    assertTrue(spl.contains("| head 50000"), "SPL must contain head limit");
    assertFalse(spl.contains("| fields"), "No fields clause when fieldList is empty");
  }

  @Test
  public void testSplWithFilter() {
    SplunkScanSpec spec = new SplunkScanSpec(
        "web", "-1h", "now", -1L, -1L, "status=\"404\"", 1000, Collections.emptyList());
    String spl = spec.toSpl();
    assertTrue(spl.contains("status=\"404\""), "Filter must appear in SPL");
    assertTrue(spl.contains("| head 1000"), "Head limit must appear in SPL");
  }

  @Test
  public void testSplWithFieldList() {
    SplunkScanSpec spec = new SplunkScanSpec(
        "main", "-1h", "now", -1L, -1L, "", 500,
        Arrays.asList("_time", "host", "status"));
    String spl = spec.toSpl();
    assertTrue(spl.contains("| fields _time host status"), "Fields clause must appear");
  }

  @Test
  public void testEffectiveEarliestDefault() {
    SplunkScanSpec spec = new SplunkScanSpec(
        "main", "-24h", "now", -1L, -1L, "", 50000, Collections.emptyList());
    assertEquals("-24h", spec.effectiveEarliest());
    assertEquals("now",  spec.effectiveLatest());
  }

  @Test
  public void testEffectiveEarliestFromEpochMs() {
    // 2024-01-15T00:00:00 UTC = 1705276800000
    SplunkScanSpec spec = new SplunkScanSpec(
        "main", "-24h", "now", 1705276800000L, 1705363200000L, "", 50000,
        Collections.emptyList());
    String earliest = spec.effectiveEarliest();
    String latest   = spec.effectiveLatest();
    assertTrue(earliest.startsWith("2024-01-15"), "Earliest must be formatted as ISO date: " + earliest);
    assertTrue(latest.startsWith("2024-01-16"),   "Latest must be formatted as ISO date: " + latest);
  }

  @Test
  public void testExtendedPropertyRoundtrip() {
    SplunkScanSpec original = new SplunkScanSpec(
        "security", "-6h", "now", 1705276800000L, -1L, "sourcetype=\"syslog\"", 10000,
        Collections.emptyList());
    String encoded = original.toExtendedProperty();
    SplunkScanSpec decoded = SplunkScanSpec.fromExtendedProperty(encoded);

    assertNotNull(decoded);
    assertEquals("security",            decoded.getIndexName());
    assertEquals("-6h",                  decoded.getEarliest());
    assertEquals("now",                  decoded.getLatest());
    assertEquals(1705276800000L,         decoded.getEarliestEpochMs());
    assertEquals(-1L,                    decoded.getLatestEpochMs());
    assertEquals("sourcetype=\"syslog\"", decoded.getSplFilter());
    assertEquals(10000,                  decoded.getMaxEvents());
  }

  @Test
  public void testFromExtendedPropertyReturnsNullOnInvalidInput() {
    assertNull(SplunkScanSpec.fromExtendedProperty(null));
    assertNull(SplunkScanSpec.fromExtendedProperty(""));
    assertNull(SplunkScanSpec.fromExtendedProperty("only|two"));
  }

  @Test
  public void testHasFilterPushdown() {
    SplunkScanSpec noFilter = new SplunkScanSpec(
        "main", "-24h", "now", -1L, -1L, "", 50000, Collections.emptyList());
    assertFalse(noFilter.hasFilterPushdown());

    SplunkScanSpec withTime = noFilter.withTimeFilter(1705276800000L, -1L);
    assertTrue(withTime.hasFilterPushdown());

    SplunkScanSpec withFilter = noFilter.withSplFilter("status=\"200\"");
    assertTrue(withFilter.hasFilterPushdown());
  }

  @Test
  public void testWithMaxEvents() {
    SplunkScanSpec original = new SplunkScanSpec(
        "main", "-24h", "now", -1L, -1L, "", 50000, Collections.emptyList());
    SplunkScanSpec limited = original.withMaxEvents(100);
    assertEquals(100, limited.getMaxEvents());
    assertTrue(limited.toSpl().contains("| head 100"));
  }
}
