package com.dremio.plugins.splunk;

import org.junit.jupiter.api.Test;

import static com.dremio.plugins.splunk.SplunkSchemaInferrer.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SplunkSchemaInferrer type inference logic.
 * No network calls — tests the pure value-ranking logic only.
 */
public class SplunkSchemaInferrerTest {

  @Test
  public void testNullIsLowestRank() {
    assertEquals(0, rankValue(null));
    assertEquals(0, rankValue(""));
  }

  @Test
  public void testBooleanRank() {
    assertEquals(0, rankValue("true"));
    assertEquals(0, rankValue("false"));
    assertEquals(0, rankValue("TRUE"));
    assertEquals(0, rankValue("False"));
  }

  @Test
  public void testBigIntRank() {
    assertEquals(1, rankValue("42"));
    assertEquals(1, rankValue("-100"));
    assertEquals(1, rankValue("0"));
    assertEquals(1, rankValue("9999999999"));
  }

  @Test
  public void testDoubleRank() {
    assertEquals(2, rankValue("3.14"));
    assertEquals(2, rankValue("-0.5"));
    assertEquals(2, rankValue("1.0e10"));
    assertEquals(2, rankValue("NaN")); // Double.parseDouble accepts NaN
  }

  @Test
  public void testVarCharRank() {
    assertEquals(3, rankValue("hello"));
    assertEquals(3, rankValue("2024-01-15")); // dates are VARCHAR (no date coercion in inferrer)
    assertEquals(3, rankValue("192.168.1.1"));
    assertEquals(3, rankValue("GET /index.html HTTP/1.1"));
  }

  @Test
  public void testParseTimeIso8601() {
    // Splunk ISO-8601 with offset
    long ts = parseTime("2024-01-15T10:23:45.000+00:00");
    assertEquals(1705314225000L, ts);
  }

  @Test
  public void testParseTimeEpochFloat() {
    // Splunk sometimes returns epoch as float string
    long ts = parseTime("1705314225.000");
    assertEquals(1705314225000L, ts);
  }

  @Test
  public void testParseTimeNull() {
    assertEquals(-1L, parseTime(null));
    assertEquals(-1L, parseTime(""));
    assertEquals(-1L, parseTime("not-a-time"));
  }

  @Test
  public void testMetadataFields() {
    // Metadata fields should always be present
    var fields = getMetadataFields();
    assertEquals(6, fields.size());
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_TIME)));
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_RAW)));
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_HOST)));
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_INDEX)));
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_SOURCE)));
    assertTrue(fields.stream().anyMatch(f -> f.getName().equals(COL_SOURCETYPE)));
  }
}
