package com.dremio.plugins.pubsub;

import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PubSubScanSpecTest {

  @Test
  void roundTripExtendedProperty() {
    PubSubScanSpec spec = new PubSubScanSpec("orders-dremio", 500, "JSON");
    String encoded = spec.toExtendedProperty();
    PubSubScanSpec decoded = PubSubScanSpec.fromExtendedProperty(encoded);
    assertNotNull(decoded);
    assertEquals("orders-dremio", decoded.getSubscription());
    assertEquals(500, decoded.getMaxMessages());
    assertEquals("JSON", decoded.getSchemaMode());
  }

  @Test
  void roundTripRawMode() {
    PubSubScanSpec spec = new PubSubScanSpec("events-sub", 1000, "RAW");
    PubSubScanSpec decoded = PubSubScanSpec.fromExtendedProperty(spec.toExtendedProperty());
    assertNotNull(decoded);
    assertEquals("RAW", decoded.getSchemaMode());
  }

  @Test
  void fromExtendedPropertyNullInput() {
    assertNull(PubSubScanSpec.fromExtendedProperty(null));
    assertNull(PubSubScanSpec.fromExtendedProperty(""));
  }

  @Test
  void defaultsSchemaMode() {
    PubSubScanSpec spec = new PubSubScanSpec("my-sub", 100, null);
    assertEquals("JSON", spec.getSchemaMode());
  }

  @Test
  void toTablePath() {
    PubSubScanSpec spec = new PubSubScanSpec("my-sub", 100, "JSON");
    assertEquals(java.util.Arrays.asList("my-sub"), spec.toTablePath());
  }

  @Test
  void toStringContainsFields() {
    PubSubScanSpec spec = new PubSubScanSpec("orders", 250, "JSON");
    String s = spec.toString();
    assertTrue(s.contains("orders"));
    assertTrue(s.contains("250"));
    assertTrue(s.contains("JSON"));
  }

  @Test
  void encodedStringContainsAllParts() {
    String encoded = new PubSubScanSpec("my-sub", 999, "RAW").toExtendedProperty();
    assertEquals("my-sub|999|RAW", encoded);
  }
}
