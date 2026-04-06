package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the static cost-estimation helpers in {@link CassandraStoragePlugin}:
 *
 * <dl>
 *   <dt>{@code estimateColumnBytes(DataType)}</dt>
 *   <dd>Verifies each type bucket maps to the documented byte estimate.</dd>
 *
 *   <dt>{@code estimateRowSizeBytes(TableMetadata)}</dt>
 *   <dd>Mocks a TableMetadata with known column types and verifies the combined
 *       estimate (sum of column sizes + overhead) and the 32-byte floor.</dd>
 * </dl>
 *
 * Both methods are package-private statics; this test is in the same package.
 */
class CassandraStoragePluginTest {

  // ============================================================================
  // estimateColumnBytes() — type bucket tests
  // ============================================================================

  // ---- 1-byte types ----------------------------------------------------------

  @Test
  void columnBytes_boolean_returns1() {
    assertEquals(1L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.BOOLEAN));
  }

  @Test
  void columnBytes_tinyint_returns1() {
    assertEquals(1L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.TINYINT));
  }

  // ---- 2-byte types ----------------------------------------------------------

  @Test
  void columnBytes_smallint_returns2() {
    assertEquals(2L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.SMALLINT));
  }

  // ---- 4-byte types ----------------------------------------------------------

  @Test
  void columnBytes_int_returns4() {
    assertEquals(4L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.INT));
  }

  @Test
  void columnBytes_float_returns4() {
    assertEquals(4L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.FLOAT));
  }

  @Test
  void columnBytes_date_returns4() {
    assertEquals(4L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.DATE));
  }

  // ---- 8-byte types ----------------------------------------------------------

  @Test
  void columnBytes_bigint_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.BIGINT));
  }

  @Test
  void columnBytes_double_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.DOUBLE));
  }

  @Test
  void columnBytes_timestamp_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.TIMESTAMP));
  }

  @Test
  void columnBytes_time_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.TIME));
  }

  @Test
  void columnBytes_counter_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.COUNTER));
  }

  @Test
  void columnBytes_varint_returns8() {
    assertEquals(8L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.VARINT));
  }

  @Test
  void columnBytes_inet_returns4() {
    // IPv4 common case
    assertEquals(4L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.INET));
  }

  // ---- 16-byte types ---------------------------------------------------------

  @Test
  void columnBytes_uuid_returns16() {
    assertEquals(16L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.UUID));
  }

  @Test
  void columnBytes_timeuuid_returns16() {
    assertEquals(16L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.TIMEUUID));
  }

  @Test
  void columnBytes_decimal_returns16() {
    assertEquals(16L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.DECIMAL));
  }

  // ---- Variable-length / collection types ------------------------------------

  @Test
  void columnBytes_text_returns20() {
    assertEquals(20L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.TEXT));
  }

  @Test
  void columnBytes_ascii_returns20() {
    assertEquals(20L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.ASCII));
  }

  @Test
  void columnBytes_blob_returns64() {
    assertEquals(64L, CassandraStoragePlugin.estimateColumnBytes(DataTypes.BLOB));
  }

  @Test
  void columnBytes_list_returns48() {
    DataType listType = DataTypes.listOf(DataTypes.TEXT);
    assertEquals(48L, CassandraStoragePlugin.estimateColumnBytes(listType));
  }

  @Test
  void columnBytes_set_returns48() {
    DataType setType = DataTypes.setOf(DataTypes.BIGINT);
    assertEquals(48L, CassandraStoragePlugin.estimateColumnBytes(setType));
  }

  @Test
  void columnBytes_map_returns96() {
    DataType mapType = DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE);
    assertEquals(96L, CassandraStoragePlugin.estimateColumnBytes(mapType));
  }

  // ============================================================================
  // estimateRowSizeBytes() — composite table estimation
  // ============================================================================

  /**
   * Builds a mock TableMetadata with the given type → expected-bytes pairs,
   * then asserts the returned row size equals the expected total (column bytes
   * + overhead: 8 + 8 per column).
   */
  private static long runEstimate(DataType... colTypes) {
    TableMetadata table = mock(TableMetadata.class);
    Map<CqlIdentifier, ColumnMetadata> cols = new LinkedHashMap<>();

    for (int i = 0; i < colTypes.length; i++) {
      CqlIdentifier id  = mock(CqlIdentifier.class);
      ColumnMetadata col = mock(ColumnMetadata.class);
      when(col.getType()).thenReturn(colTypes[i]);
      cols.put(id, col);
    }
    when(table.getColumns()).thenReturn(cols);
    return CassandraStoragePlugin.estimateRowSizeBytes(table);
  }

  @Test
  void rowSizeBytes_singleIntColumn_returnedAtFloor() {
    // INT (4 bytes) + overhead: 8 row header + 8 per-cell × 1 col = 20 bytes computed,
    // but the 32-byte floor applies (20 < 32), so the result is 32.
    assertEquals(32L, runEstimate(DataTypes.INT));
  }

  @Test
  void rowSizeBytes_multipleColumns_sumsCorrectly() {
    // INT(4) + TEXT(20) + BIGINT(8) + overhead: 8 + 8*3=24 → 4+20+8+24 = 56
    long expected = 4 + 20 + 8 + 8 + 8 * 3;
    assertEquals(expected, runEstimate(DataTypes.INT, DataTypes.TEXT, DataTypes.BIGINT));
  }

  @Test
  void rowSizeBytes_noColumns_returnsFloor32() {
    // Empty table: 0 bytes + 8 row overhead = 8 → floor at 32
    assertEquals(32L, runEstimate());
  }

  @Test
  void rowSizeBytes_allSmallColumns_atLeast32() {
    // One BOOLEAN column: 1 + 8 + 8 = 17 bytes → floor at 32
    long result = runEstimate(DataTypes.BOOLEAN);
    assertTrue(result >= 32L, "Row size must be at least 32 bytes, got: " + result);
  }

  @Test
  void rowSizeBytes_largeColumnSet_aboveFloor() {
    // 5 TEXT columns: 5×20 = 100 + 8 + 5×8 = 148
    long result = runEstimate(
        DataTypes.TEXT, DataTypes.TEXT, DataTypes.TEXT,
        DataTypes.TEXT, DataTypes.TEXT);
    long expected = 5 * 20 + 8 + 8 * 5;
    assertEquals(expected, result);
    assertTrue(result > 32, "Result should be well above floor: " + result);
  }

  @Test
  void rowSizeBytes_mapColumn_counted96BytesInEstimate() {
    // MAP (96) + INT (4) + overhead: 8 + 8×2 = 24 → total 124
    DataType mapType = DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE);
    long expected = 96 + 4 + 8 + 8 * 2;
    assertEquals(expected, runEstimate(mapType, DataTypes.INT));
  }
}
