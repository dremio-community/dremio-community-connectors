package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CassandraTypeConverter} — Cassandra CQL type → Arrow type/field mapping.
 *
 * Two test groups:
 *   1. {@code toArrowType()} — scalar type mapping (no mocks needed)
 *   2. {@code toArrowField()} — field construction including collection nesting
 */
class CassandraTypeConverterTest {

  // ---------------------------------------------------------------------------
  // toArrowType() — primitive / scalar types
  // ---------------------------------------------------------------------------

  @Test
  void text_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.TEXT));
  }

  @Test
  void ascii_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.ASCII));
  }

  @Test
  void int_mapsToInt32Signed() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.INT);
    ArrowType.Int arrowInt = assertInstanceOf(ArrowType.Int.class, result);
    assertEquals(32, arrowInt.getBitWidth());
    assertTrue(arrowInt.getIsSigned());
  }

  @Test
  void bigint_mapsToInt64Signed() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.BIGINT);
    ArrowType.Int arrowInt = assertInstanceOf(ArrowType.Int.class, result);
    assertEquals(64, arrowInt.getBitWidth());
    assertTrue(arrowInt.getIsSigned());
  }

  @Test
  void counter_mapsToInt64Signed() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.COUNTER);
    ArrowType.Int arrowInt = assertInstanceOf(ArrowType.Int.class, result);
    assertEquals(64, arrowInt.getBitWidth());
    assertTrue(arrowInt.getIsSigned());
  }

  @Test
  void smallint_mapsToInt16Signed() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.SMALLINT);
    ArrowType.Int arrowInt = assertInstanceOf(ArrowType.Int.class, result);
    assertEquals(16, arrowInt.getBitWidth());
    assertTrue(arrowInt.getIsSigned());
  }

  @Test
  void tinyint_mapsToInt8Signed() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.TINYINT);
    ArrowType.Int arrowInt = assertInstanceOf(ArrowType.Int.class, result);
    assertEquals(8, arrowInt.getBitWidth());
    assertTrue(arrowInt.getIsSigned());
  }

  @Test
  void float_mapsToSinglePrecisionFloat() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.FLOAT);
    ArrowType.FloatingPoint fp = assertInstanceOf(ArrowType.FloatingPoint.class, result);
    assertEquals(FloatingPointPrecision.SINGLE, fp.getPrecision());
  }

  @Test
  void double_mapsToDoublePrecisionFloat() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.DOUBLE);
    ArrowType.FloatingPoint fp = assertInstanceOf(ArrowType.FloatingPoint.class, result);
    assertEquals(FloatingPointPrecision.DOUBLE, fp.getPrecision());
  }

  @Test
  void boolean_mapsToBool() {
    assertEquals(ArrowType.Bool.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.BOOLEAN));
  }

  @Test
  void timestamp_mapsToTimestampMilliseconds() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.TIMESTAMP);
    ArrowType.Timestamp ts = assertInstanceOf(ArrowType.Timestamp.class, result);
    assertEquals(TimeUnit.MILLISECOND, ts.getUnit());
    assertNull(ts.getTimezone()); // no timezone attached
  }

  @Test
  void date_mapsToDateDay() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.DATE);
    ArrowType.Date date = assertInstanceOf(ArrowType.Date.class, result);
    assertEquals(DateUnit.DAY, date.getUnit());
  }

  @Test
  void time_mapsToTimeNanoseconds64bit() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.TIME);
    ArrowType.Time time = assertInstanceOf(ArrowType.Time.class, result);
    assertEquals(TimeUnit.NANOSECOND, time.getUnit());
    assertEquals(64, time.getBitWidth());
  }

  @Test
  void uuid_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.UUID));
  }

  @Test
  void timeuuid_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.TIMEUUID));
  }

  @Test
  void blob_mapsToBinary() {
    assertEquals(ArrowType.Binary.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.BLOB));
  }

  @Test
  void decimal_mapsToDecimal128WithScale10() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.DECIMAL);
    ArrowType.Decimal dec = assertInstanceOf(ArrowType.Decimal.class, result);
    assertEquals(38,  dec.getPrecision());
    assertEquals(10,  dec.getScale());
    assertEquals(128, dec.getBitWidth());
  }

  @Test
  void varint_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.VARINT));
  }

  @Test
  void inet_mapsToUtf8() {
    assertEquals(ArrowType.Utf8.INSTANCE, CassandraTypeConverter.toArrowType(DataTypes.INET));
  }

  @Test
  void duration_mapsToArrowDurationMilliseconds() {
    ArrowType result = CassandraTypeConverter.toArrowType(DataTypes.DURATION);
    ArrowType.Duration dur = assertInstanceOf(ArrowType.Duration.class, result);
    assertEquals(TimeUnit.MILLISECOND, dur.getUnit());
  }

  // ---------------------------------------------------------------------------
  // toArrowField() — scalar field: nullable + correct type
  // ---------------------------------------------------------------------------

  @Test
  void toArrowField_primitive_isNullable() {
    Field f = CassandraTypeConverter.toArrowField("col", DataTypes.INT);
    assertTrue(f.isNullable(), "All Cassandra columns must map to nullable Arrow fields");
  }

  @Test
  void toArrowField_primitive_hasNoChildren() {
    Field f = CassandraTypeConverter.toArrowField("col", DataTypes.TEXT);
    assertTrue(f.getChildren().isEmpty());
  }

  @Test
  void toArrowField_primitive_fieldNamePreserved() {
    Field f = CassandraTypeConverter.toArrowField("my_col", DataTypes.BIGINT);
    assertEquals("my_col", f.getName());
  }

  // ---------------------------------------------------------------------------
  // toArrowField() — LIST<T> → Arrow List with "$data$" child
  // ---------------------------------------------------------------------------

  @Test
  void toArrowField_list_isArrowListType() {
    Field f = CassandraTypeConverter.toArrowField("tags", DataTypes.listOf(DataTypes.TEXT));
    assertInstanceOf(ArrowType.List.class, f.getType());
  }

  @Test
  void toArrowField_list_hasOneChild() {
    Field f = CassandraTypeConverter.toArrowField("tags", DataTypes.listOf(DataTypes.TEXT));
    assertEquals(1, f.getChildren().size());
  }

  @Test
  void toArrowField_list_childNameIsDataVectorName() {
    Field f = CassandraTypeConverter.toArrowField("tags", DataTypes.listOf(DataTypes.TEXT));
    assertEquals(CassandraTypeConverter.DATA_VECTOR_NAME, f.getChildren().get(0).getName());
  }

  @Test
  void toArrowField_list_childTypeMatchesElementType() {
    Field f = CassandraTypeConverter.toArrowField("scores", DataTypes.listOf(DataTypes.INT));
    ArrowType.Int childType = assertInstanceOf(
        ArrowType.Int.class, f.getChildren().get(0).getType());
    assertEquals(32, childType.getBitWidth());
  }

  // ---------------------------------------------------------------------------
  // toArrowField() — SET<T> → Arrow List (Arrow has no native Set type)
  // ---------------------------------------------------------------------------

  @Test
  void toArrowField_set_isArrowListType() {
    Field f = CassandraTypeConverter.toArrowField("tags", DataTypes.setOf(DataTypes.TEXT));
    assertInstanceOf(ArrowType.List.class, f.getType());
  }

  @Test
  void toArrowField_set_childTypeMatchesElementType() {
    Field f = CassandraTypeConverter.toArrowField("ids", DataTypes.setOf(DataTypes.BIGINT));
    ArrowType.Int childType = assertInstanceOf(
        ArrowType.Int.class, f.getChildren().get(0).getType());
    assertEquals(64, childType.getBitWidth());
  }

  // ---------------------------------------------------------------------------
  // toArrowField() — MAP<K,V> → Arrow Map with struct child
  // ---------------------------------------------------------------------------

  @Test
  void toArrowField_map_isArrowMapType() {
    Field f = CassandraTypeConverter.toArrowField("props",
        DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE));
    assertInstanceOf(ArrowType.Map.class, f.getType());
  }

  @Test
  void toArrowField_map_hasOneStructChild() {
    Field f = CassandraTypeConverter.toArrowField("props",
        DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE));
    assertEquals(1, f.getChildren().size());
    assertInstanceOf(ArrowType.Struct.class, f.getChildren().get(0).getType());
  }

  @Test
  void toArrowField_map_structHasKeyAndValueChildren() {
    Field f = CassandraTypeConverter.toArrowField("props",
        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    Field struct = f.getChildren().get(0); // "$data$" struct
    assertEquals(2, struct.getChildren().size());
    assertEquals("key",   struct.getChildren().get(0).getName());
    assertEquals("value", struct.getChildren().get(1).getName());
  }

  @Test
  void toArrowField_map_keyIsNotNullable() {
    Field f = CassandraTypeConverter.toArrowField("props",
        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    Field keyField = f.getChildren().get(0).getChildren().get(0);
    assertFalse(keyField.isNullable(), "CQL map keys must be non-nullable");
  }

  @Test
  void toArrowField_map_valueIsNullable() {
    Field f = CassandraTypeConverter.toArrowField("props",
        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    Field valueField = f.getChildren().get(0).getChildren().get(1);
    assertTrue(valueField.isNullable(), "CQL map values must be nullable");
  }

  // ---------------------------------------------------------------------------
  // toArrowField() — UDT → Arrow Struct with one child per UDT field
  // ---------------------------------------------------------------------------

  @Test
  void toArrowField_udt_isArrowStructType() {
    UserDefinedType udt = mock(UserDefinedType.class);
    CqlIdentifier fieldId = mock(CqlIdentifier.class);
    when(fieldId.asInternal()).thenReturn("street");
    when(udt.getFieldNames()).thenReturn(Collections.singletonList(fieldId));
    when(udt.getFieldTypes()).thenReturn(Collections.singletonList(DataTypes.TEXT));

    Field f = CassandraTypeConverter.toArrowField("address", udt);
    assertInstanceOf(ArrowType.Struct.class, f.getType());
  }

  @Test
  void toArrowField_udt_childrenMatchUdtFields() {
    UserDefinedType udt = mock(UserDefinedType.class);

    CqlIdentifier streetId = mock(CqlIdentifier.class);
    CqlIdentifier zipId    = mock(CqlIdentifier.class);
    when(streetId.asInternal()).thenReturn("street");
    when(zipId.asInternal()).thenReturn("zip");

    when(udt.getFieldNames()).thenReturn(Arrays.asList(streetId, zipId));
    when(udt.getFieldTypes()).thenReturn(Arrays.asList(DataTypes.TEXT, DataTypes.INT));

    Field f = CassandraTypeConverter.toArrowField("address", udt);
    assertEquals(2, f.getChildren().size());
    assertEquals("street", f.getChildren().get(0).getName());
    assertEquals("zip",    f.getChildren().get(1).getName());
  }

  @Test
  void toArrowField_udt_emptyUdt_noChildren() {
    UserDefinedType udt = mock(UserDefinedType.class);
    when(udt.getFieldNames()).thenReturn(Collections.emptyList());
    when(udt.getFieldTypes()).thenReturn(Collections.emptyList());

    Field f = CassandraTypeConverter.toArrowField("empty_udt", udt);
    assertInstanceOf(ArrowType.Struct.class, f.getType());
    assertTrue(f.getChildren().isEmpty());
  }
}
