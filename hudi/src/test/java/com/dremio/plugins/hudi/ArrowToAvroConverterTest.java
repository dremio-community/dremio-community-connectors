package com.dremio.plugins.hudi;

import com.dremio.plugins.hudi.write.ArrowToAvroConverter;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for Arrow -> Avro schema and record conversion.
 * No Dremio cluster or running services required.
 *
 * We mock the ValueVector *interface* rather than concrete classes
 * (BigIntVector, VarCharVector, etc.) to avoid JVM module restrictions
 * that prevent Mockito from subclassing Arrow's internal vector hierarchy.
 * The converter only calls isNull() and getObject(), both on ValueVector.
 */
class ArrowToAvroConverterTest {

  // -----------------------------------------------------------------------
  // Schema conversion
  // -----------------------------------------------------------------------

  @Test
  void testBasicSchemaConversion() {
    List<Field> fields = Arrays.asList(
        new Field("id",   FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("ts",   FieldType.nullable(new ArrowType.Int(64, true)), null)
    );

    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "TestRecord", "com.test");

    assertNotNull(avroSchema);
    assertEquals(Schema.Type.RECORD, avroSchema.getType());
    assertEquals("TestRecord", avroSchema.getName());
    assertEquals(3, avroSchema.getFields().size());

    Schema.Field idField = avroSchema.getField("id");
    assertNotNull(idField);
    assertEquals(Schema.Type.UNION, idField.schema().getType());
    assertTrue(idField.schema().getTypes().stream()
        .anyMatch(s -> s.getType() == Schema.Type.LONG),
        "INT64 should map to Avro LONG");
  }

  @Test
  void testStringFieldMapping() {
    List<Field> fields = List.of(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Test", "com.test");

    assertTrue(avroSchema.getField("name").schema().getTypes().stream()
        .anyMatch(s -> s.getType() == Schema.Type.STRING),
        "Utf8 should map to Avro STRING");
  }

  @Test
  void testInt32FieldMapping() {
    List<Field> fields = List.of(
        new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Test", "com.test");

    assertTrue(avroSchema.getField("count").schema().getTypes().stream()
        .anyMatch(s -> s.getType() == Schema.Type.INT),
        "INT32 should map to Avro INT");
  }

  @Test
  void testFieldNameSanitization() {
    List<Field> fields = List.of(
        new Field("first-name", FieldType.nullable(new ArrowType.Utf8()), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Test", "com.test");

    assertNotNull(avroSchema.getField("first_name"),
        "Hyphens should be replaced with underscores");
  }

  // -----------------------------------------------------------------------
  // Record reading — mock ValueVector interface, not concrete classes
  // -----------------------------------------------------------------------

  @Test
  void testToGenericRecord_readsVectorValues() {
    List<Field> fields = Arrays.asList(
        new Field("id",   FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Row", "com.test");

    ValueVector idVec = mock(ValueVector.class);
    when(idVec.isNull(0)).thenReturn(false);
    when(idVec.getObject(0)).thenReturn(42L);

    ValueVector nameVec = mock(ValueVector.class);
    when(nameVec.isNull(0)).thenReturn(false);
    when(nameVec.getObject(0)).thenReturn(new org.apache.arrow.vector.util.Text("Alice"));

    List<ValueVector> vectors = Arrays.asList(idVec, nameVec);
    GenericRecord record = ArrowToAvroConverter.toGenericRecord(fields, avroSchema, vectors, 0);

    assertNotNull(record);
    assertEquals(42L, record.get("id"));
    assertEquals("Alice", record.get("name").toString());
  }

  @Test
  void testToGenericRecord_nullValues() {
    List<Field> fields = List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Row", "com.test");

    ValueVector idVec = mock(ValueVector.class);
    when(idVec.isNull(0)).thenReturn(true);

    GenericRecord record = ArrowToAvroConverter.toGenericRecord(
        fields, avroSchema, List.of(idVec), 0);

    assertNull(record.get("id"), "Null vector slot should produce null");
  }

  @Test
  void testToGenericRecord_int32Field() {
    List<Field> fields = List.of(
        new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)
    );
    Schema avroSchema = ArrowToAvroConverter.toAvroSchema(fields, "Row", "com.test");

    ValueVector countVec = mock(ValueVector.class);
    when(countVec.isNull(0)).thenReturn(false);
    when(countVec.getObject(0)).thenReturn(7);

    GenericRecord record = ArrowToAvroConverter.toGenericRecord(
        fields, avroSchema, List.of(countVec), 0);

    assertEquals(7, record.get("count"));
  }
}
