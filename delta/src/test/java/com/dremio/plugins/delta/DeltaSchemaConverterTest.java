package com.dremio.plugins.delta;

import com.dremio.plugins.delta.write.ArrowToDeltaSchemaConverter;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Arrow -> Delta schema conversion.
 */
class DeltaSchemaConverterTest {

  @Test
  void testBasicSchemaConversion() {
    List<Field> fields = Arrays.asList(
        new Field("id",   FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("age",  FieldType.nullable(new ArrowType.Int(32, true)), null)
    );

    StructType deltaSchema = ArrowToDeltaSchemaConverter.toStructType(fields);

    assertNotNull(deltaSchema);
    assertEquals(3, deltaSchema.getFields().length);

    StructField idField = deltaSchema.get("id");
    assertNotNull(idField);
    assertInstanceOf(LongType.class, idField.getDataType(), "INT64 should map to LongType");
    assertTrue(idField.isNullable(), "All fields should be nullable");

    StructField nameField = deltaSchema.get("name");
    assertInstanceOf(StringType.class, nameField.getDataType(), "Utf8 should map to StringType");

    StructField ageField = deltaSchema.get("age");
    assertInstanceOf(IntegerType.class, ageField.getDataType(), "INT32 should map to IntegerType");
  }

  @Test
  void testMarkerColumnsExcluded() {
    // _delta_is_deleted and _delta_upsert are control markers, not data columns
    List<Field> fields = Arrays.asList(
        new Field("id",                FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("_delta_is_deleted", FieldType.nullable(new ArrowType.Bool()), null),
        new Field("_delta_upsert",     FieldType.nullable(new ArrowType.Bool()), null)
    );

    StructType deltaSchema = ArrowToDeltaSchemaConverter.toStructType(fields);

    assertEquals(1, deltaSchema.getFields().length,
        "Marker columns should be excluded from the Delta schema");
    assertNotNull(deltaSchema.get("id"));
    assertThrows(IllegalArgumentException.class, () -> deltaSchema.get("_delta_is_deleted"));
    assertThrows(IllegalArgumentException.class, () -> deltaSchema.get("_delta_upsert"));
  }

  @Test
  void testSchemaCompatibility() {
    // Compatible: incoming Arrow schema is a subset of existing Delta schema
    List<Field> incoming = Arrays.asList(
        new Field("id",   FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
    );

    StructType existing = new StructType(new StructField[]{
        new StructField("id",    new LongType(),   true),
        new StructField("name",  new StringType(), true),
        new StructField("email", new StringType(), true)
    });

    assertTrue(ArrowToDeltaSchemaConverter.isSchemaCompatible(incoming, existing),
        "Incoming schema with subset of columns should be compatible");
  }

  @Test
  void testSchemaIncompatibility_newColumn() {
    // Incompatible: incoming has a column not in Delta schema
    List<Field> incoming = Arrays.asList(
        new Field("id",      FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("new_col", FieldType.nullable(new ArrowType.Utf8()), null)
    );

    StructType existing = new StructType(new StructField[]{
        new StructField("id", new LongType(), true)
    });

    assertFalse(ArrowToDeltaSchemaConverter.isSchemaCompatible(incoming, existing),
        "Incoming schema with new column should be incompatible (needs schema evolution)");
  }

  @Test
  void testDecimalMapping() {
    List<Field> fields = List.of(
        new Field("price",
            FieldType.nullable(new ArrowType.Decimal(18, 4, 128)), null)
    );

    StructType deltaSchema = ArrowToDeltaSchemaConverter.toStructType(fields);
    io.delta.standalone.types.DataType priceType = deltaSchema.get("price").getDataType();

    assertInstanceOf(io.delta.standalone.types.DecimalType.class, priceType);
    io.delta.standalone.types.DecimalType decimal =
        (io.delta.standalone.types.DecimalType) priceType;
    assertEquals(18, decimal.getPrecision());
    assertEquals(4,  decimal.getScale());
  }
}
