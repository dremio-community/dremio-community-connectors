package com.dremio.plugins.kafka.avro;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaAvroConverter: Avro→Arrow schema mapping and
 * nullable union resolution.
 */
class KafkaAvroConverterTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
  }

  private static Schema recordWith(String fieldsJson) {
    return parse("{\"type\":\"record\",\"name\":\"Test\",\"fields\":" + fieldsJson + "}");
  }

  // ---------------------------------------------------------------------------
  // Primitive type mapping
  // ---------------------------------------------------------------------------

  @Test
  void primitives_mappedCorrectly() {
    Schema schema = recordWith("[" +
        "{\"name\":\"b\",  \"type\":\"boolean\"}," +
        "{\"name\":\"i\",  \"type\":\"int\"}," +
        "{\"name\":\"l\",  \"type\":\"long\"}," +
        "{\"name\":\"f\",  \"type\":\"float\"}," +
        "{\"name\":\"d\",  \"type\":\"double\"}," +
        "{\"name\":\"s\",  \"type\":\"string\"}," +
        "{\"name\":\"by\", \"type\":\"bytes\"}," +
        "{\"name\":\"en\", \"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(8, fields.size());

    assertEquals(ArrowType.Bool.INSTANCE,                    fields.get(0).getType()); // boolean
    assertEquals(new ArrowType.Int(32, true),                fields.get(1).getType()); // int
    assertEquals(new ArrowType.Int(64, true),                fields.get(2).getType()); // long
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), fields.get(3).getType()); // float
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), fields.get(4).getType()); // double
    assertEquals(ArrowType.Utf8.INSTANCE,                    fields.get(5).getType()); // string
    assertEquals(ArrowType.Utf8.INSTANCE,                    fields.get(6).getType()); // bytes → hex string
    assertEquals(ArrowType.Utf8.INSTANCE,                    fields.get(7).getType()); // enum → string
  }

  @Test
  void complexTypes_mappedToVarchar() {
    Schema schema = recordWith("[" +
        "{\"name\":\"arr\",  \"type\":{\"type\":\"array\",\"items\":\"string\"}}," +
        "{\"name\":\"mp\",   \"type\":{\"type\":\"map\",\"values\":\"int\"}}," +
        "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(3, fields.size());
    fields.forEach(f -> assertEquals(ArrowType.Utf8.INSTANCE, f.getType(),
        "Complex type " + f.getName() + " should map to VARCHAR"));
  }

  // ---------------------------------------------------------------------------
  // Nullable union [null, T]
  // ---------------------------------------------------------------------------

  @Test
  void nullableUnion_string_mapsToNullableVarchar() {
    Schema schema = recordWith("[" +
        "{\"name\":\"s\",\"type\":[\"null\",\"string\"],\"default\":null}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(ArrowType.Utf8.INSTANCE, fields.get(0).getType());
    assertTrue(fields.get(0).getFieldType().isNullable(), "Union [null, string] should be nullable");
  }

  @Test
  void nullableUnion_long_mapsToNullableBigint() {
    Schema schema = recordWith("[" +
        "{\"name\":\"l\",\"type\":[\"null\",\"long\"],\"default\":null}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(new ArrowType.Int(64, true), fields.get(0).getType());
  }

  @Test
  void nullableUnion_int_mapsToNullableInt() {
    Schema schema = recordWith("[" +
        "{\"name\":\"n\",\"type\":[\"null\",\"int\"],\"default\":null}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(new ArrowType.Int(32, true), fields.get(0).getType());
  }

  @Test
  void nullableUnion_double_mapsToNullableDouble() {
    Schema schema = recordWith("[" +
        "{\"name\":\"d\",\"type\":[\"null\",\"double\"],\"default\":null}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
        fields.get(0).getType());
  }

  @Test
  void nullableUnion_nestedRecord_mapsToVarchar() {
    Schema schema = recordWith("[{\"name\":\"inner\",\"type\":[\"null\"," +
        "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}" +
        "],\"default\":null}]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(ArrowType.Utf8.INSTANCE, fields.get(0).getType());
  }

  @Test
  void nullField_skipped_otherFieldsKept() {
    Schema schema = recordWith("[" +
        "{\"name\":\"nothing\",\"type\":\"null\"}," +
        "{\"name\":\"something\",\"type\":\"string\"}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size(), "null field should be skipped, string field kept");
    assertEquals("something", fields.get(0).getName());
  }

  @Test
  void pureNullType_skipped() {
    // A field whose type is simply "null" should produce no Arrow field
    Schema schema = recordWith("[" +
        "{\"name\":\"nothing\",\"type\":\"null\"}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(0, fields.size(), "pure null field should produce no Arrow field");
  }

  @Test
  void multiTypeUnion_mapsToVarchar() {
    // [string, int] — not a nullable union, serialized as JSON
    Schema schema = recordWith("[" +
        "{\"name\":\"poly\",\"type\":[\"string\",\"int\"]}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals(1, fields.size());
    assertEquals(ArrowType.Utf8.INSTANCE, fields.get(0).getType());
  }

  // ---------------------------------------------------------------------------
  // Schema with no fields / null schema
  // ---------------------------------------------------------------------------

  @Test
  void nullSchema_returnsEmptyList() {
    List<Field> fields = KafkaAvroConverter.avroToArrowFields(null);
    assertTrue(fields.isEmpty());
  }

  @Test
  void nonRecordSchema_returnsEmptyList() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    List<Field> fields = KafkaAvroConverter.avroToArrowFields(arraySchema);
    assertTrue(fields.isEmpty());
  }

  @Test
  void emptyRecord_returnsEmptyList() {
    Schema schema = recordWith("[]");
    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertTrue(fields.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Field names are preserved
  // ---------------------------------------------------------------------------

  @Test
  void fieldNames_preserved() {
    Schema schema = recordWith("[" +
        "{\"name\":\"order_id\",\"type\":\"long\"}," +
        "{\"name\":\"customer_name\",\"type\":\"string\"}," +
        "{\"name\":\"total_amount\",\"type\":\"double\"}" +
        "]");

    List<Field> fields = KafkaAvroConverter.avroToArrowFields(schema);
    assertEquals("order_id",      fields.get(0).getName());
    assertEquals("customer_name", fields.get(1).getName());
    assertEquals("total_amount",  fields.get(2).getName());
  }
}
