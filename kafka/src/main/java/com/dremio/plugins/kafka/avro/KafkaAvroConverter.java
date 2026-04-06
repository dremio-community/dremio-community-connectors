package com.dremio.plugins.kafka.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Bridges Apache Avro schemas/records to Apache Arrow fields/vectors.
 *
 * Schema mapping (Avro → Arrow):
 *   null              → always null (skip field)
 *   boolean           → Bit (Bool)
 *   int               → Int(32, signed)
 *   long              → Int(64, signed)
 *   float             → FloatingPoint(SINGLE)
 *   double            → FloatingPoint(DOUBLE)
 *   bytes / fixed     → Utf8 (hex-encoded)
 *   string / enum     → Utf8
 *   record            → Utf8 (JSON-serialized nested record)
 *   array / map       → Utf8 (JSON-serialized)
 *   union [null, T]   → nullable T  (most common nullable pattern)
 *   union [T1, T2, …] → Utf8 (JSON representation)
 *
 * Nested records and collections are intentionally flattened to JSON strings
 * to keep the Arrow schema flat — deep nesting is rare at the top level and
 * hard to represent in relational SQL without explode(). Users who need nested
 * access can use Dremio's FLATTEN / CONVERT_FROM functions on the JSON string.
 */
public class KafkaAvroConverter {

  private static final Logger logger = LoggerFactory.getLogger(KafkaAvroConverter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Convert an Avro record schema to a list of Arrow Fields.
   * Only top-level fields are converted; nested records become VARCHAR (JSON).
   *
   * @param avroSchema the Avro RECORD schema
   * @return list of Arrow Fields
   */
  public static List<Field> avroToArrowFields(Schema avroSchema) {
    List<Field> fields = new ArrayList<>();
    if (avroSchema == null || avroSchema.getType() != Schema.Type.RECORD) {
      return fields;
    }
    for (Schema.Field avroField : avroSchema.getFields()) {
      ArrowType arrowType = avroTypeToArrow(avroField.schema());
      if (arrowType != null) {
        fields.add(new Field(avroField.name(), FieldType.nullable(arrowType), null));
      }
    }
    return fields;
  }

  /**
   * Map a single Avro schema to an Arrow type.
   * Returns null for pure-null schemas (nothing to store).
   */
  private static ArrowType avroTypeToArrow(Schema schema) {
    switch (schema.getType()) {
      case NULL:
        return null; // skip

      case BOOLEAN:
        return ArrowType.Bool.INSTANCE;

      case INT:
        return new ArrowType.Int(32, true);

      case LONG:
        return new ArrowType.Int(64, true);

      case FLOAT:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

      case DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

      case BYTES:
      case FIXED:
      case STRING:
      case ENUM:
        return ArrowType.Utf8.INSTANCE;

      case RECORD:
      case ARRAY:
      case MAP:
        // Serialize complex types to JSON string
        return ArrowType.Utf8.INSTANCE;

      case UNION:
        return resolveUnionType(schema);

      default:
        // Fall back to string for any unknown types
        return ArrowType.Utf8.INSTANCE;
    }
  }

  /**
   * Resolve a UNION schema to an Arrow type.
   * The common [null, T] pattern becomes nullable T.
   * Multi-type unions become VARCHAR (JSON).
   */
  private static ArrowType resolveUnionType(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    // Filter out null
    List<Schema> nonNullTypes = new ArrayList<>();
    for (Schema s : types) {
      if (s.getType() != Schema.Type.NULL) {
        nonNullTypes.add(s);
      }
    }
    if (nonNullTypes.isEmpty()) {
      // Union is only null — skip
      return null;
    }
    if (nonNullTypes.size() == 1) {
      // Standard nullable field: [null, T]
      return avroTypeToArrow(nonNullTypes.get(0));
    }
    // Multi-type union → serialize as JSON string
    return ArrowType.Utf8.INSTANCE;
  }

  /**
   * Write a single Avro field value to an Arrow vector at row index {@code idx}.
   *
   * @param vector    the Arrow vector to write into
   * @param arrowType the Arrow type of the vector
   * @param value     the Avro field value (may be a GenericRecord, List, Map, etc.)
   * @param idx       the row index in the batch
   */
  public static void writeToVector(ValueVector vector, ArrowType arrowType, Object value, int idx) {
    if (value == null) {
      // Leave as null — Arrow vectors are null by default
      return;
    }
    try {
      if (arrowType instanceof ArrowType.Bool) {
        ((BitVector) vector).setSafe(idx, Boolean.TRUE.equals(value) ? 1 : 0);

      } else if (arrowType instanceof ArrowType.Int) {
        int bits = ((ArrowType.Int) arrowType).getBitWidth();
        if (bits == 32) {
          ((IntVector) vector).setSafe(idx, ((Number) value).intValue());
        } else {
          ((BigIntVector) vector).setSafe(idx, ((Number) value).longValue());
        }

      } else if (arrowType instanceof ArrowType.FloatingPoint) {
        FloatingPointPrecision prec = ((ArrowType.FloatingPoint) arrowType).getPrecision();
        if (prec == FloatingPointPrecision.SINGLE) {
          ((Float4Vector) vector).setSafe(idx, ((Number) value).floatValue());
        } else {
          ((Float8Vector) vector).setSafe(idx, ((Number) value).doubleValue());
        }

      } else {
        // For Utf8 (string, enum, bytes, fixed, complex types) — convert to string
        String strVal = toStringValue(value);
        byte[] bytes = strVal.getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) vector).setSafe(idx, bytes, 0, bytes.length);
      }
    } catch (Exception e) {
      logger.debug("Failed to write Avro field to vector (type={}): {}", arrowType, e.getMessage());
      // Leave null on error
    }
  }

  /**
   * Convert an Avro value to a string representation.
   *
   * GenericRecord: use Avro's own toString() which produces valid JSON.
   *   Jackson's default ObjectMapper serializes GenericRecord via JavaBean reflection
   *   and does NOT produce correct Avro JSON — always use Avro's native serialization.
   * List/Map: Jackson serializes standard Java collections correctly.
   * Bytes/Fixed: hex-encoded.
   * Utf8 (Avro string type): toString() returns the string value.
   * EnumSymbol: toString() returns the symbol name.
   */
  private static String toStringValue(Object value) {
    if (value instanceof org.apache.avro.generic.GenericRecord) {
      // Avro's toString() produces valid JSON, e.g. {"field1": "value1", "field2": 42}
      return value.toString();
    }
    if (value instanceof java.util.List || value instanceof java.util.Map) {
      try {
        return MAPPER.writeValueAsString(value);
      } catch (Exception e) {
        return value.toString();
      }
    }
    if (value instanceof byte[]) {
      return bytesToHex((byte[]) value);
    }
    if (value instanceof org.apache.avro.generic.GenericData.Fixed) {
      return bytesToHex(((org.apache.avro.generic.GenericData.Fixed) value).bytes());
    }
    return value.toString();
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
