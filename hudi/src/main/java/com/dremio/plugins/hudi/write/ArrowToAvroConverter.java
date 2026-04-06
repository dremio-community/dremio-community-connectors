package com.dremio.plugins.hudi.write;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts between Apache Arrow (Dremio's internal format) and Apache Avro
 * (Hudi's record format).
 *
 * This class is intentionally free of Dremio-specific imports so it can be
 * compiled and unit-tested with only Arrow + Avro on the classpath.
 * Dremio integration (BatchSchema, VectorAccessible) is handled by the caller
 * (HudiRecordWriter), which extracts plain Arrow types before calling here.
 *
 * Type Mapping
 * ------------
 *   Arrow INT8/16/32  ->  Avro INT
 *   Arrow INT64       ->  Avro LONG
 *   Arrow FLOAT       ->  Avro FLOAT
 *   Arrow DOUBLE      ->  Avro DOUBLE
 *   Arrow Utf8        ->  Avro STRING
 *   Arrow Bool        ->  Avro BOOLEAN
 *   Arrow Binary      ->  Avro BYTES
 *   Arrow Date32      ->  Avro INT  (logicalType: date)
 *   Arrow Timestamp   ->  Avro LONG (logicalType: timestamp-micros)
 *   Arrow Decimal     ->  Avro BYTES (logicalType: decimal)
 *   Arrow List        ->  Avro ARRAY
 *   Arrow Struct      ->  Avro RECORD (nested)
 */
public class ArrowToAvroConverter {

  private static final Logger logger = LoggerFactory.getLogger(ArrowToAvroConverter.class);

  private ArrowToAvroConverter() {}

  // -----------------------------------------------------------------------
  // Schema conversion
  // -----------------------------------------------------------------------

  /**
   * Converts a list of Arrow Fields to an Avro Schema.
   * All fields are emitted as nullable unions [null, type] for schema evolution.
   *
   * @param fields      Arrow field definitions (from BatchSchema.getFields())
   * @param recordName  Avro record name
   * @param namespace   Avro namespace
   */
  public static Schema toAvroSchema(List<Field> fields, String recordName, String namespace) {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(recordName).namespace(namespace).fields();

    for (Field arrowField : fields) {
      String name = arrowField.getName();
      // Exclude internal write-mode marker columns from the output Avro/Parquet schema
      if ("_hoodie_is_deleted".equals(name) || "_hoodie_upsert".equals(name)) {
        continue;
      }
      Schema fieldSchema = arrowTypeToAvroSchema(arrowField.getType(), arrowField);
      Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
      assembler = assembler.name(sanitizeFieldName(name))
                           .type(nullable)
                           .withDefault(null);
    }
    return assembler.endRecord();
  }

  private static Schema arrowTypeToAvroSchema(ArrowType arrowType, Field field) {
    switch (arrowType.getTypeID()) {
      case Int:
        return ((ArrowType.Int) arrowType).getBitWidth() <= 32
            ? Schema.create(Schema.Type.INT) : Schema.create(Schema.Type.LONG);
      case FloatingPoint:
        return ((ArrowType.FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE
            ? Schema.create(Schema.Type.FLOAT) : Schema.create(Schema.Type.DOUBLE);
      case Utf8: case LargeUtf8:
        return Schema.create(Schema.Type.STRING);
      case Bool:
        return Schema.create(Schema.Type.BOOLEAN);
      case Binary: case LargeBinary: case FixedSizeBinary:
        return Schema.create(Schema.Type.BYTES);
      case Date:
        return org.apache.avro.LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case Timestamp:
        return org.apache.avro.LogicalTypes.timestampMicros()
            .addToSchema(Schema.create(Schema.Type.LONG));
      case Decimal:
        ArrowType.Decimal d = (ArrowType.Decimal) arrowType;
        return org.apache.avro.LogicalTypes.decimal(d.getPrecision(), d.getScale())
            .addToSchema(Schema.create(Schema.Type.BYTES));
      case List: case LargeList:
        Field el = field.getChildren().isEmpty() ? null : field.getChildren().get(0);
        Schema elSchema = el != null ? arrowTypeToAvroSchema(el.getType(), el)
            : Schema.create(Schema.Type.NULL);
        return Schema.createArray(elSchema);
      case Struct:
        List<Schema.Field> avroFields = new ArrayList<>();
        for (Field child : field.getChildren()) {
          Schema cs = arrowTypeToAvroSchema(child.getType(), child);
          avroFields.add(new Schema.Field(sanitizeFieldName(child.getName()),
              Schema.createUnion(Schema.create(Schema.Type.NULL), cs), null, (Object) null));
        }
        return Schema.createRecord(sanitizeFieldName(field.getName()) + "_type",
            null, "com.dremio.plugins.hudi.nested", false, avroFields);
      case Null:
        return Schema.create(Schema.Type.NULL);
      default:
        logger.warn("Unsupported Arrow type {}. Mapping to STRING.", arrowType.getTypeID());
        return Schema.create(Schema.Type.STRING);
    }
  }

  // -----------------------------------------------------------------------
  // Record conversion
  // -----------------------------------------------------------------------

  /**
   * Converts a single row to an Avro GenericRecord by reading from a list of
   * Arrow ValueVectors (one per column, in field order).
   *
   * @param fields     Arrow field definitions (schema, in column order)
   * @param avroSchema Target Avro schema
   * @param vectors    One ValueVector per column, in the same order as fields
   * @param rowIndex   Row index within the current batch
   */
  public static GenericRecord toGenericRecord(
      List<Field> fields,
      Schema avroSchema,
      List<ValueVector> vectors,
      int rowIndex) {

    GenericData.Record record = new GenericData.Record(avroSchema);

    for (int i = 0; i < fields.size(); i++) {
      Field arrowField = fields.get(i);
      String name = arrowField.getName();

      // Skip marker columns — they are not present in the Avro schema
      if ("_hoodie_is_deleted".equals(name) || "_hoodie_upsert".equals(name)) {
        continue;
      }

      String avroName = sanitizeFieldName(name);
      if (i >= vectors.size()) {
        record.put(avroName, null);
        continue;
      }
      ValueVector vector = vectors.get(i);
      record.put(avroName, vector.isNull(rowIndex) ? null
          : readValue(vector, arrowField.getType(), rowIndex));
    }
    return record;
  }

  private static Object readValue(ValueVector vector, ArrowType arrowType, int rowIndex) {
    switch (arrowType.getTypeID()) {
      case Int: {
        Object obj = vector.getObject(rowIndex);
        return ((ArrowType.Int) arrowType).getBitWidth() <= 32
            ? (obj instanceof Number ? ((Number) obj).intValue() : obj)
            : (obj instanceof Number ? ((Number) obj).longValue() : obj);
      }
      case FloatingPoint: {
        Object obj = vector.getObject(rowIndex);
        return ((ArrowType.FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE
            ? (obj instanceof Number ? ((Number) obj).floatValue() : obj)
            : (obj instanceof Number ? ((Number) obj).doubleValue() : obj);
      }
      case Utf8: case LargeUtf8: {
        Object obj = vector.getObject(rowIndex);
        return obj != null ? obj.toString() : null;
      }
      case Bool:
        return vector.getObject(rowIndex);
      case Binary: case LargeBinary: case FixedSizeBinary: {
        Object obj = vector.getObject(rowIndex);
        return obj instanceof byte[] ? ByteBuffer.wrap((byte[]) obj) : obj;
      }
      case Date: {
        Object obj = vector.getObject(rowIndex);
        if (obj instanceof LocalDate) return (int) ((LocalDate) obj).toEpochDay();
        return obj instanceof Number ? ((Number) obj).intValue() : obj;
      }
      case Timestamp: {
        Object obj = vector.getObject(rowIndex);
        if (obj instanceof Instant) {
          Instant ins = (Instant) obj;
          return ins.getEpochSecond() * 1_000_000L + ins.getNano() / 1_000L;
        }
        if (obj instanceof LocalDateTime) {
          LocalDateTime ldt = (LocalDateTime) obj;
          return ldt.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L + ldt.getNano() / 1_000L;
        }
        return obj instanceof Number ? ((Number) obj).longValue() : obj;
      }
      case Decimal: {
        Object obj = vector.getObject(rowIndex);
        if (obj instanceof java.math.BigDecimal)
          return ByteBuffer.wrap(((java.math.BigDecimal) obj).unscaledValue().toByteArray());
        return obj;
      }
      default: {
        Object obj = vector.getObject(rowIndex);
        return obj != null ? obj.toString() : null;
      }
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  static String sanitizeFieldName(String name) {
    if (name == null || name.isEmpty()) return "_field";
    return name.replaceAll("[^A-Za-z0-9_]", "_");
  }
}
