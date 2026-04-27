package com.dremio.plugins.spanner;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Maps Spanner column types to Apache Arrow field types.
 *
 * Spanner type strings (from INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE):
 *   BOOL, INT64, FLOAT32, FLOAT64, STRING(MAX), STRING(N),
 *   BYTES(MAX), BYTES(N), DATE, TIMESTAMP, NUMERIC, JSON,
 *   ARRAY<T>, PROTO<...>
 *
 * Arrays and complex types are mapped to UTF8 (JSON-serialized string).
 */
public final class SpannerTypeConverter {

  private SpannerTypeConverter() {}

  /**
   * Returns the Arrow {@link Field} for a given Spanner column type string.
   * Nullable by default (Spanner columns are nullable unless NOT NULL is set).
   */
  public static Field toArrowField(String columnName, String spannerType, boolean nullable) {
    ArrowType arrowType = toArrowType(spannerType);
    return new Field(columnName, new FieldType(nullable, arrowType, null), null);
  }

  /**
   * Returns the Arrow {@link ArrowType} for a given Spanner type string.
   */
  public static ArrowType toArrowType(String spannerType) {
    if (spannerType == null) return ArrowType.Utf8.INSTANCE;
    String t = spannerType.trim().toUpperCase();

    if (t.equals("BOOL"))       return ArrowType.Bool.INSTANCE;
    if (t.equals("INT64"))      return new ArrowType.Int(64, true);
    if (t.equals("FLOAT32"))    return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    if (t.equals("FLOAT64"))    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    if (t.equals("DATE"))       return new ArrowType.Date(DateUnit.DAY);
    if (t.equals("TIMESTAMP"))  return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
    if (t.equals("NUMERIC"))    return ArrowType.Utf8.INSTANCE; // stored as decimal string
    if (t.equals("JSON"))       return ArrowType.Utf8.INSTANCE;

    if (t.startsWith("STRING"))  return ArrowType.Utf8.INSTANCE;
    if (t.startsWith("BYTES"))   return ArrowType.Binary.INSTANCE;
    if (t.startsWith("ARRAY"))   return ArrowType.Utf8.INSTANCE; // JSON-serialized
    if (t.startsWith("PROTO"))   return ArrowType.Binary.INSTANCE;

    // Unknown / future type — fall back to string
    return ArrowType.Utf8.INSTANCE;
  }

  /**
   * Returns a short canonical type name used in {@link com.dremio.exec.record.BatchSchema}
   * field metadata (e.g. for display in Dremio UI column details).
   */
  public static String toDisplayType(String spannerType) {
    if (spannerType == null) return "VARCHAR";
    String t = spannerType.trim().toUpperCase();
    if (t.equals("BOOL"))      return "BOOLEAN";
    if (t.equals("INT64"))     return "BIGINT";
    if (t.equals("FLOAT32"))   return "FLOAT";
    if (t.equals("FLOAT64"))   return "DOUBLE";
    if (t.equals("DATE"))      return "DATE";
    if (t.equals("TIMESTAMP")) return "TIMESTAMP";
    if (t.equals("NUMERIC"))   return "DECIMAL";
    if (t.equals("JSON"))      return "VARCHAR";
    if (t.startsWith("STRING"))  return "VARCHAR";
    if (t.startsWith("BYTES"))   return "VARBINARY";
    if (t.startsWith("ARRAY"))   return "VARCHAR";
    if (t.startsWith("PROTO"))   return "VARBINARY";
    return "VARCHAR";
  }
}
