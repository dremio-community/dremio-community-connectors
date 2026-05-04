package com.dremio.plugins.redis;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Infers an Arrow schema from sampled Redis hash data.
 *
 * All Redis values are strings. We attempt to classify each field's content:
 *   - All samples parseable as long integer → BigInt
 *   - All samples parseable as double (and not long) → Float8
 *   - "true"/"false"/"1"/"0" only → Bit (boolean)
 *   - Otherwise → Utf8
 *
 * The special "_id" field (key suffix) is always Utf8.
 */
public class RedisTypeConverter {

  private RedisTypeConverter() {}

  /**
   * Infers Arrow fields from a list of sample hash rows.
   * Fields are sorted alphabetically with "_id" always first.
   */
  public static List<Field> inferFields(List<Map<String, String>> samples) {
    if (samples == null || samples.isEmpty()) {
      List<Field> fallback = new ArrayList<>();
      fallback.add(Field.nullable("_id", ArrowType.Utf8.INSTANCE));
      return fallback;
    }

    // Collect all field names and all observed values per field
    Map<String, List<String>> fieldValues = new LinkedHashMap<>();
    for (Map<String, String> row : samples) {
      for (Map.Entry<String, String> e : row.entrySet()) {
        fieldValues.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                   .add(e.getValue());
      }
    }

    List<Field> fields = new ArrayList<>();
    // _id always first
    fields.add(Field.nullable("_id", ArrowType.Utf8.INSTANCE));
    fieldValues.remove("_id");

    // Sort remaining fields alphabetically for stable schema
    List<String> sortedNames = new ArrayList<>(fieldValues.keySet());
    sortedNames.sort(String::compareToIgnoreCase);

    for (String name : sortedNames) {
      List<String> values = fieldValues.get(name);
      ArrowType type = inferType(values);
      fields.add(Field.nullable(name, type));
    }

    return fields;
  }

  private static ArrowType inferType(List<String> values) {
    if (values == null || values.isEmpty()) return ArrowType.Utf8.INSTANCE;

    boolean allLong    = true;
    boolean allDouble  = true;
    boolean allBool    = true;

    for (String v : values) {
      if (v == null || v.isEmpty()) continue;

      if (allLong) {
        try { Long.parseLong(v); }
        catch (NumberFormatException e) { allLong = false; }
      }
      if (allDouble) {
        try { Double.parseDouble(v); }
        catch (NumberFormatException e) { allDouble = false; }
      }
      if (allBool) {
        String lower = v.toLowerCase();
        if (!lower.equals("true") && !lower.equals("false")
            && !lower.equals("1") && !lower.equals("0")) {
          allBool = false;
        }
      }
    }

    if (allBool && !allLong) return ArrowType.Bool.INSTANCE;
    if (allLong)             return new ArrowType.Int(64, true);
    if (allDouble)           return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    return ArrowType.Utf8.INSTANCE;
  }
}
