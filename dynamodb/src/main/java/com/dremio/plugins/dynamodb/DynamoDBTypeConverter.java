package com.dremio.plugins.dynamodb;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Infers an Arrow schema from a sample of DynamoDB items.
 *
 * DynamoDB is schemaless — each item can have different attributes.
 * This class scans a sample of items and builds a BatchSchema by:
 *   1. Collecting all attribute names across all sampled items.
 *   2. For each attribute, determining the Arrow type from the DynamoDB type.
 *   3. If an attribute appears with conflicting types (e.g., S in one item,
 *      N in another), the attribute is typed as VarChar (most general).
 *
 * DynamoDB → Arrow type mapping:
 *   S  (String)         → VarChar
 *   N  (Number)         → BigInt (INT64) when all sampled values are integers;
 *                          Float8 (DOUBLE) otherwise
 *   B  (Binary)         → VarBinary
 *   BOOL (Boolean)      → Bit
 *   NULL                → VarChar (nullable, always null)
 *   L  (List)           → VarChar (JSON-serialized; element types vary per item)
 *   M  (Map)            → VarChar (JSON-serialized; keys/types vary per item)
 *   SS (String Set)     → List<VarChar>  (native Arrow list of strings)
 *   NS (Number Set)     → List<Float8>   (native Arrow list of doubles)
 *   BS (Binary Set)     → VarChar        (base64-encoded JSON; rare in practice)
 */
public class DynamoDBTypeConverter {

  /** Sentinel used internally when an attribute has been seen with conflicting types. */
  public static final String TYPE_CONFLICT = "CONFLICT";

  private DynamoDBTypeConverter() {}

  /**
   * Infers the DynamoDB type tag of an AttributeValue: "S", "N", "B", "BOOL",
   * "NULL", "L", "M", "SS", "NS", "BS", or "UNKNOWN".
   */
  public static String dynType(AttributeValue av) {
    if (av == null)                                        return "NULL";
    if (av.s() != null)                                    return "S";
    if (av.n() != null)                                    return "N";
    if (av.b() != null)                                    return "B";
    if (av.bool() != null)                                 return "BOOL";
    if (av.nul() != null && av.nul())                      return "NULL";
    if (av.hasL() && av.l() != null)                       return "L";
    if (av.hasM() && av.m() != null)                       return "M";
    if (av.hasSs() && av.ss() != null)                     return "SS";
    if (av.hasNs() && av.ns() != null)                     return "NS";
    if (av.hasBs() && av.bs() != null)                     return "BS";
    return "UNKNOWN";
  }

  /**
   * Maps a DynamoDB type tag to the canonical Arrow type for scalar fields.
   * For collection types (SS, NS) that need child fields, use {@link #buildField} instead.
   */
  public static ArrowType arrowTypeFor(String dynType) {
    switch (dynType) {
      case "N":    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "B":    return ArrowType.Binary.INSTANCE;
      case "BOOL": return ArrowType.Bool.INSTANCE;
      case "S":
      case "L":
      case "M":
      case "SS":
      case "NS":
      case "BS":
      case "NULL":
      case TYPE_CONFLICT:
      default:     return ArrowType.Utf8.INSTANCE;
    }
  }

  /**
   * Builds a complete Arrow Field for the given attribute name and DynamoDB type.
   *
   * For SS (String Set) and NS (Number Set), returns a List field with the
   * appropriate child type.  For integer-typed N columns, returns BigInt.
   * All other types use the standard arrowTypeFor() mapping.
   *
   * @param attrName  DynamoDB attribute name
   * @param dynType   DynamoDB type tag ("S", "N", "SS", "NS", …)
   * @param allInt    true when all sampled N values for this attribute were integers
   */
  static Field buildField(String attrName, String dynType, boolean allInt) {
    switch (dynType) {
      case "SS": {
        // String Set → List<VarChar>
        Field child = new Field("$data$",
            FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        return new Field(attrName,
            FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(child));
      }
      case "NS": {
        // Number Set → List<Float8>
        Field child = new Field("$data$",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        return new Field(attrName,
            FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(child));
      }
      case "N": {
        // Use BigInt when ALL sampled values for this column are integers
        ArrowType type = allInt
            ? new ArrowType.Int(64, true)
            : new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        return new Field(attrName, FieldType.nullable(type), null);
      }
      default:
        return new Field(attrName, FieldType.nullable(arrowTypeFor(dynType)), null);
    }
  }

  /**
   * Returns true if the number string looks like a plain integer
   * (no decimal point, no exponent notation).
   */
  static boolean looksLikeInteger(String n) {
    if (n == null || n.isEmpty()) return false;
    int start = (n.charAt(0) == '-') ? 1 : 0;
    if (start >= n.length()) return false;
    for (int i = start; i < n.length(); i++) {
      char c = n.charAt(i);
      if (c < '0' || c > '9') return false;
    }
    return true;
  }

  /**
   * Infers an ordered map of {attributeName → Arrow Field} from a list of sampled items.
   *
   * Attribute order follows first-seen order across all items. When the same
   * attribute appears with different DynamoDB types in different items, it is
   * typed as VarChar (safest common type).
   *
   * For N attributes, detects whether all sampled values are integers and
   * uses BigInt instead of Float8 when that is the case.
   *
   * @param samples  sampled DynamoDB items (may be empty → returns empty list)
   * @return ordered list of Arrow Fields representing the inferred schema
   */
  public static List<Field> inferFields(List<Map<String, AttributeValue>> samples) {
    // attribute name → observed DynamoDB type tag (or TYPE_CONFLICT)
    Map<String, String> typeMap = new LinkedHashMap<>();
    // attribute name → true iff all sampled N values were integers
    Map<String, Boolean> integerMap = new LinkedHashMap<>();

    for (Map<String, AttributeValue> item : samples) {
      for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
        String attr = entry.getKey();
        AttributeValue av = entry.getValue();
        String observedType = dynType(av);

        if ("NULL".equals(observedType) || "UNKNOWN".equals(observedType)) {
          typeMap.putIfAbsent(attr, observedType);
          continue;
        }

        String existing = typeMap.get(attr);
        if (existing == null || "NULL".equals(existing) || "UNKNOWN".equals(existing)) {
          typeMap.put(attr, observedType);
        } else if (!existing.equals(observedType) && !TYPE_CONFLICT.equals(existing)) {
          typeMap.put(attr, TYPE_CONFLICT);
        }

        // Track integer-only flag for N columns
        if ("N".equals(observedType) && av.n() != null) {
          boolean isInt = looksLikeInteger(av.n());
          integerMap.merge(attr, isInt, Boolean::logicalAnd);
        }
      }
    }

    List<Field> fields = new ArrayList<>(typeMap.size());
    for (Map.Entry<String, String> entry : typeMap.entrySet()) {
      String attrName = entry.getKey();
      String dynT = entry.getValue();
      boolean allInt = Boolean.TRUE.equals(integerMap.get(attrName));
      fields.add(buildField(attrName, dynT, allInt));
    }
    return fields;
  }
}
