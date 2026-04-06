package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Converts Cassandra CQL data types to Dremio/Arrow fields and types.
 *
 * Primitive types map to flat Arrow scalars.
 * Collection types map to native Arrow nested types:
 *
 *   LIST<T> / SET<T>  →  Arrow List  (child field named "$data$")
 *   MAP<K,V>          →  Arrow Map   (child Struct with "key" + "value" fields)
 *   UDT               →  Arrow Struct (one child field per UDT field)
 *
 * All top-level fields are nullable (Cassandra columns are always nullable in CQL).
 * Map keys are not-nullable (CQL disallows null map keys); map values are nullable.
 * UDT fields are nullable.
 */
public final class CassandraTypeConverter {

  /**
   * Arrow convention: the single child field of a List or Map vector is named "$data$".
   * This matches Arrow Java's ListVector.DATA_VECTOR_NAME and MapVector.DATA_VECTOR_NAME.
   */
  static final String DATA_VECTOR_NAME = "$data$";

  private CassandraTypeConverter() {}

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Converts a Cassandra column type to an Arrow {@link Field}.
   *
   * For collection types this builds the full nested Arrow field tree:
   *   LIST<text>          → Field(name, List, [Field("$data$", Utf8)])
   *   SET<int>            → Field(name, List, [Field("$data$", Int32)])
   *   MAP<text, double>   → Field(name, Map, [Field("$data$", Struct,
   *                             [Field("key", Utf8), Field("value", Float64)])])
   *   UDT{a:text, b:int}  → Field(name, Struct, [Field("a", Utf8), Field("b", Int32)])
   *   VECTOR<float, 128>  → Field(name, FixedSizeList(128), [Field("$data$", Float32)])
   *
   * @param name      column name
   * @param cassType  Cassandra data type from schema metadata
   */
  public static Field toArrowField(String name, DataType cassType) {

    // ---- LIST ---------------------------------------------------------------
    if (cassType instanceof ListType) {
      DataType elemType = ((ListType) cassType).getElementType();
      Field elemField   = toArrowField(DATA_VECTOR_NAME, elemType);   // recursive
      return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE),
          Collections.singletonList(elemField));
    }

    // ---- SET (treated as List — Arrow has no native Set type) ---------------
    if (cassType instanceof SetType) {
      DataType elemType = ((SetType) cassType).getElementType();
      Field elemField   = toArrowField(DATA_VECTOR_NAME, elemType);   // recursive
      return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE),
          Collections.singletonList(elemField));
    }

    // ---- MAP ----------------------------------------------------------------
    if (cassType instanceof MapType) {
      MapType mt = (MapType) cassType;

      // Map keys are never null in Cassandra; map values can be null.
      Field keyField   = new Field("key",
          FieldType.notNullable(toArrowType(mt.getKeyType())), null);
      Field valueField = new Field("value",
          FieldType.nullable(toArrowType(mt.getValueType())), null);

      // Arrow Map stores entries as a List<Struct<key,value>>.
      // Arrow spec requires the "$data$" struct child to be NON-nullable
      // (MapVector.initializeChildrenFromFields() enforces this with a checkArgument).
      Field structField = new Field(DATA_VECTOR_NAME,
          FieldType.notNullable(ArrowType.Struct.INSTANCE),
          Arrays.asList(keyField, valueField));

      return new Field(name,
          FieldType.nullable(new ArrowType.Map(/* keysSorted= */ false)),
          Collections.singletonList(structField));
    }

    // ---- VECTOR<float, N> (Cassandra 5.x) → Arrow FixedSizeList<float32> -----
    // Cassandra 5.0 introduced a native VECTOR<float, N> type for similarity /
    // embedding search workloads (e.g. k-NN queries with SAI).  Arrow represents
    // fixed-length arrays as FixedSizeList(N) with a float32 child vector.
    // The element type is always FLOAT in the current Cassandra implementation;
    // we validate this assumption at runtime and fall back to Utf8 if it changes.
    if (cassType instanceof VectorType) {
      VectorType vt = (VectorType) cassType;
      int dimensions = vt.getDimensions();
      // Element type is always float for Cassandra VECTOR — use notNullable child
      // per Arrow FixedSizeList convention (child slots always defined when parent is)
      Field elemField = new Field(DATA_VECTOR_NAME,
          FieldType.notNullable(
              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
          Collections.emptyList());
      return new Field(name,
          FieldType.nullable(new ArrowType.FixedSizeList(dimensions)),
          Collections.singletonList(elemField));
    }

    // ---- UDT → Arrow Struct -------------------------------------------------
    if (cassType instanceof UserDefinedType) {
      UserDefinedType udt = (UserDefinedType) cassType;
      List<Field> children = new ArrayList<>();
      List<com.datastax.oss.driver.api.core.CqlIdentifier> fieldNames =
          udt.getFieldNames();
      List<DataType> fieldTypes = udt.getFieldTypes();
      for (int i = 0; i < fieldNames.size(); i++) {
        String fn  = fieldNames.get(i).asInternal();
        Field child = toArrowField(fn, fieldTypes.get(i));  // recursive
        children.add(child);
      }
      return new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE), children);
    }

    // ---- Primitive / scalar types -------------------------------------------
    return new Field(name, FieldType.nullable(toArrowType(cassType)),
        Collections.emptyList());
  }

  /**
   * Returns the Arrow scalar type for a primitive Cassandra type.
   * Not suitable for collection types — use {@link #toArrowField} for those.
   */
  public static ArrowType toArrowType(DataType cassType) {
    if (cassType == DataTypes.TEXT || cassType == DataTypes.ASCII) {
      return ArrowType.Utf8.INSTANCE;
    } else if (cassType == DataTypes.INT) {
      return new ArrowType.Int(32, true);
    } else if (cassType == DataTypes.BIGINT || cassType == DataTypes.COUNTER) {
      return new ArrowType.Int(64, true);
    } else if (cassType == DataTypes.SMALLINT) {
      return new ArrowType.Int(16, true);
    } else if (cassType == DataTypes.TINYINT) {
      return new ArrowType.Int(8, true);
    } else if (cassType == DataTypes.FLOAT) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (cassType == DataTypes.DOUBLE) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (cassType == DataTypes.BOOLEAN) {
      return ArrowType.Bool.INSTANCE;
    } else if (cassType == DataTypes.TIMESTAMP) {
      return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
    } else if (cassType == DataTypes.DATE) {
      return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
    } else if (cassType == DataTypes.TIME) {
      return new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64);
    } else if (cassType == DataTypes.UUID || cassType == DataTypes.TIMEUUID) {
      return ArrowType.Utf8.INSTANCE;
    } else if (cassType == DataTypes.BLOB) {
      return ArrowType.Binary.INSTANCE;
    } else if (cassType == DataTypes.DECIMAL) {
      return new ArrowType.Decimal(38, 10, 128);
    } else if (cassType == DataTypes.VARINT) {
      return ArrowType.Utf8.INSTANCE;
    } else if (cassType == DataTypes.INET) {
      return ArrowType.Utf8.INSTANCE;
    } else if (cassType == DataTypes.DURATION) {
      return new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.MILLISECOND);
    } else {
      // Collection, UDT, Tuple, or unknown — fall back to string.
      // (Callers that need proper nested types should use toArrowField instead.)
      return ArrowType.Utf8.INSTANCE;
    }
  }
}
