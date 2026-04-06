package com.dremio.plugins.delta.write;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts an Apache Arrow BatchSchema to a Delta Lake StructType.
 *
 * Delta Lake stores the table schema in the transaction log as a StructType.
 * This converter is used when creating a new Delta table (CTAS) or when
 * performing schema evolution (mergeSchema on INSERT INTO with new columns).
 *
 * Type Mapping
 * ------------
 *   Arrow INT8         -> Delta ByteType
 *   Arrow INT16        -> Delta ShortType
 *   Arrow INT32        -> Delta IntegerType
 *   Arrow INT64        -> Delta LongType
 *   Arrow FLOAT        -> Delta FloatType
 *   Arrow DOUBLE       -> Delta DoubleType
 *   Arrow Utf8         -> Delta StringType
 *   Arrow Bool         -> Delta BooleanType
 *   Arrow Binary       -> Delta BinaryType
 *   Arrow Date32       -> Delta DateType
 *   Arrow Timestamp    -> Delta TimestampType
 *   Arrow Decimal      -> Delta DecimalType(precision, scale)
 *   Arrow List         -> Delta ArrayType(elementType, containsNull=true)
 *   Arrow Struct       -> Delta StructType (nested)
 *   Arrow Null/Unknown -> Delta StringType (fallback, with warning)
 *
 * All fields are emitted as nullable (nullable=true) to support schema
 * evolution and partial writes.
 */
public final class ArrowToDeltaSchemaConverter {

  private static final Logger logger = LoggerFactory.getLogger(ArrowToDeltaSchemaConverter.class);

  private ArrowToDeltaSchemaConverter() {}

  /**
   * Converts a list of Arrow Fields to a Delta Lake StructType.
   *
   * Internal connector marker columns (_delta_is_deleted, _delta_upsert)
   * are excluded from the output Delta schema since they are control signals,
   * not real table columns.
   *
   * @param fields  Arrow field definitions (from BatchSchema.getFields())
   * @return Delta StructType suitable for use in a Metadata action
   */
  public static StructType toStructType(List<Field> fields) {
    List<StructField> deltaFields = new ArrayList<>();

    for (Field arrowField : fields) {
      String name = arrowField.getName();

      // Skip internal write-mode marker columns
      if ("_delta_is_deleted".equals(name) || "_delta_upsert".equals(name)) {
        continue;
      }

      DataType deltaType = arrowTypeToDeltaType(arrowField.getType(), arrowField);
      deltaFields.add(new StructField(name, deltaType, /* nullable */ true));
    }

    return new StructType(deltaFields.toArray(new StructField[0]));
  }

  /**
   * Converts a single Arrow type to the corresponding Delta DataType.
   */
  public static DataType arrowTypeToDeltaType(ArrowType arrowType, Field field) {
    switch (arrowType.getTypeID()) {

      case Int: {
        ArrowType.Int intType = (ArrowType.Int) arrowType;
        switch (intType.getBitWidth()) {
          case 8:  return new ByteType();
          case 16: return new ShortType();
          case 32: return new IntegerType();
          case 64: return new LongType();
          default: return new LongType();
        }
      }

      case FloatingPoint: {
        ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
        switch (fpType.getPrecision()) {
          case SINGLE: return new FloatType();
          default:     return new DoubleType();
        }
      }

      case Utf8:
      case LargeUtf8:
        return new StringType();

      case Bool:
        return new BooleanType();

      case Binary:
      case LargeBinary:
      case FixedSizeBinary:
        return new BinaryType();

      case Date:
        return new DateType();

      case Timestamp:
        return new TimestampType();

      case Decimal: {
        ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
        return new DecimalType(decType.getPrecision(), decType.getScale());
      }

      case List:
      case LargeList: {
        Field elementField = field.getChildren().isEmpty() ? null : field.getChildren().get(0);
        DataType elementType = elementField != null
            ? arrowTypeToDeltaType(elementField.getType(), elementField)
            : new StringType();
        return new ArrayType(elementType, /* containsNull */ true);
      }

      case Struct: {
        List<StructField> nestedFields = new ArrayList<>();
        for (Field child : field.getChildren()) {
          DataType childType = arrowTypeToDeltaType(child.getType(), child);
          nestedFields.add(new StructField(child.getName(), childType, true));
        }
        return new StructType(nestedFields.toArray(new StructField[0]));
      }

      case Null:
        return new StringType();

      default:
        logger.warn("Unsupported Arrow type {} for field '{}'. Mapping to Delta StringType.",
            arrowType.getTypeID(), field.getName());
        return new StringType();
    }
  }

  /**
   * Returns true if the Delta schema contains all fields present in the Arrow
   * schema (excluding internal marker columns). Used to decide whether schema
   * evolution is needed before writing.
   */
  public static boolean isSchemaCompatible(List<Field> arrowFields, StructType deltaSchema) {
    for (Field arrowField : arrowFields) {
      String name = arrowField.getName();
      if ("_delta_is_deleted".equals(name) || "_delta_upsert".equals(name)) continue;
      try {
        deltaSchema.get(name);
      } catch (IllegalArgumentException e) {
        return false; // field missing from Delta schema
      }
    }
    return true;
  }
}
