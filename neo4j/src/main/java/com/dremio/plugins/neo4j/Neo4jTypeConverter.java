package com.dremio.plugins.neo4j;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts a Neo4j inferred schema (Map of property name to ArrowType)
 * into a list of Arrow Fields suitable for BatchSchema construction.
 *
 * All fields are nullable.
 */
public class Neo4jTypeConverter {

  private Neo4jTypeConverter() {}

  /**
   * Converts the schema map (already sorted) into an Arrow Field list.
   */
  public static List<Field> toFields(Map<String, ArrowType> schema) {
    List<Field> fields = new ArrayList<>(schema.size());
    for (Map.Entry<String, ArrowType> entry : schema.entrySet()) {
      fields.add(Field.nullable(entry.getKey(), entry.getValue()));
    }
    return fields;
  }
}
