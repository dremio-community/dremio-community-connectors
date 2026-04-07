package com.dremio.community.excel.inference;

import com.dremio.community.excel.model.ColumnDef;
import com.dremio.community.excel.model.ColumnType;
import com.dremio.community.excel.model.InferredSchema;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Infers Dremio column types from a sample of parsed rows.
 *
 * Type priority (most specific to most general):
 *   BIGINT → DOUBLE → BOOLEAN → DATE → TIMESTAMP → VARCHAR
 *
 * Conservative rules:
 *   - Any String value in a column → VARCHAR (don't coerce text to numbers)
 *   - Mixed Long + Double → DOUBLE
 *   - Mixed Date + DateTime → TIMESTAMP
 *   - Mixed numeric + temporal → VARCHAR
 *   - All nulls → VARCHAR
 */
public class SchemaInferrer {

    public static InferredSchema infer(List<String> normalizedHeaders,
                                       List<String> originalHeaders,
                                       List<Object[]> sampleRows) {
        int cols = normalizedHeaders.size();
        List<ColumnDef> defs = new ArrayList<>();

        for (int c = 0; c < cols; c++) {
            List<Object> columnValues = new ArrayList<>();
            for (Object[] row : sampleRows) {
                if (c < row.length) columnValues.add(row[c]);
            }
            ColumnType type = inferType(columnValues);
            defs.add(new ColumnDef(originalHeaders.get(c), normalizedHeaders.get(c), type));
        }

        return new InferredSchema(defs);
    }

    public static ColumnType inferType(List<Object> values) {
        boolean hasLong = false;
        boolean hasDouble = false;
        boolean hasBoolean = false;
        boolean hasDate = false;
        boolean hasDateTime = false;
        boolean hasString = false;
        int nonNullCount = 0;

        for (Object v : values) {
            if (v == null) continue;
            nonNullCount++;

            if (v instanceof Long) {
                hasLong = true;
            } else if (v instanceof Double) {
                hasDouble = true;
            } else if (v instanceof Boolean) {
                hasBoolean = true;
            } else if (v instanceof LocalDate) {
                hasDate = true;
            } else if (v instanceof LocalDateTime) {
                hasDateTime = true;
            } else if (v instanceof String) {
                hasString = true;
            }
        }

        // All nulls → VARCHAR
        if (nonNullCount == 0) return ColumnType.VARCHAR;

        // Any string → VARCHAR (don't coerce text)
        if (hasString) return ColumnType.VARCHAR;

        // Mixed temporal+numeric → VARCHAR
        boolean hasTemporal = hasDate || hasDateTime;
        boolean hasNumeric = hasLong || hasDouble;
        if (hasTemporal && hasNumeric) return ColumnType.VARCHAR;
        if (hasTemporal && hasBoolean) return ColumnType.VARCHAR;
        if (hasNumeric && hasBoolean) return ColumnType.VARCHAR;

        // Pure temporal
        if (hasTemporal) {
            return hasDateTime ? ColumnType.TIMESTAMP : ColumnType.DATE;
        }

        // Pure boolean
        if (hasBoolean) return ColumnType.BOOLEAN;

        // Numeric
        if (hasDouble) return ColumnType.DOUBLE;
        if (hasLong) return ColumnType.BIGINT;

        return ColumnType.VARCHAR;
    }
}
