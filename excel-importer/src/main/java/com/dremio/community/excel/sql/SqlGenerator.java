package com.dremio.community.excel.sql;

import com.dremio.community.excel.model.ColumnDef;
import com.dremio.community.excel.model.ColumnType;
import com.dremio.community.excel.model.InferredSchema;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates DDL and DML SQL for Dremio (Iceberg target).
 */
public class SqlGenerator {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Generates: CREATE TABLE "a"."b"."c" (col1 TYPE, col2 TYPE, ...) */
    public static String generateCreateTable(String destPath, InferredSchema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quotePath(destPath)).append(" (\n");
        List<ColumnDef> cols = schema.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            ColumnDef col = cols.get(i);
            sb.append("  \"").append(escape(col.getName())).append("\" ")
              .append(col.getType().toDremioSql());
            if (i < cols.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    /** Generates: DROP TABLE IF EXISTS "a"."b"."c" */
    public static String generateDropTable(String destPath) {
        return "DROP TABLE IF EXISTS " + quotePath(destPath);
    }

    /**
     * Generates a batched INSERT using a VALUES-based SELECT:
     *
     *   INSERT INTO "a"."b"."c"
     *   SELECT * FROM (VALUES
     *     (v1, v2, v3),
     *     ...
     *   ) AS t("col1", "col2", "col3")
     */
    public static String generateInsert(String destPath, InferredSchema schema, List<Object[]> rows) {
        List<ColumnDef> cols = schema.getColumns();
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quotePath(destPath)).append("\n");
        sb.append("SELECT * FROM (VALUES\n");

        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            sb.append("  (");
            for (int c = 0; c < cols.size(); c++) {
                if (c > 0) sb.append(", ");
                Object value = c < row.length ? row[c] : null;
                sb.append(formatValue(value, cols.get(c).getType()));
            }
            sb.append(")");
            if (r < rows.size() - 1) sb.append(",");
            sb.append("\n");
        }

        sb.append(") AS t(");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escape(cols.get(i).getName())).append("\"");
        }
        sb.append(")");
        return sb.toString();
    }

    /** Quote a dot-separated destination path: Samples.folder.table → "Samples"."folder"."table" */
    public static String quotePath(String path) {
        return Arrays.stream(path.split("\\."))
                .map(p -> "\"" + escape(p) + "\"")
                .collect(Collectors.joining("."));
    }

    public static String formatValue(Object value, ColumnType type) {
        if (value == null) return "NULL";

        switch (type) {
            case BIGINT:
                if (value instanceof Long) return value.toString();
                if (value instanceof Double) return String.valueOf(((Double) value).longValue());
                return "NULL";

            case DOUBLE:
                if (value instanceof Double) {
                    double d = (Double) value;
                    return Double.isInfinite(d) || Double.isNaN(d) ? "NULL" : String.valueOf(d);
                }
                if (value instanceof Long) return ((Long) value) + ".0";
                return "NULL";

            case BOOLEAN:
                if (value instanceof Boolean) return ((Boolean) value) ? "TRUE" : "FALSE";
                return "NULL";

            case DATE:
                if (value instanceof LocalDate)
                    return "DATE '" + ((LocalDate) value).format(DATE_FMT) + "'";
                if (value instanceof LocalDateTime)
                    return "DATE '" + ((LocalDateTime) value).toLocalDate().format(DATE_FMT) + "'";
                return "NULL";

            case TIMESTAMP:
                if (value instanceof LocalDateTime)
                    return "TIMESTAMP '" + ((LocalDateTime) value).format(TS_FMT) + "'";
                if (value instanceof LocalDate)
                    return "TIMESTAMP '" + ((LocalDate) value).atStartOfDay().format(TS_FMT) + "'";
                return "NULL";

            case VARCHAR:
            default:
                // Escape single quotes; wrap in single quotes
                return "'" + value.toString().replace("'", "''") + "'";
        }
    }

    private static String escape(String name) {
        return name.replace("\"", "\"\"");
    }
}
