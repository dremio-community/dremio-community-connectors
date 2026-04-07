package com.dremio.community.excel;

import com.dremio.community.excel.model.ColumnDef;
import com.dremio.community.excel.model.ColumnType;
import com.dremio.community.excel.model.InferredSchema;
import com.dremio.community.excel.sql.SqlGenerator;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlGeneratorTest {

    private static InferredSchema schema() {
        return new InferredSchema(List.of(
                new ColumnDef("Order ID", "order_id", ColumnType.BIGINT),
                new ColumnDef("Amount",   "amount",   ColumnType.DOUBLE),
                new ColumnDef("Shipped",  "shipped",  ColumnType.BOOLEAN),
                new ColumnDef("Order Date","order_date", ColumnType.DATE),
                new ColumnDef("Notes",    "notes",    ColumnType.VARCHAR)
        ));
    }

    @Test
    void generateCreateTable() {
        String sql = SqlGenerator.generateCreateTable("Samples.my_table", schema());
        assertTrue(sql.contains("CREATE TABLE \"Samples\".\"my_table\""));
        assertTrue(sql.contains("\"order_id\" BIGINT"));
        assertTrue(sql.contains("\"amount\" DOUBLE"));
        assertTrue(sql.contains("\"shipped\" BOOLEAN"));
        assertTrue(sql.contains("\"order_date\" DATE"));
        assertTrue(sql.contains("\"notes\" VARCHAR"));
    }

    @Test
    void generateCreateTableThreePart() {
        String sql = SqlGenerator.generateCreateTable("catalog.schema.table", schema());
        assertTrue(sql.contains("\"catalog\".\"schema\".\"table\""));
    }

    @Test
    void generateDropTable() {
        String sql = SqlGenerator.generateDropTable("Samples.my_table");
        assertEquals("DROP TABLE IF EXISTS \"Samples\".\"my_table\"", sql);
    }

    @Test
    void generateInsert() {
        List<Object[]> rows = List.of(
                new Object[]{1001L, 12.99, true, LocalDate.of(2026, 1, 15), "Rush order"},
                new Object[]{1002L, 8.50,  false, LocalDate.of(2026, 1, 16), null}
        );
        String sql = SqlGenerator.generateInsert("Samples.my_table", schema(), rows);
        assertTrue(sql.contains("INSERT INTO \"Samples\".\"my_table\""));
        assertTrue(sql.contains("1001"));
        assertTrue(sql.contains("12.99"));
        assertTrue(sql.contains("TRUE"));
        assertTrue(sql.contains("DATE '2026-01-15'"));
        assertTrue(sql.contains("'Rush order'"));
        assertTrue(sql.contains("NULL"));  // null notes on row 2
    }

    @Test
    void formatValueNull() {
        assertEquals("NULL", SqlGenerator.formatValue(null, ColumnType.VARCHAR));
        assertEquals("NULL", SqlGenerator.formatValue(null, ColumnType.BIGINT));
    }

    @Test
    void formatValueBigint() {
        assertEquals("42", SqlGenerator.formatValue(42L, ColumnType.BIGINT));
        assertEquals("-7", SqlGenerator.formatValue(-7L, ColumnType.BIGINT));
        // Double whole number coerced to long
        assertEquals("100", SqlGenerator.formatValue(100.0, ColumnType.BIGINT));
    }

    @Test
    void formatValueDouble() {
        assertEquals("3.14", SqlGenerator.formatValue(3.14, ColumnType.DOUBLE));
        assertEquals("100.0", SqlGenerator.formatValue(100L, ColumnType.DOUBLE));
    }

    @Test
    void formatValueBoolean() {
        assertEquals("TRUE",  SqlGenerator.formatValue(true,  ColumnType.BOOLEAN));
        assertEquals("FALSE", SqlGenerator.formatValue(false, ColumnType.BOOLEAN));
    }

    @Test
    void formatValueDate() {
        assertEquals("DATE '2026-04-06'",
                SqlGenerator.formatValue(LocalDate.of(2026, 4, 6), ColumnType.DATE));
    }

    @Test
    void formatValueTimestamp() {
        assertEquals("TIMESTAMP '2026-04-06 14:30:00'",
                SqlGenerator.formatValue(LocalDateTime.of(2026, 4, 6, 14, 30, 0), ColumnType.TIMESTAMP));
    }

    @Test
    void formatValueVarcharEscapesSingleQuotes() {
        assertEquals("'it''s fine'",
                SqlGenerator.formatValue("it's fine", ColumnType.VARCHAR));
    }

    @Test
    void quotePathSinglePart() {
        assertEquals("\"Samples\"", SqlGenerator.quotePath("Samples"));
    }

    @Test
    void quotePathTwoPart() {
        assertEquals("\"Samples\".\"my_table\"", SqlGenerator.quotePath("Samples.my_table"));
    }

    @Test
    void quotePathThreePart() {
        assertEquals("\"catalog\".\"schema\".\"table\"",
                SqlGenerator.quotePath("catalog.schema.table"));
    }
}
