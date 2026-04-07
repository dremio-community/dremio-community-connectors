package com.dremio.community.excel;

import com.dremio.community.excel.inference.SchemaInferrer;
import com.dremio.community.excel.model.ColumnType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaInferrerTest {

    @Test
    void allLong() {
        assertEquals(ColumnType.BIGINT, infer(1L, 2L, 3L));
    }

    @Test
    void allDouble() {
        assertEquals(ColumnType.DOUBLE, infer(1.5, 2.5, 3.0));
    }

    @Test
    void mixedLongAndDouble() {
        assertEquals(ColumnType.DOUBLE, infer(1L, 2.5, 3L));
    }

    @Test
    void allBoolean() {
        assertEquals(ColumnType.BOOLEAN, infer(true, false, true));
    }

    @Test
    void allDate() {
        assertEquals(ColumnType.DATE,
                infer(LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2)));
    }

    @Test
    void allTimestamp() {
        assertEquals(ColumnType.TIMESTAMP,
                infer(LocalDateTime.of(2026, 1, 1, 10, 30), LocalDateTime.of(2026, 1, 2, 9, 0)));
    }

    @Test
    void mixedDateAndTimestamp() {
        assertEquals(ColumnType.TIMESTAMP,
                infer(LocalDate.of(2026, 1, 1), LocalDateTime.of(2026, 1, 2, 9, 0)));
    }

    @Test
    void anyStringMeansVarchar() {
        assertEquals(ColumnType.VARCHAR, infer(1L, "hello", 3L));
    }

    @Test
    void allNull() {
        assertEquals(ColumnType.VARCHAR, infer(null, null, null));
    }

    @Test
    void allString() {
        assertEquals(ColumnType.VARCHAR, infer("a", "b", "c"));
    }

    @Test
    void nullsWithLongs() {
        assertEquals(ColumnType.BIGINT, infer(null, 1L, null, 2L));
    }

    @Test
    void mixedNumericAndDate() {
        assertEquals(ColumnType.VARCHAR, infer(1L, LocalDate.of(2026, 1, 1)));
    }

    @Test
    void inferFullSchema() {
        List<String> normalized = Arrays.asList("order_id", "amount", "shipped", "order_date", "notes");
        List<String> original   = Arrays.asList("Order ID", "Amount", "Shipped", "Order Date", "Notes");
        List<Object[]> rows = List.of(
                new Object[]{1001L, 12.99, true,  LocalDate.of(2026, 1, 15), "Rush order"},
                new Object[]{1002L, 8.50,  false, LocalDate.of(2026, 1, 16), null},
                new Object[]{1003L, 199.0, true,  LocalDate.of(2026, 1, 18), "Pending"}
        );

        var schema = SchemaInferrer.infer(normalized, original, rows);
        assertEquals(5, schema.size());
        assertEquals(ColumnType.BIGINT,  schema.getColumns().get(0).getType());
        assertEquals(ColumnType.DOUBLE,  schema.getColumns().get(1).getType());
        assertEquals(ColumnType.BOOLEAN, schema.getColumns().get(2).getType());
        assertEquals(ColumnType.DATE,    schema.getColumns().get(3).getType());
        assertEquals(ColumnType.VARCHAR, schema.getColumns().get(4).getType());
    }

    private ColumnType infer(Object... values) {
        return SchemaInferrer.inferType(Arrays.asList(values));
    }
}
