package com.dremio.community.excel;

import com.dremio.community.excel.util.ColumnNormalizer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ColumnNormalizerTest {

    private List<String> normalize(String... names) {
        return ColumnNormalizer.normalize(Arrays.asList(names));
    }

    @Test
    void basicLowercase() {
        assertEquals("order_id", normalize("Order ID").get(0));
    }

    @Test
    void spacesToUnderscore() {
        assertEquals("first_name", normalize("First Name").get(0));
    }

    @Test
    void specialCharsRemoved() {
        assertEquals("amount_usd", normalize("Amount ($USD)").get(0));
    }

    @Test
    void trailingUnderscoreStripped() {
        assertEquals("col", normalize("col_").get(0));
    }

    @Test
    void numericStart() {
        assertEquals("col_1st_quarter", normalize("1st Quarter").get(0));
    }

    @Test
    void emptyBecomesCol() {
        assertEquals("col", normalize("").get(0));
    }

    @Test
    void deduplication() {
        List<String> result = normalize("Name", "Name", "Name");
        assertEquals("name",   result.get(0));
        assertEquals("name_2", result.get(1));
        assertEquals("name_3", result.get(2));
    }

    @Test
    void noChangeAlreadyClean() {
        assertEquals("order_id", normalize("order_id").get(0));
    }

    @Test
    void multipleSpacesCollapsed() {
        assertEquals("a_b", normalize("a   b").get(0));
    }

    @Test
    void mixedHeaders() {
        List<String> result = normalize("Order ID", "Customer Name", "Amount ($)", "Ship Date");
        assertEquals("order_id",      result.get(0));
        assertEquals("customer_name", result.get(1));
        assertEquals("amount",        result.get(2));
        assertEquals("ship_date",     result.get(3));
    }
}
