package com.dremio.community.excel.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColumnNormalizer {

    /**
     * Normalize a list of raw header names into clean, unique SQL-safe column names.
     * Rules:
     *   - trim whitespace
     *   - lowercase
     *   - replace spaces and special chars with underscores
     *   - collapse multiple underscores
     *   - prefix numeric-starting names with "col_"
     *   - deduplicate by appending _2, _3, etc.
     */
    public static List<String> normalize(List<String> rawHeaders) {
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (String raw : rawHeaders) {
            String normalized = normalizeSingle(raw == null ? "" : raw);
            if (normalized.isEmpty()) normalized = "col";

            String candidate = normalized;
            int suffix = 2;
            while (seen.contains(candidate)) {
                candidate = normalized + "_" + suffix++;
            }
            seen.add(candidate);
            result.add(candidate);
        }
        return result;
    }

    private static String normalizeSingle(String raw) {
        String s = raw.trim().toLowerCase();
        // Replace any non-alphanumeric character (except underscore) with underscore
        s = s.replaceAll("[^a-z0-9_]", "_");
        // Collapse consecutive underscores
        s = s.replaceAll("_+", "_");
        // Strip leading/trailing underscores
        s = s.replaceAll("^_+|_+$", "");
        // Prefix if starts with a digit
        if (!s.isEmpty() && Character.isDigit(s.charAt(0))) {
            s = "col_" + s;
        }
        return s;
    }
}
