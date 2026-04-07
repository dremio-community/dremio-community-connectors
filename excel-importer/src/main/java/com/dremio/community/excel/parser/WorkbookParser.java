package com.dremio.community.excel.parser;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Reads .xlsx or .csv files and returns typed headers and data rows.
 *
 * XLSX cell values are returned as typed Java objects (Long, Double, Boolean,
 * LocalDate, LocalDateTime, String, null).
 *
 * CSV cell values are parsed from strings with best-effort type coercion
 * (Long → Double → Boolean → LocalDate → LocalDateTime → String).
 */
public class WorkbookParser implements AutoCloseable {

    // XLSX mode
    private final Workbook workbook;
    private final FormulaEvaluator evaluator;

    // CSV mode
    private final boolean isCsv;
    private final String csvSheetName;
    private final List<String[]> csvRawRows; // all rows including header

    // Date formats tried during CSV type coercion
    private static final List<DateTimeFormatter> DATE_FMTS = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy"),
        DateTimeFormatter.ofPattern("dd/MM/yyyy"),
        DateTimeFormatter.ofPattern("M/d/yyyy"),
        DateTimeFormatter.ofPattern("d/M/yyyy")
    );
    private static final List<DateTimeFormatter> DATETIME_FMTS = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
        DateTimeFormatter.ofPattern("M/d/yyyy H:mm:ss")
    );

    public WorkbookParser(String filePath) throws IOException {
        if (filePath.toLowerCase().endsWith(".csv")) {
            this.isCsv = true;
            this.csvSheetName = Paths.get(filePath).getFileName().toString()
                    .replaceAll("(?i)\\.csv$", "");
            this.csvRawRows = parseCsvFile(filePath);
            this.workbook = null;
            this.evaluator = null;
        } else {
            this.isCsv = false;
            this.csvSheetName = null;
            this.csvRawRows = null;
            FileInputStream fis = new FileInputStream(filePath);
            this.workbook = new XSSFWorkbook(fis);
            this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        }
    }

    /** Returns all sheet names in the workbook (CSV: single sheet = filename). */
    public List<String> getSheetNames() {
        if (isCsv) {
            List<String> names = new ArrayList<>();
            names.add(csvSheetName);
            return names;
        }
        List<String> names = new ArrayList<>();
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            names.add(workbook.getSheetName(i));
        }
        return names;
    }

    /** Returns raw string headers from the specified row. */
    public List<String> getRawHeaders(String sheetName, int headerRowIndex) {
        if (isCsv) {
            List<String> headers = new ArrayList<>();
            if (csvRawRows.isEmpty() || headerRowIndex >= csvRawRows.size()) return headers;
            String[] row = csvRawRows.get(headerRowIndex);
            for (int c = 0; c < row.length; c++) {
                String val = row[c].trim();
                headers.add(val.isEmpty() ? "col_" + (c + 1) : val);
            }
            return headers;
        }
        Sheet sheet = getSheet(sheetName);
        Row row = sheet.getRow(headerRowIndex);
        List<String> headers = new ArrayList<>();
        if (row == null) return headers;
        int lastCol = row.getLastCellNum();
        for (int c = 0; c < lastCol; c++) {
            Cell cell = row.getCell(c, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            if (cell == null) {
                headers.add("col_" + (c + 1));
            } else {
                String val = cell.toString().trim();
                headers.add(val.isEmpty() ? "col_" + (c + 1) : val);
            }
        }
        return headers;
    }

    /** Returns the number of data rows from dataStartRow onwards. */
    public int getDataRowCount(String sheetName, int dataStartRow) {
        if (isCsv) {
            return Math.max(0, csvRawRows.size() - dataStartRow);
        }
        Sheet sheet = getSheet(sheetName);
        int last = sheet.getLastRowNum();
        return Math.max(0, last - dataStartRow + 1);
    }

    /**
     * Reads all data rows from dataStartRow onwards.
     * Each row is an Object[] aligned to columnCount.
     */
    public List<Object[]> readRows(String sheetName, int dataStartRow, int columnCount, boolean skipEmptyRows) {
        if (isCsv) {
            List<Object[]> rows = new ArrayList<>();
            for (int r = dataStartRow; r < csvRawRows.size(); r++) {
                String[] raw = csvRawRows.get(r);
                Object[] values = new Object[columnCount];
                for (int c = 0; c < columnCount; c++) {
                    String s = c < raw.length ? raw[c].trim() : "";
                    values[c] = s.isEmpty() ? null : coerceCsvValue(s);
                }
                if (skipEmptyRows && isAllNull(values)) continue;
                rows.add(values);
            }
            return rows;
        }
        Sheet sheet = getSheet(sheetName);
        List<Object[]> rows = new ArrayList<>();
        int lastRow = sheet.getLastRowNum();
        for (int r = dataStartRow; r <= lastRow; r++) {
            Row row = sheet.getRow(r);
            Object[] values = readRow(row, columnCount);
            if (skipEmptyRows && isAllNull(values)) continue;
            rows.add(values);
        }
        return rows;
    }

    // ── CSV helpers ────────────────────────────────────────────────────────────

    /** Parse CSV file into a list of string arrays (one per row). */
    private static List<String[]> parseCsvFile(String filePath) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            // Auto-detect delimiter from first line
            String firstLine = reader.readLine();
            if (firstLine == null) return rows;
            char delim = detectDelimiter(firstLine);
            rows.add(parseCsvLine(firstLine, delim));
            String line;
            while ((line = reader.readLine()) != null) {
                rows.add(parseCsvLine(line, delim));
            }
        }
        return rows;
    }

    private static char detectDelimiter(String line) {
        int commas = countChar(line, ',');
        int tabs   = countChar(line, '\t');
        int semis  = countChar(line, ';');
        if (tabs >= commas && tabs >= semis) return '\t';
        if (semis > commas) return ';';
        return ',';
    }

    private static int countChar(String s, char c) {
        int n = 0;
        boolean inQuotes = false;
        for (char ch : s.toCharArray()) {
            if (ch == '"') inQuotes = !inQuotes;
            else if (ch == c && !inQuotes) n++;
        }
        return n;
    }

    /** RFC 4180-compliant CSV line parser. */
    private static String[] parseCsvLine(String line, char delim) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"'); i++; // escaped quote
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == delim) {
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(sb.toString());
        return fields.toArray(new String[0]);
    }

    /** Try to coerce a CSV string value to its most specific Java type. */
    private static Object coerceCsvValue(String s) {
        // Long
        try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
        // Double
        try { return Double.parseDouble(s); } catch (NumberFormatException ignored) {}
        // Boolean
        if (s.equalsIgnoreCase("true"))  return Boolean.TRUE;
        if (s.equalsIgnoreCase("false")) return Boolean.FALSE;
        // LocalDate
        for (DateTimeFormatter fmt : DATE_FMTS) {
            try { return LocalDate.parse(s, fmt); } catch (DateTimeParseException ignored) {}
        }
        // LocalDateTime
        for (DateTimeFormatter fmt : DATETIME_FMTS) {
            try { return LocalDateTime.parse(s, fmt); } catch (DateTimeParseException ignored) {}
        }
        return s;
    }

    // ── XLSX helpers ───────────────────────────────────────────────────────────

    private Object[] readRow(Row row, int columnCount) {
        Object[] values = new Object[columnCount];
        if (row == null) return values;
        for (int c = 0; c < columnCount; c++) {
            Cell cell = row.getCell(c, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            values[c] = readCellValue(cell);
        }
        return values;
    }

    private Object readCellValue(Cell cell) {
        if (cell == null) return null;
        CellType type = cell.getCellType();
        if (type == CellType.FORMULA) {
            try {
                type = evaluator.evaluateFormulaCell(cell);
            } catch (Exception e) {
                return null;
            }
        }
        switch (type) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return toTemporalValue(cell.getDateCellValue());
                } else {
                    double d = cell.getNumericCellValue();
                    if (d == Math.floor(d) && !Double.isInfinite(d)
                            && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                        return (long) d;
                    }
                    return d;
                }
            case STRING:
                String s = cell.getStringCellValue().trim();
                return s.isEmpty() ? null : s;
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case BLANK:
            case ERROR:
            default:
                return null;
        }
    }

    private Object toTemporalValue(Date date) {
        if (date == null) return null;
        LocalDateTime ldt = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0) {
            return ldt.toLocalDate();
        }
        return ldt;
    }

    private boolean isAllNull(Object[] row) {
        for (Object v : row) { if (v != null) return false; }
        return true;
    }

    private Sheet getSheet(String sheetName) {
        Sheet sheet = workbook.getSheet(sheetName);
        if (sheet == null) {
            throw new IllegalArgumentException(
                "Sheet not found: '" + sheetName + "'. Available: " + getSheetNames());
        }
        return sheet;
    }

    @Override
    public void close() throws IOException {
        if (workbook != null) workbook.close();
    }
}
