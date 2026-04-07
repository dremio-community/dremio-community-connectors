package com.dremio.community.excel;

import com.dremio.community.excel.dremio.DremioClient;
import com.dremio.community.excel.inference.SchemaInferrer;
import com.dremio.community.excel.model.ColumnDef;
import com.dremio.community.excel.model.ColumnType;
import com.dremio.community.excel.model.ImportConfig;
import com.dremio.community.excel.model.InferredSchema;
import com.dremio.community.excel.parser.WorkbookParser;
import com.dremio.community.excel.sql.SqlGenerator;
import com.dremio.community.excel.util.ColumnNormalizer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CLI entry point for the Dremio Excel Importer.
 *
 * Usage:
 *   java -jar dremio-excel-importer.jar \
 *     --file sales.xlsx \
 *     --sheet "Q2 Data" \
 *     --dest "iceberg_minio.dremio-test.sales_q2" \
 *     --host localhost --port 9047 \
 *     --user mark --password critter77
 *
 * Or from a Google Sheet URL:
 *   java -jar dremio-excel-importer.jar \
 *     --url "https://docs.google.com/spreadsheets/d/ID/export?format=xlsx" \
 *     --dest "iceberg_minio.dremio-test.my_table" \
 *     --user mark --password critter77
 */
public class ExcelImporter {

    public static void main(String[] args) throws Exception {
        // Force line-by-line flushing so SSE streaming works when stdout is a pipe
        System.setOut(new java.io.PrintStream(System.out, true));
        System.setErr(new java.io.PrintStream(System.err, true));
        ImportConfig config = parseArgs(args);
        runImport(config);
    }

    /** Main import logic — also called directly by the web UI tests. */
    public static void runImport(ImportConfig config) throws Exception {

        // --- Resolve file path (download from URL if needed) ---
        String filePath = resolveFile(config);

        // --- List sheets and exit ---
        if (config.isListSheets()) {
            try (WorkbookParser parser = new WorkbookParser(filePath)) {
                List<String> sheets = parser.getSheetNames();
                if (config.isJsonOutput()) {
                    System.out.println(toJsonArray(sheets));
                } else {
                    System.out.println("Sheets in " + filePath + ":");
                    sheets.forEach(s -> System.out.println("  - " + s));
                }
            }
            return;
        }

        validateConfig(config);

        if (!config.isJsonOutput()) {
            System.out.println("\nDremio Excel Importer");
            System.out.println("=====================");
            System.out.println("File   : " + filePath);
        }

        try (WorkbookParser parser = new WorkbookParser(filePath)) {

            // --- Resolve sheet ---
            String sheetName = config.getSheetName();
            if (sheetName == null) {
                List<String> sheets = parser.getSheetNames();
                if (sheets.isEmpty()) throw new IllegalArgumentException("Workbook contains no sheets.");
                sheetName = sheets.get(0);
            }
            if (!config.isJsonOutput()) System.out.println("Sheet  : " + sheetName);

            // --- Read headers ---
            List<String> rawHeaders = parser.getRawHeaders(sheetName, config.getHeaderRow());
            if (rawHeaders.isEmpty()) throw new IllegalArgumentException("No headers in row " + config.getHeaderRow());
            List<String> normalizedHeaders = ColumnNormalizer.normalize(rawHeaders);
            int dataStartRow = config.getDataStartRow();

            // --- Read rows ---
            List<Object[]> allRows = parser.readRows(sheetName, dataStartRow,
                    normalizedHeaders.size(), config.isSkipEmptyRows());
            if (!config.isJsonOutput()) {
                System.out.println("Columns: " + rawHeaders.size());
                System.out.println("Rows   : " + allRows.size());
            }

            if (allRows.isEmpty() && !config.isPreview()) {
                System.out.println("No data rows found. Nothing to import.");
                return;
            }

            // --- Infer schema ---
            int sampleSize = Math.min(config.getSampleRows(), Math.max(1, allRows.size()));
            InferredSchema schema = SchemaInferrer.infer(
                    normalizedHeaders, rawHeaders,
                    allRows.isEmpty() ? new ArrayList<>() : allRows.subList(0, sampleSize));

            // --- Apply type overrides ---
            if (config.getTypeOverrides() != null) {
                schema = applyTypeOverrides(schema, config.getTypeOverrides());
            }

            // --- Apply column renames ---
            if (config.getRenameColumns() != null) {
                Map<String, String> renames = new HashMap<>();
                for (String pair : config.getRenameColumns().split(",")) {
                    String[] parts = pair.trim().split("=", 2);
                    if (parts.length == 2) renames.put(parts[0].trim(), parts[1].trim());
                }
                List<ColumnDef> renamed = new ArrayList<>();
                for (ColumnDef col : schema.getColumns()) {
                    String newName = renames.getOrDefault(col.getName(), col.getName());
                    renamed.add(new ColumnDef(col.getOriginalName(), newName, col.getType()));
                }
                schema = new InferredSchema(renamed);
            }

            // --- Apply column exclusions ---
            if (config.getExcludeColumns() != null) {
                Set<String> excluded = Arrays.stream(config.getExcludeColumns().split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
                List<Integer> keepIndices = new ArrayList<>();
                List<ColumnDef> keepCols = new ArrayList<>();
                for (int i = 0; i < schema.getColumns().size(); i++) {
                    if (!excluded.contains(schema.getColumns().get(i).getName())) {
                        keepIndices.add(i);
                        keepCols.add(schema.getColumns().get(i));
                    }
                }
                schema = new InferredSchema(keepCols);
                final List<Integer> ki = keepIndices;
                allRows = allRows.stream().map(row -> {
                    Object[] r = new Object[ki.size()];
                    for (int j = 0; j < ki.size(); j++)
                        r[j] = ki.get(j) < row.length ? row[ki.get(j)] : null;
                    return r;
                }).collect(Collectors.toList());
            }

            // --- Preview mode: print schema and exit ---
            if (config.isPreview()) {
                if (config.isJsonOutput()) {
                    List<Object[]> previewSample = allRows.isEmpty() ? new ArrayList<>()
                            : allRows.subList(0, Math.min(5, allRows.size()));
                    System.out.println(schemaToJson(sheetName, allRows.size(), schema, previewSample));
                } else {
                    printSchema(schema, sampleSize);
                }
                return;
            }

            // --- Print schema (non-JSON mode) ---
            if (!config.isJsonOutput()) {
                printSchema(schema, sampleSize);
            }

            // --- Confirm ---
            if (!config.isYes() && !config.isJsonOutput()) {
                System.out.printf("%nImport %d rows into %s? [y/N] ",
                        allRows.size(), config.getDestPath());
                System.out.flush();
                String answer = new Scanner(System.in).nextLine().trim();
                if (!answer.equalsIgnoreCase("y")) {
                    System.out.println("Aborted.");
                    return;
                }
            }

            // --- Connect to Dremio ---
            DremioClient dremio = new DremioClient(config.getDremioHost(), config.getDremioPort());
            System.out.print("Connecting to Dremio at " +
                    config.getDremioHost() + ":" + config.getDremioPort() + "... ");
            System.out.flush();
            dremio.login(config.getUsername(), config.getPassword());
            System.out.println("OK");

            boolean appendMode = "append".equalsIgnoreCase(config.getMode());

            if (!appendMode) {
                // --- Drop if overwrite ---
                if (config.isOverwrite()) {
                    String dropSql = SqlGenerator.generateDropTable(config.getDestPath());
                    System.out.print("Dropping existing table... ");
                    System.out.flush();
                    dremio.executeAndWait(dropSql);
                    System.out.println("OK");
                }

                // --- Create table ---
                String createSql = SqlGenerator.generateCreateTable(config.getDestPath(), schema);
                System.out.print("Creating table " + config.getDestPath() + "... ");
                System.out.flush();
                dremio.executeAndWait(createSql);
                System.out.println("OK");
            } else {
                System.out.println("Append mode — skipping CREATE TABLE.");
            }

            // --- Insert data in batches ---
            int batchSize = config.getBatchSize();
            int total = allRows.size();
            int batches = (int) Math.ceil((double) total / batchSize);
            System.out.println("Inserting " + total + " rows in " + batches + " batch(es)...");

            for (int b = 0; b < batches; b++) {
                int start = b * batchSize;
                int end = Math.min(start + batchSize, total);
                List<Object[]> batch = allRows.subList(start, end);
                String insertSql = SqlGenerator.generateInsert(config.getDestPath(), schema, batch);
                System.out.printf("  Batch %d/%d (rows %d-%d)... ", b + 1, batches, start + 1, end);
                System.out.flush();
                dremio.executeAndWait(insertSql);
                System.out.println("OK");
            }

            System.out.println("\nImport complete!");
            System.out.println("  Table : " + config.getDestPath());
            System.out.println("  Rows  : " + total);
            System.out.println("  Cols  : " + schema.size());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Download URL to a temp file if --url is set; otherwise return filePath as-is. */
    private static String resolveFile(ImportConfig config) throws Exception {
        if (config.getUrl() != null) {
            File tmp = Files.createTempFile("dremio-excel-import-", ".xlsx").toFile();
            tmp.deleteOnExit();
            System.out.println("Downloading: " + config.getUrl());
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new URL(config.getUrl()).openConnection();
            conn.setInstanceFollowRedirects(true);
            conn.connect();
            int code = conn.getResponseCode();
            if (code == 401 || code == 403) {
                conn.disconnect();
                throw new IllegalArgumentException(
                    "HTTP " + code + " downloading spreadsheet. " +
                    "For Google Sheets: open the sheet, click Share → 'Anyone with the link' → Viewer, then retry.");
            }
            if (code >= 400) {
                conn.disconnect();
                throw new IllegalArgumentException("HTTP " + code + " downloading spreadsheet from URL.");
            }
            try (InputStream in = conn.getInputStream();
                 FileOutputStream out = new FileOutputStream(tmp)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            } finally {
                conn.disconnect();
            }
            System.out.println("Downloaded to: " + tmp.getAbsolutePath());
            return tmp.getAbsolutePath();
        }
        return config.getFilePath();
    }

    /** Apply --types overrides: "col=TYPE,col2=TYPE" */
    private static InferredSchema applyTypeOverrides(InferredSchema schema, String overrides) {
        Map<String, ColumnType> overrideMap = new HashMap<>();
        for (String pair : overrides.split(",")) {
            String[] parts = pair.trim().split("=", 2);
            if (parts.length == 2) {
                try {
                    overrideMap.put(parts[0].trim().toLowerCase(), ColumnType.valueOf(parts[1].trim().toUpperCase()));
                } catch (IllegalArgumentException e) {
                    System.err.println("Warning: unknown type '" + parts[1].trim() + "' for column '" + parts[0].trim() + "' — ignored");
                }
            }
        }

        List<ColumnDef> updated = new ArrayList<>();
        for (ColumnDef col : schema.getColumns()) {
            ColumnType override = overrideMap.get(col.getName().toLowerCase());
            if (override != null) {
                System.out.println("  Type override: " + col.getName() + " -> " + override);
                updated.add(new ColumnDef(col.getOriginalName(), col.getName(), override));
            } else {
                updated.add(col);
            }
        }
        return new InferredSchema(updated);
    }

    private static void printSchema(InferredSchema schema, int sampleSize) {
        System.out.println("\nInferred schema (sampled " + sampleSize + " rows):");
        System.out.printf("  %-35s %-12s  %s%n", "Column", "Type", "Original Name");
        System.out.println("  " + "-".repeat(70));
        for (ColumnDef col : schema.getColumns()) {
            String original = col.getOriginalName().equals(col.getName()) ? "" : col.getOriginalName();
            System.out.printf("  %-35s %-12s  %s%n", col.getName(), col.getType(), original);
        }
        boolean anyRenamed = schema.getColumns().stream()
                .anyMatch(c -> !c.getOriginalName().equals(c.getName()));
        if (anyRenamed) System.out.println("\n  Note: some column names were normalized.");
    }

    // -------------------------------------------------------------------------
    // JSON helpers (minimal — no Jackson needed, just string building)
    // -------------------------------------------------------------------------

    static String toJsonArray(List<String> items) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(jsonEscape(items.get(i))).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    static String schemaToJson(String sheet, int rowCount, InferredSchema schema, List<Object[]> sampleRows) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"sheet\":\"").append(jsonEscape(sheet)).append("\",");
        sb.append("\"rowCount\":").append(rowCount).append(",");
        sb.append("\"columns\":[");
        List<ColumnDef> cols = schema.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(",");
            ColumnDef c = cols.get(i);
            sb.append("{");
            sb.append("\"name\":\"").append(jsonEscape(c.getName())).append("\",");
            sb.append("\"type\":\"").append(c.getType().toDremioSql()).append("\",");
            sb.append("\"originalName\":\"").append(jsonEscape(c.getOriginalName())).append("\"");
            sb.append("}");
        }
        sb.append("],\"sampleRows\":[");
        for (int r = 0; r < sampleRows.size(); r++) {
            if (r > 0) sb.append(",");
            sb.append("[");
            Object[] row = sampleRows.get(r);
            for (int c = 0; c < row.length; c++) {
                if (c > 0) sb.append(",");
                if (row[c] == null) sb.append("null");
                else sb.append("\"").append(jsonEscape(String.valueOf(row[c]))).append("\"");
            }
            sb.append("]");
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    // -------------------------------------------------------------------------
    // CLI arg parsing
    // -------------------------------------------------------------------------

    private static ImportConfig parseArgs(String[] args) {
        ImportConfig config = new ImportConfig();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--file":          config.setFilePath(args[++i]); break;
                case "--url":           config.setUrl(args[++i]); break;
                case "--sheet":         config.setSheetName(args[++i]); break;
                case "--dest":          config.setDestPath(args[++i]); break;
                case "--host":          config.setDremioHost(args[++i]); break;
                case "--port":          config.setDremioPort(Integer.parseInt(args[++i])); break;
                case "--user":          config.setUsername(args[++i]); break;
                case "--password":      config.setPassword(args[++i]); break;
                case "--header-row":    config.setHeaderRow(Integer.parseInt(args[++i]));
                                        config.setDataStartRow(config.getHeaderRow() + 1); break;
                case "--data-start":    config.setDataStartRow(Integer.parseInt(args[++i])); break;
                case "--sample-rows":   config.setSampleRows(Integer.parseInt(args[++i])); break;
                case "--batch-size":    config.setBatchSize(Integer.parseInt(args[++i])); break;
                case "--mode":          config.setMode(args[++i]); break;
                case "--types":         config.setTypeOverrides(args[++i]); break;
                case "--exclude":       config.setExcludeColumns(args[++i]); break;
                case "--rename":        config.setRenameColumns(args[++i]); break;
                case "--overwrite":     config.setOverwrite(true); break;
                case "--yes": case "-y": config.setYes(true); break;
                case "--list-sheets":   config.setListSheets(true); break;
                case "--preview":       config.setPreview(true); break;
                case "--json":          config.setJsonOutput(true); break;
                case "--help": case "-h": printUsage(); System.exit(0); break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }
        return config;
    }

    private static void validateConfig(ImportConfig config) {
        if (config.getFilePath() == null && config.getUrl() == null)
            die("--file or --url is required");
        if (config.getFilePath() != null) {
            String lp = config.getFilePath().toLowerCase();
            if (!lp.endsWith(".xlsx") && !lp.endsWith(".csv"))
                die("Unsupported file type. Only .xlsx and .csv are supported.");
        }
        if (config.isPreview()) return; // no Dremio connection needed
        if (config.isListSheets()) return;
        if (config.getDestPath() == null) die("--dest is required");
        if (config.getUsername() == null)  die("--user is required");
        if (config.getPassword() == null)  die("--password is required");
        if (!config.getDestPath().contains("."))
            die("--dest must be a dot-separated path, e.g. 'iceberg_minio.dremio-test.my_table'");
        String mode = config.getMode();
        if (!mode.equals("create") && !mode.equals("append"))
            die("--mode must be 'create' or 'append'");
    }

    private static void die(String msg) {
        System.err.println("Error: " + msg);
        System.err.println("Run with --help for usage.");
        System.exit(1);
    }

    private static void printUsage() {
        System.out.println("Dremio Excel Importer - import .xlsx files into Dremio Iceberg tables");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar dremio-excel-importer.jar [OPTIONS]");
        System.out.println();
        System.out.println("Input (one required):");
        System.out.println("  --file <path>         Path to local .xlsx file");
        System.out.println("  --url <url>           URL to download .xlsx from (e.g. Google Sheets export URL)");
        System.out.println();
        System.out.println("Required (except with --list-sheets or --preview):");
        System.out.println("  --dest <path>         Destination table, dot-separated (e.g. iceberg_minio.dremio-test.my_table)");
        System.out.println("  --user <username>     Dremio username");
        System.out.println("  --password <password> Dremio password");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --sheet <name>        Sheet to import (default: first sheet)");
        System.out.println("  --host <host>         Dremio host (default: localhost)");
        System.out.println("  --port <port>         Dremio REST API port (default: 9047)");
        System.out.println("  --mode create|append  create: CREATE TABLE then INSERT (default); append: INSERT only");
        System.out.println("  --types <overrides>   Type overrides e.g. \"zip_code=VARCHAR,revenue=DOUBLE\"");
        System.out.println("  --header-row <n>      Row index containing headers, 0-based (default: 0)");
        System.out.println("  --data-start <n>      Row index where data starts, 0-based (default: 1)");
        System.out.println("  --sample-rows <n>     Rows to sample for type inference (default: 500)");
        System.out.println("  --batch-size <n>      Rows per INSERT batch (default: 500)");
        System.out.println("  --overwrite           Drop existing table before import (create mode only)");
        System.out.println("  --yes / -y            Skip confirmation prompt");
        System.out.println("  --list-sheets         List sheet names in the workbook and exit");
        System.out.println("  --preview             Show inferred schema without importing");
        System.out.println("  --json                Emit JSON output (for --list-sheets and --preview)");
        System.out.println("  --help / -h           Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar dremio-excel-importer.jar --file report.xlsx --list-sheets");
        System.out.println("  java -jar dremio-excel-importer.jar --file report.xlsx --preview");
        System.out.println("  java -jar dremio-excel-importer.jar \\");
        System.out.println("    --file report.xlsx --dest \"iceberg_minio.dremio-test.report\" \\");
        System.out.println("    --user mark --password critter77 --overwrite --yes");
        System.out.println("  java -jar dremio-excel-importer.jar \\");
        System.out.println("    --url \"https://docs.google.com/spreadsheets/d/ID/export?format=xlsx\" \\");
        System.out.println("    --dest \"iceberg_minio.dremio-test.budget\" --user mark --password critter77");
        System.out.println("  java -jar dremio-excel-importer.jar \\");
        System.out.println("    --file data.xlsx --dest \"iceberg_minio.dremio-test.sales\" \\");
        System.out.println("    --mode append --user mark --password critter77 --yes");
    }
}
