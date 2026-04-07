# Dremio Excel Importer

Import `.xlsx` spreadsheets, `.csv` files, and Google Sheets directly into Dremio Iceberg tables — no manual conversion or ETL pipeline required.

## What it does

1. Reads an Excel workbook, CSV file, or Google Sheets URL
2. Detects headers and infers column types automatically
3. Shows a schema preview with editable types before anything is written
4. Creates a native Iceberg table in Dremio and bulk-inserts all rows

Works entirely through the Dremio REST API — no JDBC driver or special client needed.

---

## Quick Start

**macOS:** double-click `Start Importer.command`
**Windows:** double-click `Start Importer.bat`
**Linux:** `python3 importer-ui.py`

Your browser opens to **http://localhost:8766**. The only requirement is **Java 11+** — the launcher will tell you if it's missing. No build step needed; the JAR is pre-built.

See [INSTALL.md](INSTALL.md) for details and troubleshooting.

---

## Requirements

| Requirement | Notes |
|---|---|
| Java 11+ | To run the importer JAR |
| Dremio 24+ | REST API must be accessible |
| Iceberg-compatible source | Table destination must support `CREATE TABLE` — see [Choosing a Destination](#choosing-a-destination) |
| Python 3.8+ | Only needed for the Web UI |
| Maven + Java 11 | Only needed to **build** from source |

---

## Installation

See [INSTALL.md](INSTALL.md) for full instructions. The short version:

```bash
# Clone or download this directory, then:
bash install.sh
```

This builds the fat JAR at `jars/dremio-excel-importer.jar`.

---

## Usage

### Command-Line Interface

```
java -jar jars/dremio-excel-importer.jar [OPTIONS]
```

#### Input (one required)

| Flag | Description |
|---|---|
| `--file <path>` | Path to a local `.xlsx` or `.csv` file |
| `--url <url>` | Download `.xlsx` from a URL (Google Sheets, OneDrive, etc.) |

#### Destination & Connection (required except for `--list-sheets` / `--preview`)

| Flag | Default | Description |
|---|---|---|
| `--dest <path>` | — | Dot-separated destination: `source.schema.table` |
| `--user <username>` | — | Dremio username |
| `--password <pass>` | — | Dremio password |
| `--host <host>` | `localhost` | Dremio host |
| `--port <port>` | `9047` | Dremio REST API port |

#### Import Options

| Flag | Default | Description |
|---|---|---|
| `--sheet <name>` | First sheet | Name of the worksheet to import |
| `--mode create\|append` | `create` | `create`: CREATE TABLE then INSERT. `append`: INSERT only |
| `--overwrite` | off | Drop existing table before importing (create mode only) |
| `--types <overrides>` | — | Override inferred types: `"zip_code=VARCHAR,revenue=DOUBLE"` |
| `--exclude <columns>` | — | Comma-separated column names to skip: `"notes,internal_id"` |
| `--rename <mappings>` | — | Rename columns: `"old_name=new_name,foo=bar"` |
| `--batch-size <n>` | `500` | Rows per INSERT batch |
| `--header-row <n>` | `0` | Zero-based row index of the header row |
| `--data-start <n>` | `1` | Zero-based row index where data begins |
| `--sample-rows <n>` | `500` | Rows sampled for type inference |
| `--yes` / `-y` | off | Skip the confirmation prompt |

#### Inspection Flags

| Flag | Description |
|---|---|
| `--list-sheets` | Print all sheet names in the workbook and exit |
| `--preview` | Show inferred schema without connecting to Dremio |
| `--json` | Emit JSON output (use with `--list-sheets` or `--preview`) |

---

### Examples

**List sheets in a workbook:**
```bash
java -jar jars/dremio-excel-importer.jar --file report.xlsx --list-sheets
```

**Preview schema without importing:**
```bash
java -jar jars/dremio-excel-importer.jar --file report.xlsx --preview
```

**Import first sheet, create table:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file report.xlsx \
  --dest "iceberg_minio.dremio-test.quarterly_report" \
  --user admin --password secret \
  --yes
```

**Import a specific sheet, overwrite if table exists:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file forecast.xlsx \
  --sheet "Q2 Forecast" \
  --dest "iceberg_minio.dremio-test.forecast_q2" \
  --user admin --password secret \
  --overwrite --yes
```

**Append new rows to an existing table:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file april_sales.xlsx \
  --dest "iceberg_minio.dremio-test.sales_2026" \
  --mode append \
  --user admin --password secret \
  --yes
```

**Import a CSV file:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file customers.csv \
  --dest "iceberg_minio.dremio-test.customers" \
  --user admin --password secret \
  --yes
```

**Override inferred column types:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file customers.xlsx \
  --dest "iceberg_minio.dremio-test.customers" \
  --types "zip_code=VARCHAR,customer_id=VARCHAR" \
  --user admin --password secret \
  --yes
```

**Exclude columns and rename others:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --file report.xlsx \
  --exclude "internal_notes,temp_col" \
  --rename "cust_nm=customer_name,rev=revenue" \
  --dest "iceberg_minio.dremio-test.report" \
  --user admin --password secret \
  --yes
```

**Import from Google Sheets:**
```bash
java -jar jars/dremio-excel-importer.jar \
  --url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/export?format=xlsx" \
  --dest "iceberg_minio.dremio-test.budget_2026" \
  --user admin --password secret \
  --yes
```

---

### Web UI

The web UI is the easiest way to use the importer for non-technical users.

```bash
python3 importer-ui.py
```

Open **http://localhost:8766** in your browser.

**Options:**
```
python3 importer-ui.py [--port 8766] [--jar path/to/dremio-excel-importer.jar]
                       [--docker <container>]
                       [--kubectl <pod>] [--namespace <ns>]
```

**Running when Java is not installed locally:**

If you don't have Java on your machine but Dremio is running in Docker or Kubernetes, the importer can run the JAR inside that container instead:

```bash
# Docker
python3 importer-ui.py --docker try-dremio

# Kubernetes (pod must have Java — standard Dremio images do)
python3 importer-ui.py --kubectl dremio-master-0 --namespace dremio

# kubectl port-forward if the REST API isn't externally exposed
kubectl port-forward svc/dremio 9047:9047 &
python3 importer-ui.py --kubectl dremio-master-0 --namespace dremio
```

The `Start Importer.command` launcher (macOS) auto-detects Docker containers and Kubernetes pods named `dremio*` — if Java isn't found locally it tries these automatically.

**UI workflow:**

1. **Select File** — drag and drop an `.xlsx` or `.csv` file, or paste a Google Sheets URL
2. **Configure** — pick the sheet, header row, destination path, and Dremio connection details; save connection settings as named profiles
3. **Test Connection** — verify credentials inline before importing
4. **Schema Preview** — review inferred column names and types; use the per-column type dropdowns to fix wrong types; check/uncheck columns to include/exclude; rename columns inline
5. **Import** — click "Import to Dremio" and watch the live streaming progress log
6. **History** — review past imports in the collapsible History panel

---

## Web UI Features

| Feature | Description |
|---|---|
| **Drag & drop upload** | Drop `.xlsx` or `.csv` onto the file area |
| **Google Sheets URL** | Paste any Google Sheets link — edit, view, or export URL formats all work |
| **Multi-sheet workbooks** | Preview any sheet; "Import All Sheets" creates one table per sheet |
| **Connection profiles** | Save named connection presets (host/port/user); switch with one click |
| **Test Connection** | Validates Dremio credentials before importing |
| **Header row selector** | Pick which row contains headers (for files with metadata rows at the top) |
| **Per-column type override** | Dropdown per column in the schema preview to change the inferred type |
| **Column include/exclude** | Uncheck columns to skip them during import |
| **Column rename** | Edit column names directly in the schema preview |
| **Sample data preview** | See up to 5 sample rows per column to verify types before importing |
| **Append schema validation** | In append mode, warns if the source columns don't match the existing table |
| **Live progress log** | Streaming log shows each batch as it's inserted |
| **Import history** | Last 50 imports stored in browser localStorage, with timestamps and row counts |
| **Settings persistence** | Host, port, and username saved across browser sessions |

---

## Type Inference

The importer reads up to 500 rows (configurable via `--sample-rows`) to infer column types. The rules are conservative — when in doubt, a column becomes `VARCHAR`.

### Excel / XLSX

| Excel cell type | Inferred Dremio type |
|---|---|
| Integer number (no decimal) | `BIGINT` |
| Decimal number | `DOUBLE` |
| Boolean | `BOOLEAN` |
| Date-formatted cell (no time) | `DATE` |
| Date-formatted cell (with time) | `TIMESTAMP` |
| Text | `VARCHAR` |
| Mixed types in a column | `VARCHAR` |
| Formula cell | Type of the evaluated result |
| Blank / error cell | `NULL` (column type from other rows) |

### CSV

CSV values are parsed with best-effort type coercion in this order:

| Rule | Inferred type |
|---|---|
| Parses as integer | `BIGINT` |
| Parses as decimal | `DOUBLE` |
| `true` / `false` (case-insensitive) | `BOOLEAN` |
| Matches a date pattern (e.g. `2024-01-15`, `01/15/2024`) | `DATE` |
| Matches a datetime pattern (e.g. `2024-01-15 14:30:00`) | `TIMESTAMP` |
| Everything else | `VARCHAR` |

The CSV delimiter is auto-detected from the first line (comma, tab, or semicolon).

**Column name normalization (applies to all file types):**
- Lowercased
- Spaces and special characters replaced with `_`
- Leading digits prefixed with `col_`
- Duplicate names suffixed with `_2`, `_3`, etc.

Examples: `Order ID` → `order_id`, `Amount ($)` → `amount___`, `1st Quarter` → `col_1st_quarter`

---

## Choosing a Destination

The `--dest` path must point to a location in Dremio that supports `CREATE TABLE`. In Dremio 26.x:

| Destination type | Supports CREATE TABLE | Notes |
|---|---|---|
| Iceberg source (S3, MinIO, ADLS) with `defaultCtasFormat=ICEBERG` | ✅ Yes | Recommended |
| Nessie / Arctic catalog | ✅ Yes | Full Git-style versioning |
| Filesystem source (NFS, local) | ✅ Yes | For on-prem deployments |
| Dremio Spaces (`Samples`, custom spaces) | ❌ No | Spaces don't support DDL in 26.x |
| Home space (`@username`) | ❌ No | Same limitation |

**Example paths:**
```
iceberg_minio.dremio-test.my_table          ← S3/MinIO bucket + table name
nessie.main.my_schema.my_table              ← Nessie catalog
my_nas_source.imports.my_table              ← Filesystem source + folder + table
```

---

## Google Sheets

The web UI accepts any Google Sheets link — paste the URL from your browser address bar (edit URL), a share link, or a "Publish to web" export URL. The importer automatically converts it to the correct export format.

For the command line, use the export URL directly:
```
https://docs.google.com/spreadsheets/d/SHEET_ID/export?format=xlsx
```

To import a **single sheet** by tab ID:
```
https://docs.google.com/spreadsheets/d/SHEET_ID/export?format=xlsx&gid=SHEET_GID
```

> The sheet must be shared with "Anyone with the link" (view access). Private sheets return a 401 error with instructions.

---

## Limitations

- `.xlsx` and `.csv` only — `.xls` is not supported (use Excel's "Save As" to convert)
- Formula results are imported as their evaluated values — formula logic is not preserved
- Merged cells: only the top-left cell value is read; merged neighbors are treated as blank
- No support for Excel tables, charts, images, or macros
- Password-protected workbooks are not supported
- Very large files (100K+ rows) will work but may be slow due to the REST API batch approach — consider splitting or using a direct Parquet pipeline for bulk loads

---

## Building from Source

```bash
# Prerequisites: Java 11+, Maven 3.6+
mvn package -q
# Output: jars/dremio-excel-importer.jar
```

Or using the Docker container (if Maven isn't installed locally):
```bash
bash install.sh --docker try-dremio
```

See [INSTALL.md](INSTALL.md) for details.

---

## Project Structure

```
dremio-excel-importer/
├── jars/
│   └── dremio-excel-importer.jar      # Pre-built fat JAR (24 MB)
├── sample-data/
│   └── sample.xlsx                    # Generated sample workbook for testing
├── src/main/java/com/dremio/community/excel/
│   ├── ExcelImporter.java             # CLI entry point
│   ├── dremio/DremioClient.java       # REST API client
│   ├── inference/SchemaInferrer.java  # Type inference
│   ├── model/                         # Data models
│   ├── parser/WorkbookParser.java     # Apache POI XLSX + CSV reader
│   ├── sql/SqlGenerator.java          # DDL/DML SQL generation
│   └── util/                          # Column normalizer, sample data generator
├── importer-ui.py                     # Web UI (Python 3, no dependencies)
├── install.sh                         # Build script
├── test-import.sh                     # Smoke test
├── README.md                          # This file
└── INSTALL.md                         # Detailed installation guide
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.
