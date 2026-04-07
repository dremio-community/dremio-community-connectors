package com.dremio.community.excel.model;

public class ImportConfig {
    private String filePath;
    private String sheetName;        // null = first sheet
    private int headerRow = 0;       // 0-indexed row containing column headers
    private int dataStartRow = 1;    // 0-indexed first data row
    private int sampleRows = 500;    // rows used for schema inference
    private boolean skipEmptyRows = true;
    private String dremioHost = "localhost";
    private int dremioPort = 9047;
    private String username;
    private String password;
    private String destPath;         // e.g. "Samples.my_table" or "catalog.schema.table"
    private int batchSize = 500;     // rows per INSERT batch
    private boolean yes = false;     // skip confirmation prompt
    private boolean overwrite = false; // DROP TABLE IF EXISTS before import
    private boolean listSheets = false;  // just list sheets and exit
    private boolean preview = false;     // infer schema and print, no Dremio connection
    private boolean jsonOutput = false;  // emit JSON instead of human-readable text
    private String url = null;           // download XLSX from URL (Google Sheets etc.)
    private String mode = "create";      // create | append
    private String typeOverrides = null; // e.g. "zip_code=VARCHAR,revenue=DOUBLE"
    private String excludeColumns = null; // comma-separated normalized column names to exclude
    private String renameColumns = null;  // "old_name=new_name,..." normalized-to-desired

    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }

    public String getSheetName() { return sheetName; }
    public void setSheetName(String sheetName) { this.sheetName = sheetName; }

    public int getHeaderRow() { return headerRow; }
    public void setHeaderRow(int headerRow) { this.headerRow = headerRow; }

    public int getDataStartRow() { return dataStartRow; }
    public void setDataStartRow(int dataStartRow) { this.dataStartRow = dataStartRow; }

    public int getSampleRows() { return sampleRows; }
    public void setSampleRows(int sampleRows) { this.sampleRows = sampleRows; }

    public boolean isSkipEmptyRows() { return skipEmptyRows; }
    public void setSkipEmptyRows(boolean skipEmptyRows) { this.skipEmptyRows = skipEmptyRows; }

    public String getDremioHost() { return dremioHost; }
    public void setDremioHost(String dremioHost) { this.dremioHost = dremioHost; }

    public int getDremioPort() { return dremioPort; }
    public void setDremioPort(int dremioPort) { this.dremioPort = dremioPort; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getDestPath() { return destPath; }
    public void setDestPath(String destPath) { this.destPath = destPath; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public boolean isYes() { return yes; }
    public void setYes(boolean yes) { this.yes = yes; }

    public boolean isOverwrite() { return overwrite; }
    public void setOverwrite(boolean overwrite) { this.overwrite = overwrite; }

    public boolean isListSheets() { return listSheets; }
    public void setListSheets(boolean listSheets) { this.listSheets = listSheets; }

    public boolean isPreview() { return preview; }
    public void setPreview(boolean preview) { this.preview = preview; }

    public boolean isJsonOutput() { return jsonOutput; }
    public void setJsonOutput(boolean jsonOutput) { this.jsonOutput = jsonOutput; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

    public String getTypeOverrides() { return typeOverrides; }
    public void setTypeOverrides(String typeOverrides) { this.typeOverrides = typeOverrides; }

    public String getExcludeColumns() { return excludeColumns; }
    public void setExcludeColumns(String excludeColumns) { this.excludeColumns = excludeColumns; }

    public String getRenameColumns() { return renameColumns; }
    public void setRenameColumns(String renameColumns) { this.renameColumns = renameColumns; }
}
