package com.dremio.community.excel.model;

public class ColumnDef {
    private final String originalName;
    private final String normalizedName;
    private final ColumnType type;

    public ColumnDef(String originalName, String normalizedName, ColumnType type) {
        this.originalName = originalName;
        this.normalizedName = normalizedName;
        this.type = type;
    }

    public String getOriginalName() { return originalName; }
    public String getName() { return normalizedName; }
    public ColumnType getType() { return type; }

    @Override
    public String toString() {
        return normalizedName + " " + type.toDremioSql() +
                (originalName.equals(normalizedName) ? "" : " (was: " + originalName + ")");
    }
}
