package com.dremio.community.excel.model;

public enum ColumnType {
    BIGINT("BIGINT"),
    DOUBLE("DOUBLE"),
    BOOLEAN("BOOLEAN"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    VARCHAR("VARCHAR");

    private final String dremioSql;

    ColumnType(String dremioSql) {
        this.dremioSql = dremioSql;
    }

    public String toDremioSql() {
        return dremioSql;
    }
}
