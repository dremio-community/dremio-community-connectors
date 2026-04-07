package com.dremio.community.excel.model;

import java.util.List;

public class InferredSchema {
    private final List<ColumnDef> columns;

    public InferredSchema(List<ColumnDef> columns) {
        this.columns = columns;
    }

    public List<ColumnDef> getColumns() { return columns; }
    public int size() { return columns.size(); }
}
