/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * -----------------------------------------------------------------
 * PATCHED VERSION (Dremio Pinot Connector, 2026-04-07)
 * -----------------------------------------------------------------
 * Fix: getString(row, col) — when a JSON cell is an ArrayNode
 * (Pinot multi-value column), extract the first element as a plain
 * string instead of calling toString() which adds array brackets
 * like [-2147483648].  The brackets cause Long.parseLong() /
 * Integer.parseInt() to throw NumberFormatException when Dremio
 * reads BIGINT/INTEGER columns backed by Pinot INT_ARRAY fields.
 *
 * All other methods are identical to the upstream class.
 */
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;


public class ResultTableResultSet extends AbstractResultSet {

  private final JsonNode _rowsArray;
  private final JsonNode _columnNamesArray;
  private final JsonNode _columnDataTypesArray;

  public ResultTableResultSet(JsonNode resultTableJson) {
    _rowsArray = resultTableJson.get("rows");
    JsonNode dataSchema = resultTableJson.get("dataSchema");
    _columnNamesArray = dataSchema.get("columnNames");
    _columnDataTypesArray = dataSchema.get("columnDataTypes");
  }

  @Override
  public int getRowCount() {
    return _rowsArray.size();
  }

  @Override
  public int getColumnCount() {
    return _columnNamesArray.size();
  }

  @Override
  public String getColumnName(int columnIndex) {
    return _columnNamesArray.get(columnIndex).asText();
  }

  @Override
  public String getColumnDataType(int columnIndex) {
    return _columnDataTypesArray.get(columnIndex).asText();
  }

  /**
   * Returns the string representation of a cell value.
   *
   * <p>Upstream behaviour: textual nodes → textValue(); everything else →
   * toString() (which adds array brackets for ArrayNode).
   *
   * <p>Patched behaviour: ArrayNode (Pinot multi-value column) → take the
   * first element and return its scalar text, so callers like
   * {@code Long.parseLong()} receive {@code "-2147483648"} rather than
   * {@code "[-2147483648]"}.  An empty array returns {@code null}.
   */
  @Override
  public String getString(int rowIndex, int columnIndex) {
    JsonNode node = _rowsArray.get(rowIndex).get(columnIndex);

    if (node.isTextual()) {
      return node.textValue();
    }

    // Multi-value (array) column: unwrap first element to avoid bracket-wrapped
    // representations that break numeric parsing in downstream JDBC consumers.
    if (node.isArray()) {
      if (node.size() == 0) {
        return null;
      }
      JsonNode first = node.get(0);
      if (first.isNull()) {
        return null;
      }
      if (first.isTextual()) {
        return first.textValue();
      }
      // Numeric / boolean scalar — asText() gives the plain value without quotes or brackets.
      return first.asText();
    }

    return node.toString();
  }

  public List<String> getAllColumns() {
    List<String> columns = new ArrayList<>();
    if (_columnNamesArray == null) {
      return columns;
    }
    for (JsonNode column : _columnNamesArray) {
      columns.add(column.textValue());
    }
    return columns;
  }

  public List<String> getAllColumnsDataTypes() {
    List<String> columnDataTypes = new ArrayList<>();
    if (_columnDataTypesArray == null) {
      return columnDataTypes;
    }
    for (JsonNode columnDataType : _columnDataTypesArray) {
      columnDataTypes.add(columnDataType.textValue());
    }
    return columnDataTypes;
  }

  @Override
  public int getGroupKeyLength() {
    return 0;
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    throw new AssertionError("No group key string for result table");
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    throw new AssertionError("No group key column name for result table");
  }

  @Override
  public String toString() {
    int columnCount = getColumnCount();
    TextTable textTable = new TextTable();
    String[] columnNames = new String[columnCount];
    String[] columnDataTypes = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columnNames[i] = _columnNamesArray.get(i).asText();
      columnDataTypes[i] = _columnDataTypesArray.get(i).asText();
    }
    textTable.addHeader(columnNames);
    textTable.addHeader(columnDataTypes);

    int rowCount = getRowCount();
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      String[] row = new String[columnCount];
      for (int colIndex = 0; colIndex < columnCount; colIndex++) {
        try {
          row[colIndex] = getString(rowIndex, colIndex);
        } catch (Exception e) {
          row[colIndex] = "ERROR";
        }
      }
      textTable.addRow(row);
    }
    return textTable.toString();
  }
}
