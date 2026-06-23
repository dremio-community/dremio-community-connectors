package com.dremio.plugins.googleads.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.List;

public class GoogleAdsSubScan extends SubScanWithProjection {

  private final StoragePluginId   pluginId;
  private final GoogleAdsScanSpec scanSpec;

  @JsonCreator
  public GoogleAdsSubScan(
      @JsonProperty("props")    OpProps                  props,
      @JsonProperty("schema")   BatchSchema              schema,
      @JsonProperty("tables")   Collection<List<String>> tables,
      @JsonProperty("columns")  List<SchemaPath>         columns,
      @JsonProperty("pluginId") StoragePluginId          pluginId,
      @JsonProperty("scanSpec") GoogleAdsScanSpec        scanSpec) {
    super(props, schema, tables, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @JsonProperty("pluginId") public StoragePluginId   getPluginId()  { return pluginId; }
  @JsonProperty("scanSpec") public GoogleAdsScanSpec getScanSpec()  { return scanSpec; }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new GoogleAdsSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec);
  }

  @Override public int getOperatorType() { return 0; }

  // -----------------------------------------------------------------------
  // Scan spec
  // -----------------------------------------------------------------------

  public static class GoogleAdsScanSpec {

    private final String tableName;
    private final int    dateRangeDays;

    @JsonCreator
    public GoogleAdsScanSpec(
        @JsonProperty("tableName")     String tableName,
        @JsonProperty("dateRangeDays") int    dateRangeDays) {
      this.tableName     = tableName;
      this.dateRangeDays = dateRangeDays;
    }

    @JsonProperty("tableName")     public String getTableName()     { return tableName; }
    @JsonProperty("dateRangeDays") public int    getDateRangeDays() { return dateRangeDays; }

    public String toExtendedProperty() {
      return tableName + "|" + dateRangeDays;
    }

    public static GoogleAdsScanSpec fromExtendedProperty(String s) {
      if (s == null || s.isEmpty()) return null;
      String[] parts = s.split("\\|", 2);
      int days = 30;
      if (parts.length == 2) {
        try { days = Integer.parseInt(parts[1]); } catch (NumberFormatException ignored) {}
      }
      return new GoogleAdsScanSpec(parts[0], days);
    }

    @Override public String toString() {
      return "GoogleAdsScanSpec{table=" + tableName + ", days=" + dateRangeDays + "}";
    }
  }
}
