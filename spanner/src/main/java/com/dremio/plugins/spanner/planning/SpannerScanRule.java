package com.dremio.plugins.spanner.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.spanner.SpannerStoragePluginConfig;
import com.dremio.plugins.spanner.scan.SpannerSubScan.SpannerScanSpec;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a generic ScanCrel into a SpannerScanDrel during LOGICAL planning.
 * SourceLogicalConverter matches ScanCrel nodes for GOOGLE_CLOUD_SPANNER sources.
 *
 * Column projection is captured here: if Dremio projects a subset of columns
 * the spec is populated so only those columns are fetched from Spanner.
 */
public class SpannerScanRule extends SourceLogicalConverter {

  public static final SpannerScanRule INSTANCE = new SpannerScanRule();

  private SpannerScanRule() {
    super(SpannerStoragePluginConfig.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    SpannerScanSpec spec = new SpannerScanSpec(tableName, 0, 1,
        extractColumns(scan.getProjectedColumns()), null, 0);

    return new SpannerScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
  }

  /**
   * Converts Dremio's SchemaPath projection list to column name strings.
   * Returns null (= SELECT *) when all columns are requested or the list is empty.
   */
  private static List<String> extractColumns(List<SchemaPath> projected) {
    if (projected == null || projected.isEmpty()) return null;
    // A path whose root segment is "*" means SELECT ALL columns
    boolean isStar = projected.stream()
        .anyMatch(p -> "*".equals(p.getRootSegment().getPath()));
    if (isStar) return null;
    List<String> cols = projected.stream()
        .map(p -> p.getRootSegment().getPath())
        .distinct()
        .collect(Collectors.toList());
    return cols.isEmpty() ? null : cols;
  }
}
