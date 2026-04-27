package com.dremio.plugins.spanner.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.spanner.SpannerStoragePluginConfig;
import com.dremio.plugins.spanner.scan.SpannerScanSpec;

import java.util.List;

/**
 * Converts a generic ScanCrel into a SpannerScanDrel during LOGICAL planning.
 * SourceLogicalConverter matches ScanCrel nodes for GOOGLE_CLOUD_SPANNER sources.
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

    SpannerScanSpec spec = new SpannerScanSpec(tableName);
    return new SpannerScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
  }
}
