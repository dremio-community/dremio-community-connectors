package com.dremio.plugins.spanner.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.spanner.scan.SpannerScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/** Logical plan node for a Spanner table scan. */
public class SpannerScanDrel extends ScanRelBase implements Rel {

  private final SpannerScanSpec scanSpec;

  public SpannerScanDrel(RelOptCluster cluster, RelTraitSet traitSet,
                          RelOptTable table, StoragePluginId pluginId,
                          TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                          double observedRowcountAdjustment, List<RelHint> hints,
                          SpannerScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints);
    this.scanSpec = scanSpec;
  }

  public SpannerScanSpec getScanSpec() { return scanSpec; }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SpannerScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(), getHints(), scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new SpannerScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
  }
}
