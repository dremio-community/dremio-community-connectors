package com.dremio.plugins.splunk.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/**
 * Logical plan node for a Splunk index scan.
 *
 * Created by SplunkScanRule when it converts a generic ScanCrel into a
 * Splunk-specific logical scan node. Converted to SplunkScanPrel (physical)
 * by SplunkScanPrule after filter/limit pushdown by SplunkFilterRule.
 */
public class SplunkScanDrel extends ScanRelBase implements Rel {

  private final SplunkScanSpec scanSpec;

  public SplunkScanDrel(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelOptTable table,
                         StoragePluginId pluginId,
                         TableMetadata tableMetadata,
                         List<SchemaPath> projectedColumns,
                         double observedRowcountAdjustment,
                         List<RelHint> hints,
                         SplunkScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints);
    this.scanSpec = scanSpec;
  }

  public SplunkScanSpec getScanSpec() { return scanSpec; }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SplunkScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new SplunkScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
  }
}
