package com.dremio.plugins.cassandra.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/**
 * Logical plan node for a Cassandra table scan.
 *
 * Created by CassandraScanRule when it converts a generic ScanCrel
 * (produced by Dremio's catalog scan) into a Cassandra-specific logical scan.
 * Later converted to CassandraScanPrel (physical) by CassandraScanPrule.
 */
public class CassandraScanDrel extends ScanRelBase implements Rel {

  private final CassandraScanSpec scanSpec;

  public CassandraScanDrel(RelOptCluster cluster,
                             RelTraitSet traitSet,
                             RelOptTable table,
                             StoragePluginId pluginId,
                             TableMetadata tableMetadata,
                             List<SchemaPath> projectedColumns,
                             double observedRowcountAdjustment,
                             List<RelHint> hints,
                             CassandraScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints);
    this.scanSpec = scanSpec;
  }

  public CassandraScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CassandraScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new CassandraScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
  }
}
