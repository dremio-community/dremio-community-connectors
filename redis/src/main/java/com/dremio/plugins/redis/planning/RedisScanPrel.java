package com.dremio.plugins.redis.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.plugins.redis.scan.RedisGroupScan;
import com.dremio.plugins.redis.scan.RedisScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RedisScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final RedisGroupScan groupScan;
  private final RedisScanSpec  scanSpec;

  public RedisScanPrel(RelOptCluster cluster, RelTraitSet traitSet,
                        RelOptTable table, StoragePluginId pluginId,
                        TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                        double observedRowcountAdjustment, List<RelHint> hints,
                        RedisGroupScan groupScan, RedisScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override public GroupScan getGroupScan() { return groupScan; }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new RedisGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(), getProjectedColumns(), getPluginId(), scanSpec);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RedisScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new RedisScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), groupScan, scanSpec);
  }
}
