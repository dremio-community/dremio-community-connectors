package com.dremio.plugins.dynamodb.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.dynamodb.scan.DynamoDBScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/** Logical plan node for a DynamoDB table scan. */
public class DynamoDBScanDrel extends ScanRelBase implements Rel {

  private final DynamoDBScanSpec scanSpec;

  public DynamoDBScanDrel(RelOptCluster cluster, RelTraitSet traitSet,
                           RelOptTable table, StoragePluginId pluginId,
                           TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                           double observedRowcountAdjustment, List<RelHint> hints,
                           DynamoDBScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints);
    this.scanSpec = scanSpec;
  }

  public DynamoDBScanSpec getScanSpec() { return scanSpec; }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DynamoDBScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(), getHints(), scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new DynamoDBScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
  }
}
