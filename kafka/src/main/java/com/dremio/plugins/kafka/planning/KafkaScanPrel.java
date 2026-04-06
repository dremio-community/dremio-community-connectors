package com.dremio.plugins.kafka.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.kafka.scan.KafkaGroupScan;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
import com.dremio.exec.physical.base.GroupScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Physical plan node for a Kafka topic scan.
 *
 * getPhysicalOperator() returns a KafkaGroupScan, which Dremio's fragment scheduler
 * parallelizes (one split per Kafka partition) and calls getSpecificScan() on to produce
 * KafkaSubScan instances for each executor fragment.
 */
public class KafkaScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final KafkaGroupScan groupScan;
  private final KafkaScanSpec  scanSpec;

  public KafkaScanPrel(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelOptTable table,
                        StoragePluginId pluginId,
                        TableMetadata tableMetadata,
                        List<SchemaPath> projectedColumns,
                        double observedRowcountAdjustment,
                        List<RelHint> hints,
                        KafkaGroupScan groupScan,
                        KafkaScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override
  public GroupScan getGroupScan() {
    return groupScan;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new KafkaGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(),
        getProjectedColumns(),
        getPluginId(),
        scanSpec
    );
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new KafkaScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new KafkaScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(),
        groupScan, scanSpec);
  }
}
