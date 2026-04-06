package com.dremio.plugins.kafka.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/**
 * Logical plan node for a Kafka topic scan.
 *
 * Created by KafkaScanRule when it converts a generic ScanCrel into a
 * Kafka-specific logical scan node. Later converted to KafkaScanPrel (physical)
 * by KafkaScanPrule.
 */
public class KafkaScanDrel extends ScanRelBase implements Rel {

  private final KafkaScanSpec scanSpec;

  public KafkaScanDrel(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelOptTable table,
                        StoragePluginId pluginId,
                        TableMetadata tableMetadata,
                        List<SchemaPath> projectedColumns,
                        double observedRowcountAdjustment,
                        List<RelHint> hints,
                        KafkaScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints);
    this.scanSpec = scanSpec;
  }

  public KafkaScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new KafkaScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new KafkaScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
  }
}
