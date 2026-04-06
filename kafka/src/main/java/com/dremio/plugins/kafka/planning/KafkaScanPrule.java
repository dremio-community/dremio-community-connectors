package com.dremio.plugins.kafka.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.kafka.scan.KafkaGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.Collections;

/**
 * Converts a KafkaScanDrel (logical) into a KafkaScanPrel (physical)
 * during the PHYSICAL planning phase.
 */
public class KafkaScanPrule extends RelOptRule {

  public static final KafkaScanPrule INSTANCE = new KafkaScanPrule();

  private KafkaScanPrule() {
    super(RelOptHelper.any(KafkaScanDrel.class, Rel.LOGICAL), "KafkaScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof KafkaScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    KafkaScanDrel drel = call.rel(0);

    KafkaGroupScan groupScan = new KafkaGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getPluginId(),
        drel.getScanSpec()
    );

    KafkaScanPrel prel = new KafkaScanPrel(
        drel.getCluster(),
        drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(),
        drel.getPluginId(),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getCostAdjustmentFactor(),
        drel.getHints(),
        groupScan,
        drel.getScanSpec()
    );

    call.transformTo(prel);
  }
}
