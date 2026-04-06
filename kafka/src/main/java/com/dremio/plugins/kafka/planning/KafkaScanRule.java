package com.dremio.plugins.kafka.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.kafka.KafkaConf;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.List;

/**
 * Converts a generic ScanCrel (Dremio's catalog-level scan) into a
 * KafkaScanDrel (Kafka-specific logical scan) during the LOGICAL planning phase.
 *
 * SourceLogicalConverter automatically matches ScanCrel nodes for the
 * source type associated with KafkaConf (@SourceType("APACHE_KAFKA")).
 */
public class KafkaScanRule extends SourceLogicalConverter {

  public static final KafkaScanRule INSTANCE = new KafkaScanRule();

  private KafkaScanRule() {
    super(KafkaConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    // Entity path for Kafka: [source_name, topic_name]
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String topic = qualifiedName.get(qualifiedName.size() - 1);

    // Prototype spec — partition/offsets will be resolved at partition chunk time
    KafkaScanSpec spec = new KafkaScanSpec(topic, 0, 0, 0, "JSON");

    return new KafkaScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(),
        scan.getPluginId(),
        scan.getTableMetadata(),
        scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(),
        scan.getHints(),
        spec
    );
  }
}
