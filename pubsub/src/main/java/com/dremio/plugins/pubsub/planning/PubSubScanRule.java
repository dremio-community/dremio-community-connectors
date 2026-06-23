package com.dremio.plugins.pubsub.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.pubsub.PubSubConf;
import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;

import java.util.List;

/**
 * Converts a generic ScanCrel into a PubSubScanDrel during LOGICAL planning.
 */
public class PubSubScanRule extends SourceLogicalConverter {

  public static final PubSubScanRule INSTANCE = new PubSubScanRule();

  private PubSubScanRule() {
    super(PubSubConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String subscription = qualifiedName.get(qualifiedName.size() - 1);
    PubSubScanSpec spec = new PubSubScanSpec(subscription, 1000, "JSON");

    return new PubSubScanDrel(
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
