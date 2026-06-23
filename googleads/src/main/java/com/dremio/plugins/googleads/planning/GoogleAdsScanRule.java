package com.dremio.plugins.googleads.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.googleads.GoogleAdsConf;
import com.dremio.plugins.googleads.scan.GoogleAdsSubScan.GoogleAdsScanSpec;

import java.util.List;

public class GoogleAdsScanRule extends SourceLogicalConverter {

  public static final GoogleAdsScanRule INSTANCE = new GoogleAdsScanRule();

  private GoogleAdsScanRule() {
    super(GoogleAdsConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    GoogleAdsConf cfg = scan.getPluginId().getConnectionConf();
    GoogleAdsScanSpec spec = new GoogleAdsScanSpec(tableName, cfg.dateRangeDays);

    return new GoogleAdsScanDrel(
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
