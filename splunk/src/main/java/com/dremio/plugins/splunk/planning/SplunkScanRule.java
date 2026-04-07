package com.dremio.plugins.splunk.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.splunk.SplunkConf;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.Collections;
import java.util.List;

/**
 * Converts a generic ScanCrel into a SplunkScanDrel during LOGICAL planning.
 *
 * SourceLogicalConverter automatically matches ScanCrel nodes for the source
 * type associated with SplunkConf (@SourceType("SPLUNK")).
 *
 * The spec created here has defaults from config. SplunkFilterRule may later
 * enrich the spec with time-range and field-filter pushdown.
 */
public class SplunkScanRule extends SourceLogicalConverter {

  public static final SplunkScanRule INSTANCE = new SplunkScanRule();

  private SplunkScanRule() {
    super(SplunkConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String indexName = qualifiedName.get(qualifiedName.size() - 1);

    // Prototype spec — filter/limit will be pushed in by SplunkFilterRule
    SplunkScanSpec spec = new SplunkScanSpec(
        indexName,
        null,   // earliest: will use config default at execution time
        "now",
        -1L,    // earliestEpochMs: no filter
        -1L,    // latestEpochMs: no filter
        "",     // splFilter: none
        50000,  // maxEvents: default (will be overridden by LIMIT pushdown)
        Collections.emptyList()
    );

    return new SplunkScanDrel(
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
