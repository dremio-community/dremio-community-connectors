package com.dremio.plugins.splunk.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes SELECT column projections into the Splunk SPL query.
 *
 * When a query asks for only a subset of columns — e.g.
 *   SELECT _time, _host FROM splunk.main
 * — this rule adds a "| fields _time _host" clause to the SPL string so that
 * Splunk only returns those columns in the result set. Without this, Splunk
 * returns every field and Dremio discards the unwanted ones after the fact.
 *
 * Impact: reduces JSON payload size, network transfer, and parse work in
 * proportion to the fraction of columns projected. On wide indexes with many
 * fields this can be a significant win.
 *
 * Fires during LOGICAL planning on any SplunkScanDrel whose scanSpec.fieldList
 * is empty and whose projectedColumns list is non-trivial (not star / all columns).
 *
 * Safe to fire multiple times: once fieldList is non-empty the rule is a no-op.
 */
public class SplunkProjectionRule extends RelOptRule {

  public static final SplunkProjectionRule INSTANCE = new SplunkProjectionRule();

  private static final Logger logger = LoggerFactory.getLogger(SplunkProjectionRule.class);

  private SplunkProjectionRule() {
    super(RelOptHelper.any(SplunkScanDrel.class, Rel.LOGICAL),
          "SplunkProjectionRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    SplunkScanDrel scan = call.rel(0);
    // Only fire if fieldList not yet populated
    return scan.getScanSpec().getFieldList().isEmpty();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SplunkScanDrel scan = call.rel(0);
    List<SchemaPath> projected = scan.getProjectedColumns();

    // Null or star projection means all columns — nothing to push
    if (projected == null || projected.isEmpty()) return;
    for (SchemaPath p : projected) {
      // Star-wildcard path has root segment == "*"
      String root = p.getRootSegment().getPath();
      if ("*".equals(root)) return;
    }

    // Convert SchemaPath list to flat field name strings
    List<String> fieldNames = new ArrayList<>(projected.size());
    for (SchemaPath p : projected) {
      String name = p.getRootSegment().getPath();
      if (name != null && !name.isEmpty() && !fieldNames.contains(name)) {
        fieldNames.add(name);
      }
    }
    if (fieldNames.isEmpty()) return;

    SplunkScanSpec newSpec = scan.getScanSpec().withFieldList(fieldNames);

    SplunkScanDrel newScan = new SplunkScanDrel(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getTable(),
        scan.getPluginId(),
        scan.getTableMetadata(),
        scan.getProjectedColumns(),
        scan.getCostAdjustmentFactor(),
        scan.getHints(),
        newSpec
    );

    logger.debug("SplunkProjectionRule: index='{}' projecting {} field(s): {}",
        scan.getScanSpec().getIndexName(), fieldNames.size(), fieldNames);

    call.transformTo(newScan);
  }
}
