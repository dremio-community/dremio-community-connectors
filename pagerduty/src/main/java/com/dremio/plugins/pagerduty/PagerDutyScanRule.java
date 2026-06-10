package com.dremio.plugins.pagerduty;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;

import java.util.List;

/**
 * Converts a generic ScanCrel backed by a PagerDutyConf source into a PagerDutyScanDrel.
 */
public class PagerDutyScanRule extends SourceLogicalConverter {

    public static final PagerDutyScanRule INSTANCE = new PagerDutyScanRule();

    private PagerDutyScanRule() {
        super(PagerDutyConf.class);
    }

    @Override
    public Rel convertScan(ScanCrel scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String tableName = qualifiedName.get(qualifiedName.size() - 1).toLowerCase();

        long estimatedRows = (long) Math.max(1.0,
                scan.getTableMetadata().getReadDefinition() != null
                        && scan.getTableMetadata().getReadDefinition().getScanStats() != null
                        ? scan.getTableMetadata().getReadDefinition().getScanStats().getRecordCount()
                        : 10_000L);

        PagerDutyScanSpec spec = new PagerDutyScanSpec(tableName, estimatedRows);

        return new PagerDutyScanDrel(
                scan.getCluster(),
                scan.getTraitSet().replace(Rel.LOGICAL),
                scan.getTable(),
                scan.getPluginId(),
                scan.getTableMetadata(),
                scan.getProjectedColumns(),
                scan.getObservedRowcountAdjustment(),
                scan.getHints(),
                spec);
    }
}
