package com.dremio.plugins.pagerduty;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

/**
 * Logical plan node for a PagerDuty table scan.
 */
public class PagerDutyScanDrel extends ScanRelBase implements Rel {

    private final PagerDutyScanSpec scanSpec;

    public PagerDutyScanDrel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            StoragePluginId pluginId,
            TableMetadata tableMetadata,
            List<SchemaPath> projectedColumns,
            double observedRowcountAdjustment,
            List<RelHint> hints,
            PagerDutyScanSpec scanSpec) {
        super(cluster, traitSet, table, pluginId, tableMetadata,
                projectedColumns, observedRowcountAdjustment, hints);
        this.scanSpec = scanSpec;
    }

    public PagerDutyScanSpec getScanSpec() { return scanSpec; }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PagerDutyScanDrel(getCluster(), traitSet, getTable(), getPluginId(),
                getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(), getHints(), scanSpec);
    }

    @Override
    public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
        return new PagerDutyScanDrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
                getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), scanSpec);
    }
}
