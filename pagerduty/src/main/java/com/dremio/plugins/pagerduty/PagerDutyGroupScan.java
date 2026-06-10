package com.dremio.plugins.pagerduty;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Physical group scan for PagerDuty. Single split per table.
 */
public class PagerDutyGroupScan extends AbstractGroupScan {

    private final StoragePluginId pluginId;
    private final PagerDutyScanSpec scanSpec;
    private final TableMetadata tableMetadata;

    public PagerDutyGroupScan(
            OpProps props,
            TableMetadata tableMetadata,
            List<SchemaPath> columns,
            StoragePluginId pluginId,
            PagerDutyScanSpec scanSpec) {
        super(props, tableMetadata, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
        return new PagerDutySubScan(
                getProps(),
                getFullSchema(),
                List.of(List.of(scanSpec.getTableName())),
                getColumns(),
                pluginId,
                scanSpec);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 1; // PagerDuty has no offset-based splits
    }

    @Override
    public int getOperatorType() {
        return 0;
    }

    @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
    @JsonIgnore public PagerDutyScanSpec getScanSpec() { return scanSpec; }
}
