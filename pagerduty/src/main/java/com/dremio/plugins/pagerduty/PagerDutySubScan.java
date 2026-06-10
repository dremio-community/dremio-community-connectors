package com.dremio.plugins.pagerduty;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Collection;
import java.util.List;

/**
 * Serializable unit of work for PagerDuty scan execution.
 */
@JsonTypeName("pagerduty-sub-scan")
public class PagerDutySubScan extends SubScanWithProjection {

    public static final int OPERATOR_TYPE = 5003;

    private final StoragePluginId pluginId;
    private final PagerDutyScanSpec scanSpec;

    @JsonCreator
    public PagerDutySubScan(
            @JsonProperty("props")    OpProps                  props,
            @JsonProperty("schema")   BatchSchema              schema,
            @JsonProperty("tables")   Collection<List<String>> tables,
            @JsonProperty("columns")  List<SchemaPath>         columns,
            @JsonProperty("pluginId") StoragePluginId          pluginId,
            @JsonProperty("scanSpec") PagerDutyScanSpec          scanSpec) {
        super(props, schema, tables, columns);
        this.pluginId = pluginId;
        this.scanSpec = scanSpec;
    }

    @JsonProperty("pluginId")
    public StoragePluginId getPluginId() { return pluginId; }

    @JsonProperty("scanSpec")
    public PagerDutyScanSpec getScanSpec() { return scanSpec; }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
        return visitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        return new PagerDutySubScan(getProps(), getFullSchema(), getReferencedTables(),
                getColumns(), pluginId, scanSpec);
    }

    @Override
    public int getOperatorType() { return 0; }
}
