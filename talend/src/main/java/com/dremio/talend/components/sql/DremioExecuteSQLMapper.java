package com.dremio.talend.components.sql;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.dremio.talend.components.dataset.DremioExecuteSQLDataSet;

@Version(1)
@Icon(value = Icon.IconType.DB_QUERY)
@PartitionMapper(name = "DremioExecuteSQL")
@Documentation("Submits a SQL statement to Dremio via the REST API and emits a single result record containing the job ID, final status, and any error message.")
public class DremioExecuteSQLMapper implements Serializable {

    private final DremioExecuteSQLDataSet configuration;
    private final RecordBuilderFactory recordBuilderFactory;

    public DremioExecuteSQLMapper(@Option("configuration") final DremioExecuteSQLDataSet configuration,
                                  final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<DremioExecuteSQLMapper> split(@PartitionSize final long bundles) {
        return Collections.singletonList(this);
    }

    @Emitter
    public DremioExecuteSQLSource createWorker() {
        return new DremioExecuteSQLSource(configuration, recordBuilderFactory);
    }
}
