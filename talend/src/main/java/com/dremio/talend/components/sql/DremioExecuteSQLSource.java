package com.dremio.talend.components.sql;

import java.io.Serializable;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.dremio.talend.components.dataset.DremioExecuteSQLDataSet;
import com.dremio.talend.components.datastore.DremioDataStore;
import com.dremio.talend.components.service.DremioRestClient;

public class DremioExecuteSQLSource implements Serializable {

    private final DremioExecuteSQLDataSet configuration;
    private final RecordBuilderFactory recordBuilderFactory;

    private transient Record result;
    private transient boolean emitted;

    public DremioExecuteSQLSource(final DremioExecuteSQLDataSet configuration,
                                  final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @PostConstruct
    public void init() {
        DremioDataStore store = configuration.getDatastore();
        DremioRestClient client = new DremioRestClient(
                store.getHost(),
                configuration.getRestPort(),
                store.isEnableSsl(),
                store.getPersonalAccessToken());

        String jobId = null;
        String jobState = null;
        String errorMessage = null;

        try {
            jobId = client.submitSql(configuration.getSqlStatement());
            jobState = client.pollJobUntilDone(
                    jobId,
                    configuration.getPollIntervalMs(),
                    configuration.getTimeoutSeconds());
        } catch (Exception e) {
            jobState = "FAILED";
            errorMessage = e.getMessage();
        }

        result = recordBuilderFactory.newRecordBuilder()
                .withString("job_id", jobId != null ? jobId : "")
                .withString("job_state", jobState != null ? jobState : "FAILED")
                .withString("sql_statement", configuration.getSqlStatement())
                .withString("error_message", errorMessage != null ? errorMessage : "")
                .build();

        if ("FAILED".equals(jobState) && errorMessage != null) {
            throw new ComponentException(
                    "Dremio SQL execution failed (job=" + jobId + "): " + errorMessage);
        }

        emitted = false;
    }

    @Producer
    public Record next() {
        if (!emitted) {
            emitted = true;
            return result;
        }
        return null;
    }
}
