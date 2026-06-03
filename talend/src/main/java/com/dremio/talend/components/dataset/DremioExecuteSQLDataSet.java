package com.dremio.talend.components.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import com.dremio.talend.components.datastore.DremioDataStore;

@DataSet("DremioExecuteSQLDataSet")
@GridLayout({
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "sqlStatement" }),
    @GridLayout.Row({ "restPort" }),
    @GridLayout.Row({ "pollIntervalMs" }),
    @GridLayout.Row({ "timeoutSeconds" })
})
@Documentation("Dremio Execute SQL DataSet — submits a SQL statement to Dremio and emits the job result.")
public class DremioExecuteSQLDataSet implements Serializable {

    @Option
    @Documentation("Dremio Connection")
    private DremioDataStore datastore;

    @Option
    @TextArea
    @Documentation("SQL statement to execute (DDL or DML — e.g. INSERT INTO ... SELECT ..., CTAS, DROP TABLE).")
    private String sqlStatement;

    @Option
    @Documentation("Dremio REST API port (9047 for HTTP, 443 for Dremio Cloud / HTTPS).")
    private int restPort = 9047;

    @Option
    @Documentation("How often to poll for job completion, in milliseconds.")
    private int pollIntervalMs = 500;

    @Option
    @Documentation("Maximum seconds to wait before declaring the job timed out.")
    private int timeoutSeconds = 300;

    public DremioDataStore getDatastore() { return datastore; }
    public void setDatastore(DremioDataStore datastore) { this.datastore = datastore; }

    public String getSqlStatement() { return sqlStatement; }
    public void setSqlStatement(String sqlStatement) { this.sqlStatement = sqlStatement; }

    public int getRestPort() { return restPort; }
    public void setRestPort(int restPort) { this.restPort = restPort; }

    public int getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(int pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getTimeoutSeconds() { return timeoutSeconds; }
    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
}
