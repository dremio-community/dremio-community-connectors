package com.dremio.talend.components.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import com.dremio.talend.components.datastore.DremioDataStore;

@DataSet("DremioOutputDataSet")
@GridLayout({
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "tablePath" }),
    @GridLayout.Row({ "writeMode" }),
    @GridLayout.Row({ "batchSize" }),
    @GridLayout.Row({ "restPort" })
})
@Documentation("Dremio Output DataSet — writes Talend records into a Dremio Iceberg table via the REST API.")
public class DremioOutputDataSet implements Serializable {

    @Option
    @Documentation("Dremio Connection")
    private DremioDataStore datastore;

    @Option
    @Documentation("Fully-qualified target table path (e.g. my_catalog.my_schema.my_table)")
    private String tablePath;

    @Option
    @Documentation("APPEND adds rows to an existing table. OVERWRITE deletes all existing rows before writing.")
    private WriteMode writeMode = WriteMode.APPEND;

    @Option
    @Documentation("Number of records to accumulate before sending each INSERT batch to Dremio.")
    private int batchSize = 500;

    @Option
    @Documentation("Dremio REST API port (9047 for HTTP, 443 for Dremio Cloud / HTTPS).")
    private int restPort = 9047;

    public enum WriteMode { APPEND, OVERWRITE }

    public DremioDataStore getDatastore() { return datastore; }
    public void setDatastore(DremioDataStore datastore) { this.datastore = datastore; }

    public String getTablePath() { return tablePath; }
    public void setTablePath(String tablePath) { this.tablePath = tablePath; }

    public WriteMode getWriteMode() { return writeMode; }
    public void setWriteMode(WriteMode writeMode) { this.writeMode = writeMode; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getRestPort() { return restPort; }
    public void setRestPort(int restPort) { this.restPort = restPort; }
}
