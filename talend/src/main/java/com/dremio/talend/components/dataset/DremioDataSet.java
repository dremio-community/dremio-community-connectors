package com.dremio.talend.components.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import com.dremio.talend.components.datastore.DremioDataStore;

@DataSet("DremioDataSet")
@GridLayout({
    @GridLayout.Row({ "datastore" }),
    @GridLayout.Row({ "sqlQuery" })
})
@Documentation("Dremio DataSet configuration")
public class DremioDataSet implements Serializable {

    @Option
    @Documentation("Dremio Connection")
    private DremioDataStore datastore;

    @Option
    @TextArea
    @Documentation("SQL Query to execute via Arrow Flight")
    private String sqlQuery = "SELECT * FROM sys.version";

    public DremioDataStore getDatastore() { return datastore; }
    public void setDatastore(DremioDataStore datastore) { this.datastore = datastore; }

    public String getSqlQuery() { return sqlQuery; }
    public void setSqlQuery(String sqlQuery) { this.sqlQuery = sqlQuery; }
}
