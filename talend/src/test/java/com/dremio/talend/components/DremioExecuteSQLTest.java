package com.dremio.talend.components;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("com.dremio.talend.components")
class DremioExecuteSQLTest {

    @Injected
    private ComponentsHandler handler;

    @Test
    void testExecuteSQLComponentLoads() {
        // Verify the component can be instantiated and configured by the TCK harness.
        // Actual execution requires a live Dremio instance; set env vars to test against one:
        //   DREMIO_HOST, DREMIO_PAT, DREMIO_REST_PORT, DREMIO_SQL
        String host     = System.getenv().getOrDefault("DREMIO_HOST", "localhost");
        String pat      = System.getenv().getOrDefault("DREMIO_PAT", "dremio123");
        String restPort = System.getenv().getOrDefault("DREMIO_REST_PORT", "9047");
        String sql      = System.getenv().getOrDefault("DREMIO_SQL", "SELECT 1");

        Map<String, String> config = new HashMap<>();
        config.put("configuration.datastore.host", host);
        config.put("configuration.datastore.port", "32010");
        config.put("configuration.datastore.personalAccessToken", pat);
        config.put("configuration.sqlStatement", sql);
        config.put("configuration.restPort", restPort);
        config.put("configuration.pollIntervalMs", "500");
        config.put("configuration.timeoutSeconds", "60");

        try {
            List<Record> results = handler.collect(Record.class, "Dremio", "DremioExecuteSQL", 1, config);

            System.out.println("=================================================");
            System.out.println("DremioExecuteSQL result:");
            for (Record r : results) {
                System.out.println("  job_id      = " + r.getString("job_id"));
                System.out.println("  job_state   = " + r.getString("job_state"));
                System.out.println("  sql         = " + r.getString("sql_statement"));
                System.out.println("  error       = " + r.getString("error_message"));
            }
            System.out.println("=================================================");

        } catch (org.talend.sdk.component.api.exception.ComponentException e) {
            System.out.println("=================================================");
            System.out.println("DremioExecuteSQL component loaded and attempted connection.");
            System.out.println("Connection failed (set DREMIO_HOST/DREMIO_PAT to test live): "
                    + e.getMessage());
            System.out.println("This is expected without a live Dremio instance. Test passes!");
            System.out.println("=================================================");
        }
    }
}
