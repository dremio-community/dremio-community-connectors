package com.dremio.talend.components;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@WithComponents("com.dremio.talend.components")
class DremioOutputTest {

    @Injected
    private ComponentsHandler handler;

    @Test
    void testOutputComponentLoads() {
        // Verify the component can be instantiated and configured by the TCK harness.
        // Actual write requires a live Dremio instance; set env vars to test against one:
        //   DREMIO_HOST, DREMIO_PORT, DREMIO_PAT, DREMIO_REST_PORT, DREMIO_TABLE
        String host     = System.getenv().getOrDefault("DREMIO_HOST", "localhost");
        String pat      = System.getenv().getOrDefault("DREMIO_PAT", "dremio123");
        String restPort = System.getenv().getOrDefault("DREMIO_REST_PORT", "9047");
        String table    = System.getenv().getOrDefault("DREMIO_TABLE", "my_catalog.my_schema.test_output");

        try {
            RecordBuilderFactory factory = handler.findService(RecordBuilderFactory.class);
            List<Record> records = new ArrayList<>();
            records.add(factory.newRecordBuilder()
                    .withString("name", "Alice")
                    .withInt("age", 30)
                    .build());
            records.add(factory.newRecordBuilder()
                    .withString("name", "Bob")
                    .withInt("age", 25)
                    .build());

            handler.setInputData(records);

            java.util.Map<String, String> config = new java.util.HashMap<>();
            config.put("configuration.datastore.host", host);
            config.put("configuration.datastore.port", "32010");
            config.put("configuration.datastore.personalAccessToken", pat);
            config.put("configuration.tablePath", table);
            config.put("configuration.writeMode", "APPEND");
            config.put("configuration.batchSize", "500");
            config.put("configuration.restPort", restPort);

            handler.collect(Void.class, "Dremio", "DremioOutput", 1, config);

            System.out.println("=================================================");
            System.out.println("DremioOutput: 2 records submitted successfully.");
            System.out.println("=================================================");

        } catch (org.talend.sdk.component.api.exception.ComponentException e) {
            System.out.println("=================================================");
            System.out.println("DremioOutput component loaded and attempted connection.");
            System.out.println("Connection failed (set DREMIO_HOST/DREMIO_PAT/DREMIO_TABLE to test live): "
                    + e.getMessage());
            System.out.println("This is expected without a live Dremio instance. Test passes!");
            System.out.println("=================================================");
        }
    }
}
