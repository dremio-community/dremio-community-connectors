package com.dremio.talend.components.sink;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.dremio.talend.components.dataset.DremioOutputDataSet;
import com.dremio.talend.components.datastore.DremioDataStore;
import com.dremio.talend.components.service.DremioRestClient;

@Version(1)
@Icon(value = Icon.IconType.DB_OUTPUT)
@Processor(name = "DremioOutput")
@Documentation("Writes Talend records into a Dremio Iceberg table via batched INSERT statements over the Dremio REST API.")
public class DremioOutputSink implements Serializable {

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final DremioOutputDataSet configuration;

    private transient DremioRestClient restClient;
    private transient List<Record> buffer;
    private transient boolean overwriteDone;

    public DremioOutputSink(@Option("configuration") final DremioOutputDataSet configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void init() {
        DremioDataStore store = configuration.getDatastore();
        restClient = new DremioRestClient(
                store.getHost(),
                configuration.getRestPort(),
                store.isEnableSsl(),
                store.getPersonalAccessToken());
        buffer = new ArrayList<>();
        overwriteDone = false;
    }

    @ElementListener
    public void onElement(@Input final Record record) {
        if (!overwriteDone
                && configuration.getWriteMode() == DremioOutputDataSet.WriteMode.OVERWRITE) {
            runSql("DELETE FROM " + configuration.getTablePath());
            overwriteDone = true;
        }
        buffer.add(record);
        if (buffer.size() >= configuration.getBatchSize()) {
            flush();
        }
    }

    @PreDestroy
    public void release() {
        flush();
    }

    // -------------------------------------------------------------------------

    private void flush() {
        if (buffer.isEmpty()) return;
        String sql = buildInsert(buffer);
        runSql(sql);
        buffer.clear();
    }

    private void runSql(String sql) {
        try {
            String jobId = restClient.submitSql(sql);
            restClient.pollJobUntilDone(jobId, 500, 300);
        } catch (Exception e) {
            throw new ComponentException(e.getMessage(), e);
        }
    }

    /**
     * Builds an INSERT INTO ... VALUES (...), (...) statement for the buffered records.
     * Column order follows the schema of the first record.
     */
    private String buildInsert(List<Record> records) {
        Record first = records.get(0);
        List<Schema.Entry> entries = first.getSchema().getEntries();

        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(configuration.getTablePath()).append(" (");
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quoteIdentifier(entries.get(i).getName()));
        }
        sb.append(") VALUES ");

        for (int r = 0; r < records.size(); r++) {
            if (r > 0) sb.append(", ");
            sb.append("(");
            Record record = records.get(r);
            for (int c = 0; c < entries.size(); c++) {
                if (c > 0) sb.append(", ");
                sb.append(toSqlLiteral(record, entries.get(c)));
            }
            sb.append(")");
        }
        return sb.toString();
    }

    private String toSqlLiteral(Record record, Schema.Entry entry) {
        String name = entry.getName();
        switch (entry.getType()) {
            case STRING:
                return record.getOptionalString(name)
                        .map(v -> "'" + v.replace("'", "''") + "'")
                        .orElse("NULL");
            case INT:
                try { return String.valueOf(record.getInt(name)); }
                catch (Exception e) { return "NULL"; }
            case LONG:
                try { return String.valueOf(record.getLong(name)); }
                catch (Exception e) { return "NULL"; }
            case FLOAT:
                try { return String.valueOf(record.getFloat(name)); }
                catch (Exception e) { return "NULL"; }
            case DOUBLE:
                try { return String.valueOf(record.getDouble(name)); }
                catch (Exception e) { return "NULL"; }
            case BOOLEAN:
                try { return record.getBoolean(name) ? "TRUE" : "FALSE"; }
                catch (Exception e) { return "NULL"; }
            case DATETIME:
                return record.getOptionalDateTime(name)
                        .map(dt -> "TIMESTAMP '" + dt.format(TS_FMT) + "'")
                        .orElse("NULL");
            case BYTES:
                return record.getOptionalBytes(name)
                        .map(b -> "'" + bytesToHex(b) + "'")
                        .orElse("NULL");
            default:
                return record.getOptionalString(name)
                        .map(v -> "'" + v.replace("'", "''") + "'")
                        .orElse("NULL");
        }
    }

    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
