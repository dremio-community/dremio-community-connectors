/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.jira;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.jira.JiraSubScan.JiraScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JiraRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(JiraRecordReader.class);

    private final JiraConnection connection;
    private final JiraScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final JiraConf conf;

    private List<Field> projectedFields;
    private List<ValueVector> vectors;

    // Pagination state
    private Iterator<JsonNode> currentBatch;
    private int startAt;
    private String issueCursor; // cursor for issues cursor-based pagination
    private boolean exhausted;
    private boolean firstCall;
    // For non-paginated tables, we load everything up front
    private List<JsonNode> allRecords;
    private int allRecordsIdx;

    public JiraRecordReader(OperatorContext context, JiraConnection connection,
            JiraScanSpec spec, com.dremio.exec.record.BatchSchema schema, JiraConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
        this.conf = conf;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedFields = new ArrayList<>(schema.getFieldCount());
        vectors = new ArrayList<>(schema.getFieldCount());
        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            projectedFields.add(field);
            vectors.add(v);
        }
        exhausted = false;
        firstCall = true;
        startAt = 0;
        issueCursor = null;
        allRecords = null;
        allRecordsIdx = 0;
    }

    @Override
    public int next() {
        if (exhausted) return 0;
        try {
            JiraTableDef def = JiraTableDef.fromName(spec.getTableName()).orElse(null);
            if (def == null) { exhausted = true; return 0; }

            if (!def.isPaginated()) {
                return nextFromAll(def);
            } else {
                return nextFromPage(def);
            }
        } catch (IOException e) {
            logger.error("Error reading from Jira table {}", spec.getTableName(), e);
            throw new RuntimeException("Failed to read Jira table " + spec.getTableName() + ": " + e.getMessage(), e);
        }
    }

    private int nextFromAll(JiraTableDef def) throws IOException {
        if (firstCall) {
            firstCall = false;
            allRecords = connection.fetchAll(def.tableName);
            allRecordsIdx = 0;
        }
        if (allRecords == null || allRecordsIdx >= allRecords.size()) {
            exhausted = true;
            return 0;
        }
        int batchSize = Math.min(conf.pageSize, allRecords.size() - allRecordsIdx);
        int count = writeBatch(allRecords, allRecordsIdx, batchSize, def.tableName);
        allRecordsIdx += batchSize;
        if (allRecordsIdx >= allRecords.size()) exhausted = true;
        return count;
    }

    private int nextFromPage(JiraTableDef def) throws IOException {
        if ("issues".equals(def.tableName)) {
            return nextFromIssues();
        }
        if (firstCall) {
            firstCall = false;
            JiraConnection.JiraPage page = connection.fetchPage(def.tableName, 0);
            startAt = page.records.size();
            if (page.isLast || page.records.isEmpty()) exhausted = true;
            return writeBatchFromIterator(page.records, def.tableName);
        }
        if (exhausted) return 0;
        JiraConnection.JiraPage page = connection.fetchPage(def.tableName, startAt);
        startAt += page.records.size();
        if (page.isLast || page.records.isEmpty()) exhausted = true;
        return writeBatchFromIterator(page.records, def.tableName);
    }

    private int nextFromIssues() throws IOException {
        if (!firstCall && exhausted) return 0;
        firstCall = false;
        JiraConnection.JiraPage page = connection.fetchIssuesPage(issueCursor);
        issueCursor = page.nextPageToken;
        if (page.isLast || page.records.isEmpty() || issueCursor == null) exhausted = true;
        return writeBatchFromIterator(page.records, "issues");
    }

    private int writeBatchFromIterator(List<JsonNode> records, String tableName) {
        return writeBatch(records, 0, records.size(), tableName);
    }

    private int writeBatch(List<JsonNode> records, int offset, int size, String tableName) {
        int count = 0;
        for (int i = offset; i < offset + size; i++) {
            writeRecord(records.get(i), tableName, count);
            count++;
        }
        for (ValueVector v : vectors) v.setValueCount(count);
        return count;
    }

    @Override
    public void close() throws Exception {
        logger.debug("JiraRecordReader closed for {}", spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // Field writing
    // -------------------------------------------------------------------------

    private void writeRecord(JsonNode record, String tableName, int idx) {
        switch (tableName) {
            case "issues":      writeIssue(record, idx);     break;
            case "projects":    writeProject(record, idx);   break;
            case "users":       writeUser(record, idx);      break;
            case "boards":      writeBoard(record, idx);     break;
            case "priorities":  writePriority(record, idx);  break;
            case "issue_types": writeIssueType(record, idx); break;
            case "statuses":    writeStatus(record, idx);    break;
            case "fields":      writeField(record, idx);     break;
            case "components":  writeComponent(record, idx); break;
            case "versions":    writeVersion(record, idx);   break;
        }
    }

    private void writeIssue(JsonNode rec, int idx) {
        JsonNode f = rec.path("fields");
        set(idx, "id",                     rec.path("id").asText(null));
        set(idx, "key",                    rec.path("key").asText(null));
        set(idx, "summary",                f.path("summary").asText(null));
        set(idx, "description",            extractAdfText(f.path("description")));
        set(idx, "status",                 f.path("status").path("name").asText(null));
        set(idx, "status_category",        f.path("status").path("statusCategory").path("name").asText(null));
        set(idx, "issue_type",             f.path("issuetype").path("name").asText(null));
        set(idx, "priority",               f.path("priority").path("name").asText(null));
        set(idx, "assignee_display_name",  f.path("assignee").path("displayName").asText(null));
        set(idx, "assignee_account_id",    f.path("assignee").path("accountId").asText(null));
        set(idx, "reporter_display_name",  f.path("reporter").path("displayName").asText(null));
        set(idx, "reporter_account_id",    f.path("reporter").path("accountId").asText(null));
        set(idx, "created",                f.path("created").asText(null));
        set(idx, "updated",                f.path("updated").asText(null));
        set(idx, "resolved",               f.path("resolutiondate").asText(null));
        set(idx, "due_date",               f.path("duedate").asText(null));
        set(idx, "project_key",            f.path("project").path("key").asText(null));
        set(idx, "project_name",           f.path("project").path("name").asText(null));
        set(idx, "labels",                 joinArray(f.path("labels")));
        set(idx, "components",             joinNames(f.path("components")));
        set(idx, "fix_versions",           joinNames(f.path("fixVersions")));
        set(idx, "parent_key",             f.path("parent").path("key").asText(null));
        setFloat(idx, "story_points",      f.path("customfield_10016"));
        setFloat(idx, "time_original_estimate", f.path("timeoriginalestimate"));
        setFloat(idx, "time_spent",        f.path("timespent"));
        setFloat(idx, "comment_count",     f.path("comment").path("total"));
    }

    private void writeProject(JsonNode rec, int idx) {
        set(idx, "id",               rec.path("id").asText(null));
        set(idx, "key",              rec.path("key").asText(null));
        set(idx, "name",             rec.path("name").asText(null));
        set(idx, "description",      rec.path("description").asText(null));
        set(idx, "project_type_key", rec.path("projectTypeKey").asText(null));
        set(idx, "style",            rec.path("style").asText(null));
        setBool(idx, "is_private",   rec.path("isPrivate"));
    }

    private void writeUser(JsonNode rec, int idx) {
        set(idx, "account_id",    rec.path("accountId").asText(null));
        set(idx, "display_name",  rec.path("displayName").asText(null));
        set(idx, "email_address", rec.path("emailAddress").asText(null));
        setBool(idx, "active",    rec.path("active"));
        set(idx, "account_type",  rec.path("accountType").asText(null));
    }

    private void writeBoard(JsonNode rec, int idx) {
        setFloat(idx, "id",           rec.path("id"));
        set(idx, "name",              rec.path("name").asText(null));
        set(idx, "type",              rec.path("type").asText(null));
        set(idx, "project_key",       rec.path("location").path("projectKey").asText(null));
        set(idx, "project_name",      rec.path("location").path("projectName").asText(null));
    }

    private void writePriority(JsonNode rec, int idx) {
        set(idx, "id",           rec.path("id").asText(null));
        set(idx, "name",         rec.path("name").asText(null));
        set(idx, "description",  rec.path("description").asText(null));
        set(idx, "status_color", rec.path("statusColor").asText(null));
        set(idx, "icon_url",     rec.path("iconUrl").asText(null));
    }

    private void writeIssueType(JsonNode rec, int idx) {
        set(idx, "id",          rec.path("id").asText(null));
        set(idx, "name",        rec.path("name").asText(null));
        set(idx, "description", rec.path("description").asText(null));
        setBool(idx, "subtask", rec.path("subtask"));
        setFloat(idx, "hierarchy_level", rec.path("hierarchyLevel"));
    }

    private void writeStatus(JsonNode rec, int idx) {
        set(idx, "id",                   rec.path("id").asText(null));
        set(idx, "name",                 rec.path("name").asText(null));
        set(idx, "description",          rec.path("description").asText(null));
        set(idx, "status_category",      rec.path("statusCategory").path("name").asText(null));
        set(idx, "status_category_key",  rec.path("statusCategory").path("key").asText(null));
    }

    private void writeField(JsonNode rec, int idx) {
        set(idx, "id",           rec.path("id").asText(null));
        set(idx, "name",         rec.path("name").asText(null));
        setBool(idx, "custom",      rec.path("custom"));
        setBool(idx, "orderable",   rec.path("orderable"));
        setBool(idx, "navigable",   rec.path("navigable"));
        setBool(idx, "searchable",  rec.path("searchable"));
        set(idx, "schema_type",  rec.path("schema").path("type").asText(null));
        set(idx, "clause_names", joinArray(rec.path("clauseNames")));
    }

    private void writeComponent(JsonNode rec, int idx) {
        set(idx, "id",          rec.path("id").asText(null));
        set(idx, "name",        rec.path("name").asText(null));
        set(idx, "description", rec.path("description").asText(null));
        set(idx, "project_key", rec.path("projectKey").asText(null));
    }

    private void writeVersion(JsonNode rec, int idx) {
        set(idx, "id",           rec.path("id").asText(null));
        set(idx, "name",         rec.path("name").asText(null));
        set(idx, "description",  rec.path("description").asText(null));
        set(idx, "project_key",  rec.path("projectKey").asText(null));
        setBool(idx, "released",  rec.path("released"));
        setBool(idx, "archived",  rec.path("archived"));
        set(idx, "release_date", rec.path("releaseDate").asText(null));
    }

    // -------------------------------------------------------------------------
    // Vector write helpers
    // -------------------------------------------------------------------------

    private void set(int idx, String name, String value) {
        if (value == null || value.isEmpty()) return;
        int pos = fieldIndex(name);
        if (pos < 0) return;
        ValueVector v = vectors.get(pos);
        if (v instanceof VarCharVector) {
            ((VarCharVector) v).setSafe(idx, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void setFloat(int idx, String name, JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return;
        int pos = fieldIndex(name);
        if (pos < 0) return;
        ValueVector v = vectors.get(pos);
        if (v instanceof Float8Vector) {
            try {
                ((Float8Vector) v).setSafe(idx, node.asDouble());
            } catch (Exception e) {
                // leave unset
            }
        }
    }

    private void setBool(int idx, String name, JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return;
        int pos = fieldIndex(name);
        if (pos < 0) return;
        ValueVector v = vectors.get(pos);
        if (v instanceof BitVector) {
            ((BitVector) v).setSafe(idx, node.asBoolean() ? 1 : 0);
        }
    }

    private int fieldIndex(String name) {
        for (int i = 0; i < projectedFields.size(); i++) {
            if (projectedFields.get(i).getName().equals(name)) return i;
        }
        return -1;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String joinArray(JsonNode arr) {
        if (arr == null || !arr.isArray() || arr.size() == 0) return null;
        StringBuilder sb = new StringBuilder();
        for (JsonNode n : arr) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(n.isTextual() ? n.asText() : n.toString());
        }
        return sb.toString();
    }

    private String joinNames(JsonNode arr) {
        if (arr == null || !arr.isArray() || arr.size() == 0) return null;
        StringBuilder sb = new StringBuilder();
        for (JsonNode n : arr) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(n.path("name").asText(n.toString()));
        }
        return sb.toString();
    }

    /** Extracts plain text from Atlassian Document Format (ADF) or returns raw text. */
    private String extractAdfText(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        if (node.isTextual()) return node.asText();
        if (node.has("content")) {
            StringBuilder sb = new StringBuilder();
            extractAdfContent(node.get("content"), sb);
            String text = sb.toString().trim();
            return text.isEmpty() ? null : text;
        }
        return node.toString();
    }

    private void extractAdfContent(JsonNode content, StringBuilder sb) {
        if (content == null || !content.isArray()) return;
        for (JsonNode block : content) {
            String type = block.path("type").asText("");
            if ("text".equals(type)) {
                sb.append(block.path("text").asText(""));
            } else if ("hardBreak".equals(type) || "paragraph".equals(type)) {
                if (sb.length() > 0) sb.append(" ");
            }
            if (block.has("content")) {
                extractAdfContent(block.get("content"), sb);
            }
        }
    }
}
