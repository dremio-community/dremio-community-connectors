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

import com.dremio.exec.record.BatchSchema;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public enum JiraTableDef {

    ISSUES("issues"),
    PROJECTS("projects"),
    USERS("users"),
    BOARDS("boards"),
    PRIORITIES("priorities"),
    ISSUE_TYPES("issue_types"),
    STATUSES("statuses"),
    FIELDS("fields"),
    COMPONENTS("components"),
    VERSIONS("versions");

    public final String tableName;

    JiraTableDef(String tableName) {
        this.tableName = tableName;
    }

    public static final List<String> ALL_TABLES;
    static {
        List<String> names = new ArrayList<>();
        for (JiraTableDef t : values()) names.add(t.tableName);
        ALL_TABLES = Collections.unmodifiableList(names);
    }

    public static Optional<JiraTableDef> fromName(String name) {
        for (JiraTableDef t : values()) {
            if (t.tableName.equals(name)) return Optional.of(t);
        }
        return Optional.empty();
    }

    public BatchSchema getSchema() {
        switch (this) {
            case ISSUES:      return issuesSchema();
            case PROJECTS:    return projectsSchema();
            case USERS:       return usersSchema();
            case BOARDS:      return boardsSchema();
            case PRIORITIES:  return prioritiesSchema();
            case ISSUE_TYPES: return issueTypesSchema();
            case STATUSES:    return statusesSchema();
            case FIELDS:      return fieldsSchema();
            case COMPONENTS:  return componentsSchema();
            case VERSIONS:    return versionsSchema();
            default: throw new IllegalArgumentException("Unknown table: " + tableName);
        }
    }

    /** true = table uses offset pagination; false = single-call (small lookup or multi-project). */
    public boolean isPaginated() {
        switch (this) {
            case ISSUES: case PROJECTS: case USERS: case BOARDS: return true;
            default: return false;
        }
    }

    // -------------------------------------------------------------------------
    // Schema definitions
    // -------------------------------------------------------------------------

    private static BatchSchema issuesSchema() {
        return schema(
            utf8("id"), utf8("key"), utf8("summary"), utf8("description"),
            utf8("status"), utf8("status_category"), utf8("issue_type"), utf8("priority"),
            utf8("assignee_display_name"), utf8("assignee_account_id"),
            utf8("reporter_display_name"), utf8("reporter_account_id"),
            utf8("created"), utf8("updated"), utf8("resolved"), utf8("due_date"),
            utf8("project_key"), utf8("project_name"),
            utf8("labels"), utf8("components"), utf8("fix_versions"), utf8("parent_key"),
            float8("story_points"), float8("time_original_estimate"),
            float8("time_spent"), float8("comment_count")
        );
    }

    private static BatchSchema projectsSchema() {
        return schema(
            utf8("id"), utf8("key"), utf8("name"), utf8("description"),
            utf8("project_type_key"), utf8("style"), bool8("is_private")
        );
    }

    private static BatchSchema usersSchema() {
        return schema(
            utf8("account_id"), utf8("display_name"), utf8("email_address"),
            bool8("active"), utf8("account_type")
        );
    }

    private static BatchSchema boardsSchema() {
        return schema(
            float8("id"), utf8("name"), utf8("type"),
            utf8("project_key"), utf8("project_name")
        );
    }

    private static BatchSchema prioritiesSchema() {
        return schema(
            utf8("id"), utf8("name"), utf8("description"),
            utf8("status_color"), utf8("icon_url")
        );
    }

    private static BatchSchema issueTypesSchema() {
        return schema(
            utf8("id"), utf8("name"), utf8("description"),
            bool8("subtask"), float8("hierarchy_level")
        );
    }

    private static BatchSchema statusesSchema() {
        return schema(
            utf8("id"), utf8("name"), utf8("description"),
            utf8("status_category"), utf8("status_category_key")
        );
    }

    private static BatchSchema fieldsSchema() {
        return schema(
            utf8("id"), utf8("name"), bool8("custom"),
            bool8("orderable"), bool8("navigable"), bool8("searchable"),
            utf8("schema_type"), utf8("clause_names")
        );
    }

    private static BatchSchema componentsSchema() {
        return schema(
            utf8("id"), utf8("name"), utf8("description"), utf8("project_key")
        );
    }

    private static BatchSchema versionsSchema() {
        return schema(
            utf8("id"), utf8("name"), utf8("description"), utf8("project_key"),
            bool8("released"), bool8("archived"), utf8("release_date")
        );
    }

    // -------------------------------------------------------------------------
    // Arrow field helpers
    // -------------------------------------------------------------------------

    private static BatchSchema schema(Field... fields) {
        return BatchSchema.newBuilder().addFields(Arrays.asList(fields)).build();
    }

    static Field utf8(String name) {
        return new Field(name, new FieldType(true, ArrowType.Utf8.INSTANCE, null), Collections.emptyList());
    }

    static Field float8(String name) {
        return new Field(name, new FieldType(true,
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), Collections.emptyList());
    }

    static Field bool8(String name) {
        return new Field(name, new FieldType(true, ArrowType.Bool.INSTANCE, null), Collections.emptyList());
    }
}
