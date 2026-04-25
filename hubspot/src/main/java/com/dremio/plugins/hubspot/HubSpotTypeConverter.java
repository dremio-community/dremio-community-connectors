/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.hubspot;

import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.hubspot.HubSpotConnection.HubSpotProperty;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Maps HubSpot property types to Apache Arrow types.
 *
 * <p>HubSpot's built-in fields (id, createdAt, updatedAt, archived) are always
 * prepended to the schema before the dynamic property columns.
 */
public final class HubSpotTypeConverter {

    /** Built-in fields present on every CRM record (except owners which are special). */
    public static final List<Field> BUILTIN_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("id",        ArrowType.Utf8.INSTANCE),
            field("createdAt", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            field("updatedAt", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            field("archived",  ArrowType.Bool.INSTANCE)
    ));

    /** Fixed schema for the owners endpoint (no dynamic properties). */
    public static final List<Field> OWNER_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("id",        ArrowType.Utf8.INSTANCE),
            field("email",     ArrowType.Utf8.INSTANCE),
            field("firstName", ArrowType.Utf8.INSTANCE),
            field("lastName",  ArrowType.Utf8.INSTANCE),
            field("userId",    new ArrowType.Int(64, true)),
            field("createdAt", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            field("updatedAt", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            field("archived",  ArrowType.Bool.INSTANCE)
    ));

    private HubSpotTypeConverter() {}

    /**
     * Builds a BatchSchema for a standard CRM object.
     * Schema = BUILTIN_FIELDS + one field per HubSpot property.
     */
    public static BatchSchema toSchema(List<HubSpotProperty> properties) {
        List<Field> fields = new ArrayList<>(BUILTIN_FIELDS);
        for (HubSpotProperty p : properties) {
            fields.add(field(p.name, toArrowType(p.type)));
        }
        return BatchSchema.newBuilder().addFields(fields).build();
    }

    /** BatchSchema for owners (fixed schema). */
    public static BatchSchema toOwnerSchema() {
        return BatchSchema.newBuilder().addFields(OWNER_FIELDS).build();
    }

    /**
     * Maps a HubSpot property type string to the corresponding Arrow type.
     */
    public static ArrowType toArrowType(String hsType) {
        if (hsType == null) return ArrowType.Utf8.INSTANCE;
        switch (hsType.toLowerCase()) {
            case "number":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "bool":
            case "boolean":
                return ArrowType.Bool.INSTANCE;
            case "date":
                return new ArrowType.Date(DateUnit.DAY);
            case "datetime":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
            // string, enumeration, phone_number, etc. → VARCHAR
            default:
                return ArrowType.Utf8.INSTANCE;
        }
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, new FieldType(true, type, null), Collections.emptyList());
    }
}
