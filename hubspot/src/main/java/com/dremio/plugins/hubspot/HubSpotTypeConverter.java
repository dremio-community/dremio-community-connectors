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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class HubSpotTypeConverter {

    public static final List<Field> BUILTIN_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("id",        ArrowType.Utf8.INSTANCE),
            field("createdAt", ArrowType.Utf8.INSTANCE),
            field("updatedAt", ArrowType.Utf8.INSTANCE),
            field("archived",  ArrowType.Bool.INSTANCE)
    ));

    public static final List<Field> OWNER_FIELDS = Collections.unmodifiableList(Arrays.asList(
            field("id",        ArrowType.Utf8.INSTANCE),
            field("email",     ArrowType.Utf8.INSTANCE),
            field("firstName", ArrowType.Utf8.INSTANCE),
            field("lastName",  ArrowType.Utf8.INSTANCE),
            field("userId",    ArrowType.Utf8.INSTANCE),
            field("createdAt", ArrowType.Utf8.INSTANCE),
            field("updatedAt", ArrowType.Utf8.INSTANCE),
            field("archived",  ArrowType.Bool.INSTANCE)
    ));

    private HubSpotTypeConverter() {}

    public static BatchSchema toSchema(List<HubSpotProperty> properties) {
        List<Field> fields = new ArrayList<>(BUILTIN_FIELDS);
        for (HubSpotProperty p : properties) {
            fields.add(field(p.name, toArrowType(p.type)));
        }
        return BatchSchema.newBuilder().addFields(fields).build();
    }

    public static BatchSchema toOwnerSchema() {
        return BatchSchema.newBuilder().addFields(OWNER_FIELDS).build();
    }

    public static ArrowType toArrowType(String hsType) {
        if (hsType == null) return ArrowType.Utf8.INSTANCE;
        switch (hsType) {
            case "number":
            case "currency":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "bool":
                return ArrowType.Bool.INSTANCE;
            default:
                return ArrowType.Utf8.INSTANCE;
        }
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, new FieldType(true, type, null), Collections.emptyList());
    }
}
