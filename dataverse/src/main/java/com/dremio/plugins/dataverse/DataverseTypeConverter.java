/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.dataverse.DataverseConnection.DataverseField;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Maps Dataverse attribute types to Apache Arrow types used by Dremio.
 */
public final class DataverseTypeConverter {

    private DataverseTypeConverter() {}

    public static Field toField(DataverseField df) {
        ArrowType arrowType = toArrowType(df.attributeType);
        FieldType fieldType = new FieldType(true, arrowType, null);
        return new Field(df.logicalName, fieldType, Collections.emptyList());
    }

    public static BatchSchema toBatchSchema(List<DataverseField> fields) {
        List<Field> arrowFields = new ArrayList<>(fields.size());
        for (DataverseField df : fields) {
            arrowFields.add(toField(df));
        }
        return BatchSchema.newBuilder().addFields(arrowFields).build();
    }

    /**
     * Maps a Dataverse AttributeType string to the corresponding Arrow type.
     *
     * <p>Dataverse attribute types: https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/reference/attributetypecode
     */
    public static ArrowType toArrowType(String attrType) {
        if (attrType == null) return ArrowType.Utf8.INSTANCE;

        switch (attrType) {
            // Text types
            case "String":
            case "Memo":
            case "EntityName":
            case "Lookup":
            case "Owner":
            case "Customer":
            case "PartyList":
            case "UniqueIdentifier":
            case "File":
            case "Image":
                return ArrowType.Utf8.INSTANCE;

            // Integer types
            case "Integer":
            case "Picklist":
            case "State":
            case "Status":
                return new ArrowType.Int(32, true);

            case "BigInt":
                return new ArrowType.Int(64, true);

            // Floating point types
            case "Double":
            case "Money":
            case "Decimal":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

            // Boolean
            case "Boolean":
                return ArrowType.Bool.INSTANCE;

            // Date/time
            case "DateTime":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");

            case "DateOnly":
                return new ArrowType.Date(DateUnit.DAY);

            // Fallback: stringify
            default:
                return ArrowType.Utf8.INSTANCE;
        }
    }
}
