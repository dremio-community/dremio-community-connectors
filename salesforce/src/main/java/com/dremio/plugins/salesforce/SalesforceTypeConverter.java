/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

import com.dremio.exec.record.BatchSchema;
import com.dremio.plugins.salesforce.SalesforceConnection.SalesforceField;
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
 * Maps Salesforce field types to Apache Arrow types used by Dremio.
 */
public final class SalesforceTypeConverter {

    private SalesforceTypeConverter() {
        // Utility class
    }

    /**
     * Converts a single Salesforce field descriptor to an Arrow Field.
     *
     * @param sf the Salesforce field description
     * @return Arrow Field suitable for use in a BatchSchema
     */
    public static Field toField(SalesforceField sf) {
        ArrowType arrowType = toArrowType(sf.type);
        // All Salesforce fields are nullable (nillable), including Ids in practice
        FieldType fieldType = new FieldType(true, arrowType, null);
        return new Field(sf.name, fieldType, Collections.emptyList());
    }

    /**
     * Builds a BatchSchema from a list of Salesforce fields.
     *
     * @param fields list of Salesforce field descriptors
     * @return Dremio BatchSchema
     */
    public static BatchSchema toBatchSchema(List<SalesforceField> fields) {
        List<Field> arrowFields = new ArrayList<>(fields.size());
        for (SalesforceField sf : fields) {
            arrowFields.add(toField(sf));
        }
        return BatchSchema.newBuilder().addFields(arrowFields).build();
    }

    /**
     * Maps a Salesforce field type string to the corresponding Arrow type.
     */
    public static ArrowType toArrowType(String sfType) {
        if (sfType == null) {
            return ArrowType.Utf8.INSTANCE;
        }
        switch (sfType.toLowerCase()) {
            // String-like types
            case "id":
            case "string":
            case "textarea":
            case "url":
            case "email":
            case "phone":
            case "picklist":
            case "multipicklist":
            case "combobox":
            case "reference":
            case "anytype":
            case "encryptedstring":
                return ArrowType.Utf8.INSTANCE;

            // Boolean
            case "boolean":
                return ArrowType.Bool.INSTANCE;

            // Integer types
            case "int":
                return new ArrowType.Int(32, true);

            case "long":
                return new ArrowType.Int(64, true);

            // Floating point types
            case "double":
            case "currency":
            case "percent":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

            // Date / time types
            case "date":
                return new ArrowType.Date(DateUnit.DAY);

            case "datetime":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");

            case "time":
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);

            // Binary
            case "base64":
                return ArrowType.Binary.INSTANCE;

            // Complex / compound types — stringify as JSON
            case "address":
            case "location":
                return ArrowType.Utf8.INSTANCE;

            // Everything else — treat as string
            default:
                return ArrowType.Utf8.INSTANCE;
        }
    }

    /**
     * Returns true if a Salesforce type maps to a string/Utf8 Arrow type.
     * Used by the record reader to decide how to write values.
     */
    public static boolean isStringType(String sfType) {
        if (sfType == null) return true;
        switch (sfType.toLowerCase()) {
            case "id": case "string": case "textarea": case "url":
            case "email": case "phone": case "picklist": case "multipicklist":
            case "combobox": case "reference": case "anytype": case "encryptedstring":
            case "address": case "location":
                return true;
            default:
                return false;
        }
    }
}
