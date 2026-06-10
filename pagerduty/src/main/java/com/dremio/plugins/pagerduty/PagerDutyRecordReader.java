package com.dremio.plugins.pagerduty;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.pagerduty.PagerDutyConnection.PagerDutyField;
import com.dremio.plugins.pagerduty.PagerDutyConnection.PagerDutyPage;
import com.dremio.plugins.pagerduty.PagerDutyConnection.PagerDutyTable;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reads records from PagerDuty REST API pages into Arrow vectors.
 *
 * <p>Handles offset-based pagination transparently across next() calls.
 * Supports path-based JSON extraction including indexing and wildcards.
 */
public class PagerDutyRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(PagerDutyRecordReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int BATCH_SIZE = 100;

    private final PagerDutyConnection connection;
    private final PagerDutyScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final PagerDutyTable table;

    // Projected fields + vectors (populated in setup)
    private List<PagerDutyField> projectedFields;
    private List<ValueVector> vectors;

    // Pagination state
    private PagerDutyPage currentPage;
    private int pageOffset;
    private boolean exhausted;
    private boolean firstCall;
    private String nextUrl;

    public PagerDutyRecordReader(
            OperatorContext context,
            PagerDutyConnection connection,
            PagerDutyScanSpec spec,
            com.dremio.exec.record.BatchSchema schema) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
        this.table = PagerDutyConnection.getTable(spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // AbstractRecordReader overrides
    // -------------------------------------------------------------------------

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedFields = new ArrayList<>();
        vectors = new ArrayList<>();

        // Build a name→PagerDutyField map for fast lookup
        Map<String, PagerDutyField> pdFieldByName = new java.util.LinkedHashMap<>();
        for (PagerDutyField f : table.fields) {
            pdFieldByName.put(f.name, f);
        }

        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            PagerDutyField pdf = pdFieldByName.get(field.getName());
            if (pdf == null) continue;
            projectedFields.add(pdf);
            vectors.add(v);
        }

        exhausted = false;
        firstCall = true;
        pageOffset = 0;
        currentPage = null;
        nextUrl = table.endpoint;

        logger.debug("PagerDutyRecordReader setup: table={}", spec.getTableName());
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            if (firstCall) {
                firstCall = false;
                logger.debug("Fetching first page: {}", nextUrl);
                currentPage = connection.fetchPage(nextUrl);
                pageOffset = 0;
                nextUrl = currentPage.nextUrl;
            }

            // Advance to next page if current is consumed
            if (currentPage == null || pageOffset >= currentPage.records.size()) {
                if (!currentPage.hasMore || nextUrl == null) {
                    exhausted = true;
                    return 0;
                }
                logger.debug("Fetching next page: {}", nextUrl);
                currentPage = connection.fetchPage(nextUrl);
                pageOffset = 0;
                nextUrl = currentPage.nextUrl;
            }

            if (currentPage.records == null || currentPage.records.isEmpty()) {
                exhausted = true;
                return 0;
            }

            int batchSize = Math.min(BATCH_SIZE, currentPage.records.size() - pageOffset);
            if (batchSize <= 0) {
                exhausted = true;
                return 0;
            }

            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                Map<String, Object> record = currentPage.records.get(pageOffset + i);
                writeRecord(record, count);
                count++;
            }
            pageOffset += batchSize;

            for (ValueVector v : vectors) {
                v.setValueCount(count);
            }

            if (pageOffset >= currentPage.records.size() && !currentPage.hasMore) {
                exhausted = true;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from PagerDuty", e);
            throw new RuntimeException("Failed to read from PagerDuty: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("PagerDutyRecordReader closed for table '{}'", spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(Map<String, Object> record, int idx) {
        for (int i = 0; i < projectedFields.size(); i++) {
            PagerDutyField pdf = projectedFields.get(i);
            ValueVector vector = vectors.get(i);
            Object value = extractValue(record, pdf.jsonPath);
            if (value == null) continue;
            try {
                writeValue(vector, pdf.type, value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}", pdf.name, value, e.getMessage());
            }
        }
    }

    /**
     * Extracts a value from a record using a dot-notation path, with support
     * for array indices (e.g. assignments[0].assignee.id) and wildcards
     * (e.g. teams[*].id) which are returned as comma-separated strings.
     */
    @SuppressWarnings("unchecked")
    private Object extractValue(Map<String, Object> record, String path) {
        if (path == null || path.isEmpty()) return null;

        Object current = record;
        String[] parts = path.split("\\.");

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (current == null) return null;

            // Check for wildcards like teams[*].id
            if (part.contains("[*]")) {
                String key = part.substring(0, part.indexOf("[*]"));
                if (!(current instanceof Map)) return null;
                Object listObj = ((Map<String, Object>) current).get(key);
                if (!(listObj instanceof List)) return null;
                List<?> list = (List<?>) listObj;

                // If it is the last part, just return string list representation
                // Else, map all items in the list to the remaining path
                if (i == parts.length - 1) {
                    return joinList(list, null);
                } else {
                    // Build the remaining path
                    StringBuilder remaining = new StringBuilder();
                    for (int j = i + 1; j < parts.length; j++) {
                        if (remaining.length() > 0) remaining.append(".");
                        remaining.append(parts[j]);
                    }
                    List<Object> results = new ArrayList<>();
                    for (Object item : list) {
                        if (item instanceof Map) {
                            Object val = extractValue((Map<String, Object>) item, remaining.toString());
                            if (val != null) {
                                results.add(val);
                            }
                        }
                    }
                    return joinList(results, null);
                }
            } else if (part.contains("[") && part.endsWith("]")) {
                int open = part.indexOf("[");
                String key = part.substring(0, open);
                String idxStr = part.substring(open + 1, part.length() - 1);
                int idx = Integer.parseInt(idxStr);

                if (!(current instanceof Map)) return null;
                Object listObj = ((Map<String, Object>) current).get(key);
                if (!(listObj instanceof List)) return null;
                List<?> list = (List<?>) listObj;
                if (idx < 0 || idx >= list.size()) return null;
                current = list.get(idx);
            } else {
                if (!(current instanceof Map)) return null;
                current = ((Map<String, Object>) current).get(part);
            }
        }
        return current;
    }

    private String joinList(List<?> list, String delimiter) {
        if (list == null || list.isEmpty()) return "";
        String delim = delimiter != null ? delimiter : ",";
        StringBuilder sb = new StringBuilder();
        for (Object item : list) {
            if (item == null) continue;
            if (sb.length() > 0) sb.append(delim);
            sb.append(item.toString());
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private void writeValue(ValueVector vector, ArrowType type, Object value, int idx) {
        if (type instanceof ArrowType.Utf8) {
            String str;
            if (value instanceof List || value instanceof Map) {
                try { str = MAPPER.writeValueAsString(value); } catch (Exception e) { str = value.toString(); }
            } else {
                str = value.toString();
            }
            ((VarCharVector) vector).setSafe(idx, str.getBytes(StandardCharsets.UTF_8));

        } else if (type instanceof ArrowType.Bool) {
            boolean b = (value instanceof Boolean) ? (Boolean) value : Boolean.parseBoolean(value.toString());
            ((BitVector) vector).setSafe(idx, b ? 1 : 0);

        } else if (type instanceof ArrowType.Int) {
            int bitWidth = ((ArrowType.Int) type).getBitWidth();
            if (bitWidth == 32) {
                int v = (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                ((IntVector) vector).setSafe(idx, v);
            } else {
                long v = (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
                ((BigIntVector) vector).setSafe(idx, v);
            }

        } else if (type instanceof ArrowType.FloatingPoint) {
            double d = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
            ((Float8Vector) vector).setSafe(idx, d);

        } else if (type instanceof ArrowType.Timestamp) {
            String s = value.toString();
            Instant instant;
            try {
                instant = Instant.parse(s);
            } catch (Exception e) {
                try {
                    instant = OffsetDateTime.parse(s).toInstant();
                } catch (Exception e2) {
                    logger.warn("Cannot parse timestamp '{}': {}", s, e2.getMessage());
                    return;
                }
            }
            ((TimeStampMilliVector) vector).setSafe(idx, instant.toEpochMilli());

        } else {
            // Fallback: write as string
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
