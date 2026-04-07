package com.dremio.plugins.splunk.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.splunk.SplunkClient;
import com.dremio.plugins.splunk.SplunkSchemaInferrer;
import com.dremio.plugins.splunk.SplunkStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Reads Splunk search results and writes them into Arrow vectors.
 *
 * Execution flow:
 *   setup()  — build SPL, create async Splunk search job, wait for completion
 *   next()   — page through job results, write events into Arrow vectors
 *   close()  — cancel/delete the Splunk job (cleanup)
 *
 * Schema handling:
 *   Only writes fields that are in the declared schema (from SplunkStoragePlugin).
 *   Extra fields returned by Splunk but not in the schema are silently ignored.
 *   Missing fields (not present in a given event) write null.
 *
 * _time handling:
 *   Parsed from ISO-8601 string (e.g. "2024-01-15T10:23:45.000+00:00") into
 *   epoch milliseconds, written into a TimeStampMilliVector.
 *
 * Blocking mode:
 *   When maxEvents ≤ BLOCKING_THRESHOLD, uses client.runSearch() with
 *   exec_mode=blocking. The POST waits for job completion and results are
 *   returned in one round-trip, cached in-memory, and served from next().
 *   No separate create-job → poll → paginate flow is needed for small scans.
 *
 * IMPORTANT: Never call addField() on the OutputMutator — vectors must be looked up
 * by name only. Adding fields causes schema corruption in Dremio.
 */
public class SplunkRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(SplunkRecordReader.class);

  private static final int MAX_VARCHAR_BYTES = 65536; // 64KB max per cell

  /**
   * Maximum event count for which we use exec_mode=blocking (one-shot POST).
   * Queries with maxEvents above this threshold use the async create-job/poll/fetch flow.
   */
  private static final int BLOCKING_THRESHOLD = 1_000;

  private final SplunkStoragePlugin plugin;
  private final SplunkSubScan       subScan;
  private final SplunkScanSpec      scanSpec;

  // Execution state
  private SplunkClient    client;
  private String          jobSid;          // Splunk search job SID (null in blocking mode)
  private int             cursor;          // current results offset (streaming mode)
  private int             totalResults;    // total result count from Splunk (streaming mode)
  private boolean         done;

  // Blocking mode: all results cached up-front; null means streaming mode
  private List<JsonNode>  cachedResults  = null;
  private int             cachedOffset   = 0;

  // Bound vectors: column name → (vector, arrowType)
  private final Map<String, BoundColumn> boundColumns = new LinkedHashMap<>();

  private static class BoundColumn {
    final ValueVector vector;
    final ArrowType   type;
    BoundColumn(ValueVector vector, ArrowType type) {
      this.vector = vector;
      this.type   = type;
    }
  }

  public SplunkRecordReader(SplunkStoragePlugin plugin,
                             SplunkSubScan subScan,
                             OperatorContext context,
                             SplunkScanSpec scanSpec) {
    super(context, subScan.getColumns());
    this.plugin   = plugin;
    this.subScan  = subScan;
    this.scanSpec = scanSpec;
  }

  // -----------------------------------------------------------------------
  // AbstractRecordReader lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.client = plugin.getClient();

    // Bind vectors for every field in the schema.
    // NEVER call addField() — only look up existing vectors.
    List<Field> schemaFields = subScan.getFullSchema().getFields();
    for (Field field : schemaFields) {
      ValueVector vec = output.getVector(field.getName());
      if (vec != null) {
        boundColumns.put(field.getName(), new BoundColumn(vec, field.getType()));
      }
    }

    // Create the Splunk search job (or use blocking mode for small result sets)
    String spl = scanSpec.toSpl();
    int    maxEvents = scanSpec.getMaxEvents();
    logger.debug("SplunkRecordReader starting search: {} [earliest={}, latest={}, maxEvents={}]",
        spl, scanSpec.effectiveEarliest(), scanSpec.effectiveLatest(), maxEvents);

    try {
      if (maxEvents > 0 && maxEvents <= BLOCKING_THRESHOLD) {
        // Blocking mode: one POST, results returned immediately — no poll loop needed
        cachedResults = client.runSearch(spl,
            scanSpec.effectiveEarliest(), scanSpec.effectiveLatest(), maxEvents);
        cachedOffset  = 0;
        logger.debug("SplunkRecordReader blocking mode: {} results cached", cachedResults.size());
      } else {
        // Async mode: create job, wait for completion, page through results
        jobSid = client.createSearchJob(spl,
            scanSpec.effectiveEarliest(), scanSpec.effectiveLatest(), maxEvents);
        client.waitForJob(jobSid);
        totalResults = client.getJobResultCount(jobSid);
        logger.debug("Splunk job {} ready, {} results", jobSid, totalResults);
      }
    } catch (Exception e) {
      throw new ExecutionSetupException("Failed to execute Splunk search: " + spl, e);
    }

    this.cursor = 0;
    this.done   = (cachedResults != null ? cachedResults.isEmpty() : totalResults == 0);
  }

  @Override
  public int next() {
    if (done) return 0;

    int pageSize = plugin.getConfig().resultsPageSize;
    List<JsonNode> events;

    if (cachedResults != null) {
      // Blocking mode: slice from in-memory list
      int remaining = cachedResults.size() - cachedOffset;
      if (remaining <= 0) {
        done = true;
        return 0;
      }
      int end = Math.min(cachedOffset + pageSize, cachedResults.size());
      events = cachedResults.subList(cachedOffset, end);
      cachedOffset = end;
      if (cachedOffset >= cachedResults.size()) done = true;
    } else {
      // Async mode: fetch next page from Splunk
      try {
        events = client.fetchResultsPage(jobSid, cursor, pageSize);
      } catch (Exception e) {
        throw UserException.dataReadError(e)
            .message("Failed to fetch Splunk results page for job %s at offset %d: %s",
                jobSid, cursor, e.getMessage())
            .build(logger);
      }

      if (events.isEmpty()) {
        done = true;
        return 0;
      }

      cursor += events.size();
      if (cursor >= totalResults || events.size() < pageSize) {
        done = true;
      }
    }

    if (events.isEmpty()) {
      done = true;
      return 0;
    }

    int count = events.size();

    // Allocate vectors for this batch
    for (BoundColumn bc : boundColumns.values()) {
      bc.vector.allocateNew();
    }

    // Write each event into vectors
    for (int row = 0; row < count; row++) {
      JsonNode event = events.get(row);
      writeEvent(event, row);
    }

    // Set value counts
    for (BoundColumn bc : boundColumns.values()) {
      bc.vector.setValueCount(count);
    }

    return count;
  }

  @Override
  public void close() throws Exception {
    if (jobSid != null && client != null) {
      client.cancelJob(jobSid);
      jobSid = null;
    }
  }

  // -----------------------------------------------------------------------
  // Event writing
  // -----------------------------------------------------------------------

  private void writeEvent(JsonNode event, int row) {
    for (Map.Entry<String, BoundColumn> entry : boundColumns.entrySet()) {
      String      colName = entry.getKey();
      BoundColumn bc      = entry.getValue();
      String      value   = getEventField(event, colName);

      try {
        writeValue(bc, row, value);
      } catch (Exception e) {
        // Write null on conversion error; don't fail the whole batch
        setNull(bc.vector, row);
        logger.trace("Type conversion error for column '{}' value '{}': {}",
            colName, value, e.getMessage());
      }
    }
  }

  private String getEventField(JsonNode event, String fieldName) {
    // _time is always present; other metadata fields may be present
    JsonNode node = event.get(fieldName);
    if (node == null || node.isNull()) return null;
    return node.asText(null);
  }

  private void writeValue(BoundColumn bc, int row, String value) {
    ArrowType type = bc.type;

    if (type instanceof ArrowType.Timestamp) {
      TimeStampMilliVector v = (TimeStampMilliVector) bc.vector;
      if (value == null) { v.setNull(row); return; }
      long epochMs = SplunkSchemaInferrer.parseTime(value);
      if (epochMs < 0) { v.setNull(row); return; }
      v.setSafe(row, epochMs);

    } else if (type instanceof ArrowType.Utf8) {
      VarCharVector v = (VarCharVector) bc.vector;
      if (value == null) { v.setNull(row); return; }
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length > MAX_VARCHAR_BYTES) {
        bytes = java.util.Arrays.copyOf(bytes, MAX_VARCHAR_BYTES);
      }
      v.setSafe(row, bytes);

    } else if (type instanceof ArrowType.Int) {
      int bitWidth = ((ArrowType.Int) type).getBitWidth();
      if (bitWidth == 64) {
        BigIntVector v = (BigIntVector) bc.vector;
        if (value == null) { v.setNull(row); return; }
        v.setSafe(row, Long.parseLong(value.trim()));
      } else {
        // INT32, etc. — write as BigInt anyway (Dremio upcasts)
        BigIntVector v = (BigIntVector) bc.vector;
        if (value == null) { v.setNull(row); return; }
        v.setSafe(row, Long.parseLong(value.trim()));
      }

    } else if (type instanceof ArrowType.FloatingPoint) {
      Float8Vector v = (Float8Vector) bc.vector;
      if (value == null) { v.setNull(row); return; }
      v.setSafe(row, Double.parseDouble(value.trim()));

    } else if (type instanceof ArrowType.Bool) {
      BitVector v = (BitVector) bc.vector;
      if (value == null) { v.setNull(row); return; }
      v.setSafe(row, Boolean.parseBoolean(value.trim()) ? 1 : 0);

    } else {
      // Unknown type — write as VARCHAR
      VarCharVector v = (VarCharVector) bc.vector;
      if (value == null) { v.setNull(row); return; }
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length > MAX_VARCHAR_BYTES) {
        bytes = java.util.Arrays.copyOf(bytes, MAX_VARCHAR_BYTES);
      }
      v.setSafe(row, bytes);
    }
  }

  private static void setNull(ValueVector vector, int row) {
    if (vector instanceof VarCharVector)          ((VarCharVector)          vector).setNull(row);
    else if (vector instanceof BigIntVector)      ((BigIntVector)           vector).setNull(row);
    else if (vector instanceof Float8Vector)      ((Float8Vector)           vector).setNull(row);
    else if (vector instanceof BitVector)         ((BitVector)              vector).setNull(row);
    else if (vector instanceof TimeStampMilliVector)
                                                  ((TimeStampMilliVector) vector).setNull(row);
  }
}
