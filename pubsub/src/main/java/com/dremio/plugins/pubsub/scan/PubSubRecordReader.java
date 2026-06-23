package com.dremio.plugins.pubsub.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.pubsub.PubSubClient;
import com.dremio.plugins.pubsub.PubSubClient.PubSubMessage;
import com.dremio.plugins.pubsub.PubSubStoragePlugin;
import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads messages from a Pub/Sub subscription and writes them into Dremio Arrow vectors.
 *
 * Design principles:
 * - Pull-without-ack: messages are pulled in batches, written as rows, then NACKed.
 *   This preserves messages for other consumers and allows repeated queries.
 * - Bounded scan: pulls until maxMessages total are collected or the subscription
 *   returns empty twice in a row (no more pending messages).
 * - Per-batch writes: each next() call returns up to TARGET_BATCH_SIZE records.
 *
 * Data flow:
 *   setup()  → pull all messages up to maxMessages, NACK them, buffer locally
 *   next()   → drain buffer into Arrow vectors, return row count (0 when done)
 *   close()  → nothing (messages were already NACKed in setup)
 */
public class PubSubRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(PubSubRecordReader.class);
  private static final int TARGET_BATCH_SIZE = 4_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final PubSubStoragePlugin plugin;
  private final PubSubSubScan       subScan;
  private final PubSubScanSpec      spec;

  // Buffered messages pulled during setup()
  private List<PubSubMessage> buffer;
  private int bufferPos = 0;

  // Metadata vectors (always present)
  private VarCharVector subscriptionVec;
  private VarCharVector messageIdVec;
  private BigIntVector  publishTimeVec;
  private VarCharVector orderingKeyVec;
  private VarCharVector attributesVec;
  private VarCharVector valueRawVec;

  // Payload vectors (JSON mode; keyed by field name)
  private List<PayloadField> payloadFields;

  private static class PayloadField {
    final String    name;
    final ArrowType arrowType;
    final ValueVector vector;

    PayloadField(String name, ArrowType arrowType, ValueVector vector) {
      this.name      = name;
      this.arrowType = arrowType;
      this.vector    = vector;
    }
  }

  public PubSubRecordReader(PubSubStoragePlugin plugin,
                             PubSubSubScan subScan,
                             OperatorContext context,
                             PubSubScanSpec spec) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
    this.spec    = spec;
  }

  // -----------------------------------------------------------------------
  // RecordReader lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    payloadFields = new ArrayList<>();
    allocateVectors(output);
    buffer = pullAllMessages();
    logger.debug("PubSubRecordReader: subscription={} pulled {} messages",
        spec.getSubscription(), buffer.size());
  }

  /**
   * Binds Arrow vectors using only vectors pre-materialized by ScanOperator.
   * Never calls output.addField() — that would cause SCHEMA_CHANGE.
   */
  private void allocateVectors(OutputMutator output) throws ExecutionSetupException {
    for (Field field : subScan.getFullSchema().getFields()) {
      String name = field.getName();
      ValueVector vec = output.getVector(name);
      if (vec == null) continue;

      ArrowType type = field.getType();
      if      (PubSubStoragePlugin.COL_SUBSCRIPTION.equals(name)) subscriptionVec = (VarCharVector) vec;
      else if (PubSubStoragePlugin.COL_MESSAGE_ID.equals(name))   messageIdVec    = (VarCharVector) vec;
      else if (PubSubStoragePlugin.COL_PUBLISH_TIME.equals(name)) publishTimeVec  = (BigIntVector)  vec;
      else if (PubSubStoragePlugin.COL_ORDERING_KEY.equals(name)) orderingKeyVec  = (VarCharVector) vec;
      else if (PubSubStoragePlugin.COL_ATTRIBUTES.equals(name))   attributesVec   = (VarCharVector) vec;
      else if (PubSubStoragePlugin.COL_VALUE_RAW.equals(name))    valueRawVec     = (VarCharVector) vec;
      else payloadFields.add(new PayloadField(name, type, vec));
    }
  }

  @Override
  public int next() {
    if (bufferPos >= buffer.size()) return 0;

    int rowCount = 0;
    while (rowCount < TARGET_BATCH_SIZE && bufferPos < buffer.size()) {
      writeMessage(buffer.get(bufferPos), rowCount);
      bufferPos++;
      rowCount++;
    }
    return rowCount;
  }

  @Override
  public void close() throws Exception {
    // Messages were NACKed during setup — nothing to do here
  }

  // -----------------------------------------------------------------------
  // Message pulling
  // -----------------------------------------------------------------------

  /**
   * Pulls messages from the subscription until maxMessages total are collected
   * or two consecutive empty pulls indicate no more pending messages.
   * All pulled messages are NACKed immediately so they stay in the subscription.
   */
  private List<PubSubMessage> pullAllMessages() {
    PubSubClient client = plugin.getClient();
    String sub          = spec.getSubscription();
    int    maxMessages  = spec.getMaxMessages();
    int    timeout      = plugin.getConfig().pullTimeoutSeconds;

    List<PubSubMessage> all      = new ArrayList<>();
    List<String>        allAcks  = new ArrayList<>();
    int emptyCount = 0;
    long deadline  = System.currentTimeMillis() + (timeout * 1000L);

    while (all.size() < maxMessages && System.currentTimeMillis() < deadline) {
      int batchSize = Math.min(1000, maxMessages - all.size());
      try {
        List<PubSubMessage> batch = client.pull(sub, batchSize);
        if (batch.isEmpty()) {
          emptyCount++;
          if (emptyCount >= 2) break; // subscription is drained
          continue;
        }
        emptyCount = 0;
        all.addAll(batch);
        allAcks.addAll(batch.stream().map(m -> m.ackId).collect(Collectors.toList()));
      } catch (Exception e) {
        logger.warn("[{}] Pull error on '{}': {}", plugin.getConfig().projectId, sub, e.getMessage());
        break;
      }
    }

    // NACK all: set ack deadline to 0 so Pub/Sub redelivers immediately
    if (!allAcks.isEmpty()) {
      try {
        // NACK in chunks of 1000 (API limit)
        for (int i = 0; i < allAcks.size(); i += 1000) {
          client.nack(sub, allAcks.subList(i, Math.min(i + 1000, allAcks.size())));
        }
      } catch (Exception e) {
        logger.warn("[{}] NACK failed for '{}': {}", plugin.getConfig().projectId, sub, e.getMessage());
      }
    }

    return all;
  }

  // -----------------------------------------------------------------------
  // Record writing
  // -----------------------------------------------------------------------

  private void writeMessage(PubSubMessage msg, int idx) {
    writeVarChar(subscriptionVec, spec.getSubscription(), idx);
    writeVarChar(messageIdVec,    msg.messageId, idx);
    if (publishTimeVec != null) publishTimeVec.setSafe(idx, msg.publishTimeMs);
    writeVarChar(orderingKeyVec,  msg.orderingKey, idx);
    writeVarChar(attributesVec,   encodeAttributes(msg.attributes), idx);
    writeVarChar(valueRawVec,     msg.dataAsUtf8(), idx);

    if (!payloadFields.isEmpty() && "JSON".equalsIgnoreCase(spec.getSchemaMode())) {
      try {
        JsonNode root = MAPPER.readTree(msg.dataAsUtf8());
        if (root != null && root.isObject()) {
          for (PayloadField pf : payloadFields) {
            JsonNode node = root.get(pf.name);
            if (node != null && !node.isNull() && !node.isMissingNode()) {
              writeJsonField(pf, node, idx);
            }
          }
        }
      } catch (Exception ignore) { }
    }
  }

  private void writeVarChar(VarCharVector vec, String value, int idx) {
    if (vec == null || value == null) return;
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    vec.setSafe(idx, bytes, 0, bytes.length);
  }

  private void writeJsonField(PayloadField pf, JsonNode node, int idx) {
    try {
      ArrowType type = pf.arrowType;
      if (type instanceof ArrowType.Bool) {
        ((BitVector) pf.vector).setSafe(idx, node.asBoolean() ? 1 : 0);
      } else if (type instanceof ArrowType.Int) {
        ((org.apache.arrow.vector.BigIntVector) pf.vector).setSafe(idx, node.asLong());
      } else if (type instanceof ArrowType.FloatingPoint) {
        ((Float8Vector) pf.vector).setSafe(idx, node.asDouble());
      } else {
        byte[] bytes = node.asText().getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) pf.vector).setSafe(idx, bytes, 0, bytes.length);
      }
    } catch (Exception ignore) { }
  }

  /**
   * Encodes Pub/Sub message attributes as a flat JSON object: {"key": "value", ...}
   * Queryable via CONVERT_FROM(_attributes, 'JSON')['key-name']
   */
  private String encodeAttributes(Map<String, String> attrs) {
    if (attrs == null || attrs.isEmpty()) return "{}";
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> e : attrs.entrySet()) {
      if (!first) sb.append(",");
      first = false;
      appendJsonString(sb, e.getKey());
      sb.append(":");
      appendJsonString(sb, e.getValue());
    }
    sb.append("}");
    return sb.toString();
  }

  private void appendJsonString(StringBuilder sb, String s) {
    if (s == null) { sb.append("null"); return; }
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if      (c == '"')  sb.append("\\\"");
      else if (c == '\\') sb.append("\\\\");
      else if (c == '\n') sb.append("\\n");
      else if (c == '\r') sb.append("\\r");
      else if (c == '\t') sb.append("\\t");
      else                sb.append(c);
    }
    sb.append('"');
  }
}
