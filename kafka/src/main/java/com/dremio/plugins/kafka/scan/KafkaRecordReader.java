package com.dremio.plugins.kafka.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.kafka.KafkaStoragePlugin;
import com.dremio.plugins.kafka.avro.KafkaAvroConverter;
import com.dremio.plugins.kafka.avro.KafkaSchemaRegistryClient;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads records from a single Kafka partition and writes them into Dremio Arrow vectors.
 *
 * <p>Design principles:
 * <ul>
 *   <li>Manual partition assignment — no consumer-group coordination during scans.</li>
 *   <li>Bounded snapshot — reads exactly [startOffset, endOffset) frozen at plan time.</li>
 *   <li>Per-batch writes — each next() call writes up to TARGET_BATCH_SIZE records.</li>
 * </ul>
 *
 * <p>Schema modes:
 * <ul>
 *   <li>RAW  — writes only the 8 metadata columns; _value_raw contains raw bytes as UTF-8.</li>
 *   <li>JSON — additionally parses the message value as JSON and writes top-level fields
 *              that match the declared schema. Missing fields → null. Non-JSON messages
 *              → all payload fields null, _value_raw still populated.</li>
 * </ul>
 *
 * <p>Data flow:
 * <pre>
 *   setup()  → create KafkaConsumer, assign partition, seek to startOffset, allocate vectors
 *   next()   → poll() → write records to vectors → return count (0 when done)
 *   close()  → close KafkaConsumer
 * </pre>
 */
public class KafkaRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(KafkaRecordReader.class);

  /** Maximum records per Arrow batch. */
  private static final int TARGET_BATCH_SIZE = 4_000;

  /** How many consecutive empty polls before declaring end-of-partition. */
  private static final int MAX_EMPTY_POLLS = 5;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final KafkaStoragePlugin plugin;
  private final KafkaSubScan       subScan;
  private final KafkaScanSpec      spec;

  // Allocated in setup()
  private KafkaConsumer<byte[], byte[]> consumer;
  private TopicPartition               topicPartition;

  // Metadata vectors (always present)
  private VarCharVector topicVec;
  private IntVector     partitionVec;
  private BigIntVector  offsetVec;
  private BigIntVector  timestampVec;
  private VarCharVector timestampTypeVec;
  private VarCharVector keyVec;
  private VarCharVector headersVec;
  private VarCharVector valueRawVec;
  private IntVector     schemaIdVec;

  // Payload vectors (JSON/AVRO mode; keyed by field name)
  private List<PayloadField> payloadFields;

  // AVRO mode support
  private KafkaSchemaRegistryClient schemaRegistryClient; // null unless AVRO mode

  // State — scanStartOffset / scanEndOffset are the final resolved bounds used by next().
  // They start from spec.effectiveStartOffset/EndOffset() and may be further narrowed
  // by timestamp-filter resolution via offsetsForTimes() in setup().
  private long    scanStartOffset;
  private long    scanEndOffset;
  private long    currentOffset;
  private boolean exhausted      = false;
  private int     emptyPollCount = 0;

  // Buffer for partially-consumed poll results between next() calls
  private Iterator<ConsumerRecord<byte[], byte[]>> bufferedRecords = Collections.emptyIterator();

  /** Associates a schema field name with its Arrow vector and type. */
  private static class PayloadField {
    final String name;
    final ArrowType arrowType;
    final ValueVector vector;

    PayloadField(String name, ArrowType arrowType, ValueVector vector) {
      this.name      = name;
      this.arrowType = arrowType;
      this.vector    = vector;
    }
  }

  public KafkaRecordReader(KafkaStoragePlugin plugin,
                            KafkaSubScan subScan,
                            OperatorContext context,
                            KafkaScanSpec spec) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
    this.spec    = spec;
  }

  // -------------------------------------------------------------------------
  // RecordReader lifecycle
  // -------------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    payloadFields = new ArrayList<>();

    // If this split is excluded by partition filter or known-empty offset window, skip it
    if (spec.shouldSkip()) {
      exhausted = true;
      allocateVectors(output);
      logger.debug("KafkaRecordReader skipping: topic={} partition={} (filter pushdown or empty)",
          spec.getTopic(), spec.getPartition());
      return;
    }

    // Allocate Arrow vectors from the declared schema
    allocateVectors(output);

    // Initialize AVRO mode schema registry client if needed
    if ("AVRO".equalsIgnoreCase(spec.getSchemaMode())) {
      String regUrl = plugin.getConfig().schemaRegistryUrl;
      if (regUrl != null && !regUrl.isEmpty()) {
        schemaRegistryClient = new KafkaSchemaRegistryClient(
            regUrl, plugin.getConfig().requestTimeoutMs,
            plugin.getConfig().metadataCacheTtlSeconds * 1000L,
            plugin.getConfig().schemaRegistryUsername,
            plugin.getConfig().schemaRegistryPassword,
            plugin.getConfig().schemaRegistryDisableSslVerification);
      }
    }

    // Initialize scan bounds from offset-level pushdown
    scanStartOffset = spec.effectiveStartOffset();
    scanEndOffset   = spec.effectiveEndOffset();

    // Create consumer and assign the partition
    Properties props = plugin.buildConsumerProps();
    consumer = new KafkaConsumer<>(
        props,
        new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
        new org.apache.kafka.common.serialization.ByteArrayDeserializer());

    topicPartition = new TopicPartition(spec.getTopic(), spec.getPartition());
    consumer.assign(Collections.singletonList(topicPartition));

    // ── Timestamp → offset resolution ──────────────────────────────────────
    // KafkaConsumer.offsetsForTimes() returns the first offset whose message
    // timestamp is >= the requested timestamp, which is exactly what we need
    // to seek to for both lower and upper timestamp bounds.
    if (spec.getTimestampStartMs() >= 0 || spec.getTimestampEndMs() >= 0) {
      resolveTimestampBounds();
    }

    // Skip this split if timestamp resolution narrowed the window to empty
    if (scanStartOffset >= scanEndOffset) {
      exhausted = true;
      logger.debug("KafkaRecordReader skipping: topic={} partition={} "
          + "(timestamp pushdown yielded empty window [{},{}))",
          spec.getTopic(), spec.getPartition(), scanStartOffset, scanEndOffset);
      return;
    }

    consumer.seek(topicPartition, scanStartOffset);
    currentOffset = scanStartOffset;

    logger.debug("KafkaRecordReader started: topic={} partition={} offsets=[{},{})",
        spec.getTopic(), spec.getPartition(), scanStartOffset, scanEndOffset);
  }

  /**
   * Allocates Arrow vectors for the projected columns only.
   *
   * Dremio's OutputMutator places projected fields at their expected positions in the
   * output schema. Adding non-projected fields causes the SCHEMA_CHANGE error because
   * their positions differ from the stored schema. By only allocating what was requested,
   * the output schema always matches the planned schema exactly.
   *
   * For star queries (SELECT *) all schema fields are allocated; otherwise only the
   * requested columns from getColumns() are allocated.
   */
  /**
   * Binds Arrow vectors to our typed fields using ONLY the vectors that
   * ScanOperator.materializeVectors() has already placed in the OutputMutator.
   *
   * We intentionally NEVER call output.addField() here. Calling addField() appends
   * a new vector to the outgoing VectorContainer and nulls out outgoing.schema,
   * which causes ScanOperator.checkAndLearnSchema() to throw SCHEMA_CHANGE even when
   * the old and new schemas are identical in content.
   *
   * ScanOperator.setup() calls materializeVectors(selectedColumns, mutator) before
   * calling our setup(). For projecting queries (SELECT *, SELECT col1, col2, ...),
   * those columns are pre-populated and getVector() returns them.  For aggregate-only
   * queries like COUNT(*), selectedColumns may be empty, so getVector() returns null
   * for every field — in that case all our field references stay null, next() still
   * returns the correct row counts, and no vectors are written (Dremio only needs
   * the count, not the data).
   */
  private void allocateVectors(OutputMutator output) throws ExecutionSetupException {
    for (Field field : subScan.getFullSchema().getFields()) {
      String name = field.getName();
      ValueVector vec = output.getVector(name);
      if (vec == null) continue;  // not in selectedColumns — skip

      ArrowType type = field.getType();
      if (KafkaStoragePlugin.COL_TOPIC.equals(name))               topicVec         = (VarCharVector) vec;
      else if (KafkaStoragePlugin.COL_PARTITION.equals(name))      partitionVec     = (IntVector)     vec;
      else if (KafkaStoragePlugin.COL_OFFSET.equals(name))         offsetVec        = (BigIntVector)  vec;
      else if (KafkaStoragePlugin.COL_TIMESTAMP.equals(name))      timestampVec     = (BigIntVector)  vec;
      else if (KafkaStoragePlugin.COL_TIMESTAMP_TYPE.equals(name)) timestampTypeVec = (VarCharVector) vec;
      else if (KafkaStoragePlugin.COL_KEY.equals(name))            keyVec           = (VarCharVector) vec;
      else if (KafkaStoragePlugin.COL_HEADERS.equals(name))        headersVec       = (VarCharVector) vec;
      else if (KafkaStoragePlugin.COL_VALUE_RAW.equals(name))      valueRawVec      = (VarCharVector) vec;
      else if (KafkaStoragePlugin.COL_SCHEMA_ID.equals(name))      schemaIdVec      = (IntVector)     vec;
      else payloadFields.add(new PayloadField(name, type, vec));
    }
  }

  @Override
  public int next() {
    if (exhausted) return 0;

    int rowCount = 0;

    while (rowCount < TARGET_BATCH_SIZE) {
      // Drain any buffered records from a previous poll first
      while (bufferedRecords.hasNext() && rowCount < TARGET_BATCH_SIZE) {
        ConsumerRecord<byte[], byte[]> record = bufferedRecords.next();

        if (record.offset() >= scanEndOffset) {
          exhausted = true;
          return rowCount;
        }

        writeRecord(record, rowCount++);
        currentOffset = record.offset() + 1;
      }

      if (exhausted) break;
      if (currentOffset >= scanEndOffset) {
        exhausted = true;
        break;
      }
      if (rowCount >= TARGET_BATCH_SIZE) break;

      // Need more records — poll
      try {
        ConsumerRecords<byte[], byte[]> polled =
            consumer.poll(Duration.ofMillis(200));

        if (polled.isEmpty()) {
          emptyPollCount++;
          if (emptyPollCount >= MAX_EMPTY_POLLS) {
            logger.debug("Exhausted after {} empty polls at offset {} (end={})",
                emptyPollCount, currentOffset, scanEndOffset);
            exhausted = true;
            break;
          }
          continue;
        }

        emptyPollCount = 0;
        bufferedRecords = polled.records(topicPartition).iterator();

      } catch (Exception e) {
        logger.error("Error polling Kafka partition {}-{}: {}",
            spec.getTopic(), spec.getPartition(), e.getMessage());
        exhausted = true;
        break;
      }
    }

    return rowCount;
  }

  @Override
  public void close() throws Exception {
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }
  }

  // -------------------------------------------------------------------------
  // Timestamp → offset resolution
  // -------------------------------------------------------------------------

  /**
   * Uses KafkaConsumer.offsetsForTimes() to convert the pushed-down timestamp
   * bounds into actual partition offsets, then narrows scanStartOffset /
   * scanEndOffset accordingly.
   *
   * offsetsForTimes(ts) returns the first offset whose message timestamp is >= ts.
   * This maps cleanly to our pre-adjusted filter semantics:
   *   timestampStartMs = T  →  seek start to first offset with timestamp >= T
   *   timestampEndMs   = T  →  stop before first offset with timestamp >= T
   *
   * If no message in the partition has a timestamp >= the requested value,
   * offsetsForTimes returns null for that partition — meaning:
   *   - null for start → no messages match the start bound → empty window
   *   - null for end   → all messages are before the end bound → keep full end
   */
  private void resolveTimestampBounds() {
    try {
      if (spec.getTimestampStartMs() >= 0) {
        Map<TopicPartition, Long> query = Collections.singletonMap(
            topicPartition, spec.getTimestampStartMs());
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> result =
            consumer.offsetsForTimes(query);
        org.apache.kafka.clients.consumer.OffsetAndTimestamp oat = result.get(topicPartition);
        if (oat == null) {
          // No messages at or after the start timestamp in this partition
          scanStartOffset = scanEndOffset; // force empty window
          logger.debug("Timestamp start {} resolved to empty for {}-{}",
              spec.getTimestampStartMs(), spec.getTopic(), spec.getPartition());
        } else {
          scanStartOffset = Math.max(scanStartOffset, oat.offset());
          logger.debug("Timestamp start {} resolved to offset {} for {}-{}",
              spec.getTimestampStartMs(), oat.offset(), spec.getTopic(), spec.getPartition());
        }
      }

      if (spec.getTimestampEndMs() >= 0 && scanStartOffset < scanEndOffset) {
        Map<TopicPartition, Long> query = Collections.singletonMap(
            topicPartition, spec.getTimestampEndMs());
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> result =
            consumer.offsetsForTimes(query);
        org.apache.kafka.clients.consumer.OffsetAndTimestamp oat = result.get(topicPartition);
        if (oat != null) {
          // Stop before the first message at or after the end timestamp
          scanEndOffset = Math.min(scanEndOffset, oat.offset());
          logger.debug("Timestamp end {} resolved to offset {} for {}-{}",
              spec.getTimestampEndMs(), oat.offset(), spec.getTopic(), spec.getPartition());
        }
        // null → all messages are before end timestamp → keep current scanEndOffset
      }
    } catch (Exception e) {
      logger.warn("offsetsForTimes() failed for {}-{}, falling back to full window: {}",
          spec.getTopic(), spec.getPartition(), e.getMessage());
      // Fall back to the offset-level bounds; Dremio's residual filter will still
      // enforce correctness, just without the seek optimization.
    }
  }

  // -------------------------------------------------------------------------
  // Record writing
  // -------------------------------------------------------------------------

  private void writeRecord(ConsumerRecord<byte[], byte[]> record, int idx) {
    // --- Metadata columns (only write if vector was allocated/projected) ---
    if (topicVec != null) {
      byte[] topicBytes = spec.getTopic().getBytes(StandardCharsets.UTF_8);
      topicVec.setSafe(idx, topicBytes, 0, topicBytes.length);
    }
    if (partitionVec != null) partitionVec.setSafe(idx, record.partition());
    if (offsetVec    != null) offsetVec.setSafe(idx, record.offset());

    if (timestampVec != null || timestampTypeVec != null) {
      if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
        if (timestampVec     != null) timestampVec.setSafe(idx, record.timestamp());
        if (timestampTypeVec != null) {
          byte[] tsTypeBytes = record.timestampType().name().getBytes(StandardCharsets.UTF_8);
          timestampTypeVec.setSafe(idx, tsTypeBytes, 0, tsTypeBytes.length);
        }
      } else {
        if (timestampVec     != null) timestampVec.setNull(idx);
        if (timestampTypeVec != null) timestampTypeVec.setNull(idx);
      }
    }

    if (keyVec != null) {
      if (record.key() != null) {
        byte[] keyBytes = new String(record.key(), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
        keyVec.setSafe(idx, keyBytes, 0, keyBytes.length);
      } else {
        keyVec.setNull(idx);
      }
    }

    if (headersVec != null) {
      if (record.headers() != null && record.headers().toArray().length > 0) {
        byte[] hBytes = encodeHeaders(record.headers().toArray()).getBytes(StandardCharsets.UTF_8);
        headersVec.setSafe(idx, hBytes, 0, hBytes.length);
      } else {
        headersVec.setNull(idx);
      }
    }

    // --- Value ---
    byte[] rawBytes = record.value();

    // _schema_id: detect Confluent Avro wire format magic byte (0x00 prefix)
    int detectedSchemaId = -1;
    if (rawBytes != null && rawBytes.length >= 5 && rawBytes[0] == 0x00) {
      detectedSchemaId = java.nio.ByteBuffer.wrap(rawBytes, 1, 4).getInt();
      if (schemaIdVec != null) schemaIdVec.setSafe(idx, detectedSchemaId);
    } else {
      if (schemaIdVec != null) schemaIdVec.setNull(idx);
    }

    // Write _value_raw: always write raw bytes as UTF-8 string (accessible in all modes)
    String valueStr = null;
    if (rawBytes != null) {
      valueStr = new String(rawBytes, StandardCharsets.UTF_8);
      if (valueRawVec != null) {
        byte[] vBytes = valueStr.getBytes(StandardCharsets.UTF_8);
        valueRawVec.setSafe(idx, vBytes, 0, vBytes.length);
      }
    } else {
      if (valueRawVec != null) valueRawVec.setNull(idx);
    }

    // --- AVRO payload fields ---
    if ("AVRO".equalsIgnoreCase(spec.getSchemaMode())
        && schemaRegistryClient != null && detectedSchemaId >= 0 && !payloadFields.isEmpty()) {
      try {
        org.apache.avro.Schema writerSchema = schemaRegistryClient.getSchemaById(detectedSchemaId);
        org.apache.avro.io.DatumReader<org.apache.avro.generic.GenericRecord> reader =
            new org.apache.avro.generic.GenericDatumReader<>(writerSchema);
        org.apache.avro.io.Decoder decoder = org.apache.avro.io.DecoderFactory.get()
            .binaryDecoder(rawBytes, 5, rawBytes.length - 5, null);
        org.apache.avro.generic.GenericRecord avroRecord = reader.read(null, decoder);
        for (PayloadField pf : payloadFields) {
          Object val = avroRecord.get(pf.name);
          if (val != null) {
            KafkaAvroConverter.writeToVector(pf.vector, pf.arrowType, val, idx);
          }
        }
      } catch (Exception e) {
        logger.debug("Avro deserialization failed for schema {}: {}", detectedSchemaId, e.getMessage());
      }
      return; // Skip JSON parsing in AVRO mode
    }

    // --- JSON payload fields ---
    if (!payloadFields.isEmpty() && valueStr != null) {
      try {
        JsonNode root = MAPPER.readTree(valueStr);
        if (root != null && root.isObject()) {
          for (PayloadField pf : payloadFields) {
            JsonNode fieldNode = root.get(pf.name);
            if (fieldNode != null && !fieldNode.isNull() && !fieldNode.isMissingNode()) {
              writeJsonField(pf, fieldNode, idx);
            }
          }
        }
      } catch (Exception e) {
        // Not JSON — payload fields remain null
      }
    }
  }

  private void writeJsonField(PayloadField pf, JsonNode node, int idx) {
    try {
      ArrowType type = pf.arrowType;

      if (type instanceof ArrowType.Bool) {
        ((BitVector) pf.vector).setSafe(idx, node.asBoolean() ? 1 : 0);
      } else if (type instanceof ArrowType.Int && ((ArrowType.Int) type).getBitWidth() == 64) {
        ((BigIntVector) pf.vector).setSafe(idx, node.asLong());
      } else if (type instanceof ArrowType.FloatingPoint) {
        ((Float8Vector) pf.vector).setSafe(idx, node.asDouble());
      } else {
        // Default: convert to string
        byte[] bytes = node.asText().getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) pf.vector).setSafe(idx, bytes, 0, bytes.length);
      }
    } catch (Exception e) {
      // Type coercion failure — leave null
    }
  }

  /**
   * Encodes Kafka headers as a flat JSON object: {"header-name": "header-value", ...}
   *
   * This format allows direct key lookup in SQL:
   *   CONVERT_FROM(_headers, 'JSON')['x-request-id']
   *
   * If a header key appears more than once, the last value wins.
   * Header values are decoded as UTF-8 strings. Null values are written as JSON null.
   */
  private String encodeHeaders(Header[] headers) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Header h : headers) {
      if (!first) sb.append(",");
      first = false;
      appendJsonString(sb, h.key());
      sb.append(":");
      byte[] val = h.value();
      if (val != null) {
        appendJsonString(sb, new String(val, StandardCharsets.UTF_8));
      } else {
        sb.append("null");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  private void appendJsonString(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"')       sb.append("\\\"");
      else if (c == '\\') sb.append("\\\\");
      else if (c == '\n') sb.append("\\n");
      else if (c == '\r') sb.append("\\r");
      else if (c == '\t') sb.append("\\t");
      else                sb.append(c);
    }
    sb.append('"');
  }
}
