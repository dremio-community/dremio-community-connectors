package com.dremio.plugins.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Infers an Arrow schema for a Kafka topic by sampling recent messages
 * and inspecting their JSON structure.
 *
 * <p>Schema inference algorithm:
 * <ol>
 *   <li>Sample up to {@code sampleCount} records from the latest offsets across all partitions.</li>
 *   <li>Parse each message value as JSON.</li>
 *   <li>For each top-level field, determine the most general Arrow type across all sampled records:
 *       <ul>
 *         <li>boolean  → Bit</li>
 *         <li>integer  → BigInt</li>
 *         <li>float    → Float64</li>
 *         <li>string   → Utf8 (VarChar)</li>
 *         <li>object/array → Utf8 (stringified)</li>
 *         <li>integer + float in same field → Float64 (promoted)</li>
 *         <li>any scalar + string  → Utf8 (promoted)</li>
 *       </ul>
 *   </li>
 *   <li>All inferred fields are nullable.</li>
 *   <li>Returns an empty list when no JSON records were found (RAW mode fallback).</li>
 * </ol>
 */
public class KafkaSchemaInferrer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSchemaInferrer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Inferred Arrow type tokens (ordered from least to most general for promotion).
   */
  private enum InferredType {
    BOOLEAN, INT, FLOAT, STRING;

    /** Returns the more general of two types. */
    static InferredType promote(InferredType a, InferredType b) {
      if (a == b) return a;
      if (a == STRING || b == STRING) return STRING;
      if (a == FLOAT   || b == FLOAT)  return FLOAT;
      return STRING; // boolean + int → string as safest option
    }
  }

  private final Properties consumerProps;
  private final int sampleCount;

  /**
   * @param consumerProps  base consumer Properties (bootstrap.servers + security config);
   *                       group.id will be overridden to a temporary unique value.
   * @param sampleCount    max records to sample across all partitions.
   */
  public KafkaSchemaInferrer(Properties consumerProps, int sampleCount) {
    this.consumerProps = consumerProps;
    this.sampleCount   = Math.max(1, sampleCount);
  }

  /**
   * Infers Arrow payload fields for the given topic.
   * Returns an empty list if the topic is empty or no parseable JSON was found.
   */
  public List<Field> inferFields(String topic, List<TopicPartition> partitions,
                                  Map<TopicPartition, Long> earliestOffsets,
                                  Map<TopicPartition, Long> latestOffsets) {
    if (partitions.isEmpty()) {
      return Collections.emptyList();
    }

    // Collect field → type observations across all sample records
    Map<String, InferredType> fieldTypes = new LinkedHashMap<>();
    int totalSampled = 0;
    int samplesPerPartition = Math.max(1, sampleCount / partitions.size());

    Properties props = new Properties();
    props.putAll(consumerProps);
    props.put("group.id", "dremio-schema-inferrer-" + System.currentTimeMillis());
    props.put("auto.offset.reset", "latest");
    props.put("enable.auto.commit", "false");
    props.put("max.poll.records", String.valueOf(samplesPerPartition));

    try (KafkaConsumer<byte[], byte[]> consumer =
             new KafkaConsumer<>(props,
                 new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                 new org.apache.kafka.common.serialization.ByteArrayDeserializer())) {

      consumer.assign(partitions);

      for (TopicPartition tp : partitions) {
        long latest  = latestOffsets.getOrDefault(tp, 0L);
        long earliest = earliestOffsets.getOrDefault(tp, 0L);
        if (latest <= earliest) continue; // empty partition

        long seekTo = Math.max(earliest, latest - samplesPerPartition);
        consumer.seek(tp, seekTo);
      }

      // Poll in bursts until we have enough samples or nothing left
      int emptyPolls = 0;
      while (totalSampled < sampleCount && emptyPolls < 3) {
        ConsumerRecords<byte[], byte[]> records =
            consumer.poll(Duration.ofMillis(500));

        if (records.isEmpty()) {
          emptyPolls++;
          continue;
        }
        emptyPolls = 0;

        for (ConsumerRecord<byte[], byte[]> record : records) {
          if (record.value() == null) continue;
          try {
            String valueStr = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode root = MAPPER.readTree(valueStr);
            if (root != null && root.isObject()) {
              mergeFields(root, fieldTypes);
              totalSampled++;
            }
          } catch (Exception ignore) {
            // Not JSON — skip silently
          }
          if (totalSampled >= sampleCount) break;
        }
      }

    } catch (Exception e) {
      logger.warn("Schema inference failed for topic {}: {}", topic, e.getMessage());
      return Collections.emptyList();
    }

    if (fieldTypes.isEmpty()) {
      logger.debug("No JSON records found for topic {} — using RAW mode schema", topic);
      return Collections.emptyList();
    }

    logger.info("Inferred {} payload fields for topic {} from {} sample records",
        fieldTypes.size(), topic, totalSampled);

    return buildArrowFields(fieldTypes);
  }

  /** Merges field observations from one JSON object into the accumulated type map. */
  private void mergeFields(JsonNode root, Map<String, InferredType> fieldTypes) {
    root.fields().forEachRemaining(entry -> {
      String name = entry.getKey();
      InferredType observed = toInferredType(entry.getValue());
      if (observed == null) return;

      fieldTypes.merge(name, observed, InferredType::promote);
    });
  }

  private InferredType toInferredType(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN: return InferredType.BOOLEAN;
      case NUMBER:  return node.isIntegralNumber() ? InferredType.INT : InferredType.FLOAT;
      case STRING:  return InferredType.STRING;
      case OBJECT:
      case ARRAY:   return InferredType.STRING; // stringify nested structures
      default:      return null; // NULL, MISSING — skip
    }
  }

  private List<Field> buildArrowFields(Map<String, InferredType> fieldTypes) {
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<String, InferredType> entry : fieldTypes.entrySet()) {
      String name = entry.getKey();
      ArrowType arrowType;
      switch (entry.getValue()) {
        case BOOLEAN:
          arrowType = ArrowType.Bool.INSTANCE;
          break;
        case INT:
          arrowType = new ArrowType.Int(64, true);
          break;
        case FLOAT:
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
          break;
        case STRING:
        default:
          arrowType = ArrowType.Utf8.INSTANCE;
          break;
      }
      // All inferred fields are nullable — missing or null values are common in JSON payloads
      fields.add(new Field(name, FieldType.nullable(arrowType), null));
    }
    return fields;
  }
}
