package com.dremio.plugins.kafka.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Physical operator representing a Kafka topic scan in the planning layer.
 *
 * Produced by KafkaScanPrel.getPhysicalOperator() during physical planning.
 * Dremio's fragment scheduler calls getSplits() to distribute work, then
 * getSpecificScan() to produce one SubScan per executor fragment.
 *
 * One split = one Kafka partition (offset range frozen at plan time).
 * Multiple splits can be co-located on one executor fragment in single-node deployments.
 */
public class KafkaGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(KafkaGroupScan.class);

  private final StoragePluginId pluginId;
  private final KafkaScanSpec scanSpec; // prototype spec (topic + schemaMode; partition/offsets per split)

  public KafkaGroupScan(OpProps props,
                         TableMetadata tableMetadata,
                         List<SchemaPath> columns,
                         StoragePluginId pluginId,
                         KafkaScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId  = pluginId;
    this.scanSpec  = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Decode all splits assigned to this executor fragment.
    // Each SplitWork carries the serialized KafkaScanSpec in its extended property bytes.
    List<KafkaScanSpec> specs = new ArrayList<>();
    for (SplitWork sw : work) {
      KafkaScanSpec decoded = decodeSplitSpec(sw);
      if (decoded != null) {
        specs.add(decoded);
      }
    }
    if (specs.isEmpty()) {
      specs.add(scanSpec); // fallback: prototype spec
    }

    return new KafkaSubScan(
        getProps(),
        getFullSchema(),
        List.of(specs.get(0).toTablePath()),
        getColumns(),
        pluginId,
        specs.get(0),
        specs
    );
  }

  /**
   * Decodes a SplitWork's extended property bytes into a KafkaScanSpec.
   */
  private KafkaScanSpec decodeSplitSpec(SplitWork sw) {
    try {
      byte[] extBytes = null;
      try {
        Object extPropObj = sw.getClass()
            .getMethod("getSplitExtendedProperty").invoke(sw);
        if (extPropObj != null) {
          extBytes = (byte[]) extPropObj.getClass().getMethod("toByteArray").invoke(extPropObj);
        }
      } catch (Exception ignore) { }

      if (extBytes != null && extBytes.length > 0) {
        String encoded = new String(extBytes, StandardCharsets.UTF_8);
        KafkaScanSpec decoded = KafkaScanSpec.fromExtendedProperty(encoded);
        if (decoded != null) {
          return decoded;
        }
      }
    } catch (Exception e) {
      logger.warn("Could not decode Kafka split spec, falling back to prototype: {}", e.getMessage());
    }
    return scanSpec;
  }

  @Override
  public int getMaxParallelizationWidth() {
    // Allow Dremio to use all available splits (one per Kafka partition).
    return Integer.MAX_VALUE;
  }

  @Override
  public int getOperatorType() {
    return 0; // UNKNOWN — operator type is only used for metrics labeling
  }

  @JsonIgnore
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonIgnore
  public KafkaScanSpec getScanSpec() {
    return scanSpec;
  }
}
