package com.dremio.plugins.pubsub.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * Physical operator representing a Pub/Sub subscription scan in the planning layer.
 *
 * Pub/Sub has no partitions, so there is always exactly one split.
 * getSpecificScan() returns a PubSubSubScan wrapping the single scan spec.
 */
public class PubSubGroupScan extends AbstractGroupScan {

  private final StoragePluginId pluginId;
  private final PubSubScanSpec  scanSpec;

  public PubSubGroupScan(OpProps props,
                          TableMetadata tableMetadata,
                          List<SchemaPath> columns,
                          StoragePluginId pluginId,
                          PubSubScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Attempt to decode spec from split extended property; fall back to prototype
    PubSubScanSpec spec = scanSpec;
    if (!work.isEmpty()) {
      PubSubScanSpec decoded = decodeSplitSpec(work.get(0));
      if (decoded != null) spec = decoded;
    }

    return new PubSubSubScan(
        getProps(),
        getFullSchema(),
        Collections.singletonList(Collections.singletonList(spec.getSubscription())),
        getColumns(),
        pluginId,
        spec
    );
  }

  private PubSubScanSpec decodeSplitSpec(SplitWork sw) {
    try {
      byte[] extBytes = null;
      try {
        Object extPropObj = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
        if (extPropObj != null) {
          extBytes = (byte[]) extPropObj.getClass().getMethod("toByteArray").invoke(extPropObj);
        }
      } catch (Exception ignore) { }

      if (extBytes != null && extBytes.length > 0) {
        return PubSubScanSpec.fromExtendedProperty(new String(extBytes, StandardCharsets.UTF_8));
      }
    } catch (Exception e) { }
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1; // Pub/Sub subscriptions are single-split
  }

  @Override
  public int getOperatorType() {
    return 0;
  }

  @JsonIgnore
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonIgnore
  public PubSubScanSpec getScanSpec() { return scanSpec; }
}
