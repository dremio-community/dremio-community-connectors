package com.dremio.plugins.splunk.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Physical operator representing a Splunk index scan in the planning layer.
 *
 * Produced by SplunkScanPrel.getPhysicalOperator() during physical planning.
 * Dremio's fragment scheduler calls getSpecificScan() to produce SplunkSubScan
 * instances for each executor fragment.
 *
 * In V1, there is one split per index (single-partition scan).
 */
public class SplunkGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(SplunkGroupScan.class);

  private final StoragePluginId pluginId;
  private final SplunkScanSpec scanSpec;

  public SplunkGroupScan(OpProps props,
                          TableMetadata tableMetadata,
                          List<SchemaPath> columns,
                          StoragePluginId pluginId,
                          SplunkScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Decode the scan spec from each split's extended property bytes
    List<SplunkScanSpec> specs = new ArrayList<>();
    for (SplitWork sw : work) {
      SplunkScanSpec decoded = decodeSplitSpec(sw);
      if (decoded != null) specs.add(decoded);
    }
    if (specs.isEmpty()) {
      specs.add(scanSpec);
    }

    SplunkScanSpec primary = specs.get(0);
    return new SplunkSubScan(
        getProps(),
        getFullSchema(),
        Collections.singletonList(primary.toTablePath()),
        getColumns(),
        pluginId,
        primary,
        specs
    );
  }

  private SplunkScanSpec decodeSplitSpec(SplitWork sw) {
    try {
      byte[] extBytes = null;
      try {
        Object extPropObj = sw.getClass()
            .getMethod("getSplitExtendedProperty").invoke(sw);
        if (extPropObj != null) {
          extBytes = (byte[]) extPropObj.getClass().getMethod("toByteArray").invoke(extPropObj);
        }
      } catch (Exception ignore) {}

      if (extBytes != null && extBytes.length > 0) {
        String encoded = new String(extBytes, StandardCharsets.UTF_8);
        SplunkScanSpec decoded = SplunkScanSpec.fromExtendedProperty(encoded);
        if (decoded != null) return decoded;
      }
    } catch (Exception e) {
      logger.warn("Could not decode Splunk split spec, falling back to prototype: {}",
          e.getMessage());
    }
    return scanSpec;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1; // Single partition in V1
  }

  @Override
  public int getOperatorType() {
    return 0;
  }

  @JsonIgnore
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonIgnore
  public SplunkScanSpec getScanSpec() { return scanSpec; }
}
