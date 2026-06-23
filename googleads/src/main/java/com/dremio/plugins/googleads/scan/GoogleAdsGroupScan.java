package com.dremio.plugins.googleads.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.googleads.scan.GoogleAdsSubScan.GoogleAdsScanSpec;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GoogleAdsGroupScan extends AbstractGroupScan {

  private final StoragePluginId  pluginId;
  private final GoogleAdsScanSpec scanSpec;

  public GoogleAdsGroupScan(OpProps props, TableMetadata tableMetadata,
                             List<SchemaPath> columns, StoragePluginId pluginId,
                             GoogleAdsScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId  = pluginId;
    this.scanSpec  = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    return new GoogleAdsSubScan(
        getProps(),
        getFullSchema(),
        Collections.singletonList(Arrays.asList(scanSpec.getTableName())),
        getColumns(),
        pluginId,
        scanSpec
    );
  }

  @Override public int getMaxParallelizationWidth() { return 1; }
  @Override public int getOperatorType()             { return 0; }

  @JsonIgnore public StoragePluginId   getPluginId()  { return pluginId; }
  @JsonIgnore public GoogleAdsScanSpec getScanSpec()  { return scanSpec; }
}
