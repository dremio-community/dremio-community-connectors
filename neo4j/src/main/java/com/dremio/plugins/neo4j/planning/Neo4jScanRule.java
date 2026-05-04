package com.dremio.plugins.neo4j.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.neo4j.Neo4jConf;
import com.dremio.plugins.neo4j.scan.Neo4jScanSpec;

import java.util.List;

public class Neo4jScanRule extends SourceLogicalConverter {

  public static final Neo4jScanRule INSTANCE = new Neo4jScanRule();

  private Neo4jScanRule() {
    super(Neo4jConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String label = qualifiedName.get(qualifiedName.size() - 1);

    Neo4jScanSpec spec = new Neo4jScanSpec(label);
    return new Neo4jScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
  }
}
