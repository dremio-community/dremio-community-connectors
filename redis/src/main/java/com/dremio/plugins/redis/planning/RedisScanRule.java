package com.dremio.plugins.redis.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.redis.RedisConf;
import com.dremio.plugins.redis.scan.RedisScanSpec;

import java.util.List;

public class RedisScanRule extends SourceLogicalConverter {

  public static final RedisScanRule INSTANCE = new RedisScanRule();

  private RedisScanRule() {
    super(RedisConf.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    RedisScanSpec spec = new RedisScanSpec(tableName);
    return new RedisScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
  }
}
