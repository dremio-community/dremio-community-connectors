package com.dremio.plugins.redis.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.redis.RedisPlugin;
import com.dremio.plugins.redis.scan.RedisRecordReader;
import com.dremio.plugins.redis.scan.RedisSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

public class RedisScanCreator implements ProducerOperator.Creator<RedisSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  RedisSubScan subScan) throws ExecutionSetupException {
    RedisPlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
    RecordReader reader = new RedisRecordReader(plugin, subScan, context);
    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
