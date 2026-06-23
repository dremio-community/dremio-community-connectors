package com.dremio.plugins.pubsub.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.pubsub.PubSubStoragePlugin;
import com.dremio.plugins.pubsub.scan.PubSubRecordReader;
import com.dremio.plugins.pubsub.scan.PubSubSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

/**
 * Execution-side factory for Pub/Sub scan operators.
 *
 * Discovered via classpath scanning (sabot-module.conf registers the package).
 * Maps PubSubSubScan → PubSubRecordReader → ScanOperator.
 *
 * Pub/Sub has no partitions, so there is always exactly one RecordReader per SubScan.
 */
public class PubSubScanCreator implements ProducerOperator.Creator<PubSubSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  PubSubSubScan subScan)
      throws ExecutionSetupException {

    PubSubStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

    RecordReader reader = new PubSubRecordReader(plugin, subScan, context, subScan.getScanSpec());

    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
