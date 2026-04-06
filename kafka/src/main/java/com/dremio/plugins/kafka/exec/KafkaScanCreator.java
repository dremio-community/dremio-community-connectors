package com.dremio.plugins.kafka.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.kafka.KafkaStoragePlugin;
import com.dremio.plugins.kafka.scan.KafkaRecordReader;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
import com.dremio.plugins.kafka.scan.KafkaSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution-side factory for Kafka scan operators.
 *
 * Dremio's OperatorCreatorRegistry discovers this class via classpath scanning
 * (package registered in sabot-module.conf) and maps it to KafkaSubScan
 * through the generic type parameter {@code ProducerOperator.Creator<KafkaSubScan>}.
 *
 * Creates one KafkaRecordReader per partition spec in the SubScan, chained together
 * via RecordReaderIterator so they execute sequentially within the fragment.
 */
public class KafkaScanCreator implements ProducerOperator.Creator<KafkaSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  KafkaSubScan subScan)
      throws ExecutionSetupException {

    KafkaStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

    List<KafkaScanSpec> specs = subScan.getScanSpecs();
    List<RecordReader> readers = new ArrayList<>(specs.size());
    for (KafkaScanSpec spec : specs) {
      readers.add(new KafkaRecordReader(plugin, subScan, context, spec));
    }

    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(readers.iterator()));
  }
}
