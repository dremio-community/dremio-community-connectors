package com.dremio.plugins.splunk.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.plugins.splunk.SplunkStoragePlugin;
import com.dremio.plugins.splunk.scan.SplunkRecordReader;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import com.dremio.plugins.splunk.scan.SplunkSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution-side factory for Splunk scan operators.
 *
 * Dremio's OperatorCreatorRegistry discovers this class via classpath scanning
 * (package registered in sabot-module.conf) and maps it to SplunkSubScan
 * through the generic type parameter {@code ProducerOperator.Creator<SplunkSubScan>}.
 *
 * Creates one SplunkRecordReader per scan spec in the SubScan (typically one per index).
 */
public class SplunkScanCreator implements ProducerOperator.Creator<SplunkSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  SplunkSubScan subScan)
      throws ExecutionSetupException {

    SplunkStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

    List<SplunkScanSpec> specs = subScan.getScanSpecs();
    List<RecordReader> readers = new ArrayList<>(specs.size());
    for (SplunkScanSpec spec : specs) {
      readers.add(new SplunkRecordReader(plugin, subScan, context, spec));
    }

    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(readers.iterator()));
  }
}
