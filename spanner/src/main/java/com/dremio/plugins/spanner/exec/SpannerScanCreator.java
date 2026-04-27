package com.dremio.plugins.spanner.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.spanner.SpannerStoragePlugin;
import com.dremio.plugins.spanner.scan.SpannerRecordReader;
import com.dremio.plugins.spanner.scan.SpannerSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

/**
 * Execution-side factory for Spanner scan operators.
 *
 * Discovered by Dremio's OperatorCreatorRegistry via classpath scanning
 * (package registered in sabot-module.conf). Maps to SpannerSubScan
 * via the generic type parameter {@code ProducerOperator.Creator<SpannerSubScan>}.
 */
public class SpannerScanCreator implements ProducerOperator.Creator<SpannerSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  SpannerSubScan subScan) throws ExecutionSetupException {
    SpannerStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
    RecordReader reader = new SpannerRecordReader(plugin, subScan, context);
    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
