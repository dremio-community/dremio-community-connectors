package com.dremio.plugins.googleads.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.plugins.googleads.GoogleAdsStoragePlugin;
import com.dremio.plugins.googleads.GoogleAdsTableDef;
import com.dremio.plugins.googleads.scan.GoogleAdsRecordReader;
import com.dremio.plugins.googleads.scan.GoogleAdsSubScan;
import com.dremio.plugins.googleads.scan.GoogleAdsSubScan.GoogleAdsScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

public class GoogleAdsScanCreator implements ProducerOperator.Creator<GoogleAdsSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  GoogleAdsSubScan subScan) throws ExecutionSetupException {

    GoogleAdsStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
    GoogleAdsScanSpec spec = subScan.getScanSpec();

    GoogleAdsTableDef tableDef = GoogleAdsTableDef.ALL.get(spec.getTableName());
    if (tableDef == null) {
      throw new ExecutionSetupException("Unknown Google Ads table: " + spec.getTableName());
    }

    RecordReader reader = new GoogleAdsRecordReader(
        context, plugin.getClient(), tableDef, spec.getDateRangeDays());

    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
