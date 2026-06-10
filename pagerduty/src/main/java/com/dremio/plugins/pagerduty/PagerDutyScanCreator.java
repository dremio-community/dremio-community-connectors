package com.dremio.plugins.pagerduty;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

/**
 * Execution-side factory for PagerDuty scan operators.
 */
public class PagerDutyScanCreator implements ProducerOperator.Creator<PagerDutySubScan> {

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                    OperatorContext context,
                                    PagerDutySubScan subScan) throws ExecutionSetupException {
        PagerDutyStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

        RecordReader reader = new PagerDutyRecordReader(
                context,
                plugin.getConnection(),
                subScan.getScanSpec(),
                subScan.getFullSchema());

        return new ScanOperator(fec, subScan, context,
                RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
    }
}
