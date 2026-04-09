package com.dremio.plugins.dynamodb.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.dynamodb.DynamoDBStoragePlugin;
import com.dremio.plugins.dynamodb.scan.DynamoDBRecordReader;
import com.dremio.plugins.dynamodb.scan.DynamoDBSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

/**
 * Execution-side factory for DynamoDB scan operators.
 *
 * Discovered by Dremio's OperatorCreatorRegistry via classpath scanning
 * (package registered in sabot-module.conf). Maps to DynamoDBSubScan
 * via the generic type parameter {@code ProducerOperator.Creator<DynamoDBSubScan>}.
 */
public class DynamoDBScanCreator implements ProducerOperator.Creator<DynamoDBSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  DynamoDBSubScan subScan) throws ExecutionSetupException {
    DynamoDBStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
    RecordReader reader = new DynamoDBRecordReader(plugin, subScan, context);
    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
