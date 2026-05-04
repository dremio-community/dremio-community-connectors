package com.dremio.plugins.neo4j.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.neo4j.Neo4jStoragePlugin;
import com.dremio.plugins.neo4j.scan.Neo4jRecordReader;
import com.dremio.plugins.neo4j.scan.Neo4jSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

public class Neo4jScanCreator implements ProducerOperator.Creator<Neo4jSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  Neo4jSubScan subScan) throws ExecutionSetupException {
    Neo4jStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
    RecordReader reader = new Neo4jRecordReader(plugin, subScan, context);
    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
  }
}
