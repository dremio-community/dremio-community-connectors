package com.dremio.plugins.cassandra.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.plugins.cassandra.CassandraStoragePlugin;
import com.dremio.plugins.cassandra.scan.CassandraRecordReader;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import com.dremio.plugins.cassandra.scan.CassandraSubScan;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution-side factory for Cassandra scan operators.
 *
 * Dremio's OperatorCreatorRegistry discovers this class via classpath scanning
 * (package registered in sabot-module.conf) and maps it to CassandraSubScan
 * through the generic type parameter {@code ProducerOperator.Creator<CassandraSubScan>}.
 *
 * Called on each executor node for each assigned CassandraSubScan fragment.
 * Creates a CassandraRecordReader that executes the CQL query and converts
 * Cassandra rows into Dremio Arrow batches.
 */
public class CassandraScanCreator implements ProducerOperator.Creator<CassandraSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  CassandraSubScan subScan)
      throws ExecutionSetupException {

    CassandraStoragePlugin plugin =
        fec.getStoragePlugin(subScan.getPluginId());

    // Create one RecordReader per token-range spec.
    // Multiple specs arise when several splits are co-located on one executor fragment
    // (common in single-node deployments). Each reader executes its own CQL range query.
    List<CassandraScanSpec> specs = subScan.getScanSpecs();
    List<RecordReader> readers = new ArrayList<>(specs.size());
    for (CassandraScanSpec spec : specs) {
      readers.add(new CassandraRecordReader(plugin, subScan, context, spec));
    }

    return new ScanOperator(fec, subScan, context,
        RecordReaderIterator.from(readers.iterator()));
  }
}
