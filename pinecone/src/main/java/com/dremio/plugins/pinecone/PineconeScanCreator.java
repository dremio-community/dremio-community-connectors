/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.pinecone;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.plugins.pinecone.PineconeSubScan.PineconeScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.ArrayList;
import java.util.List;

public class PineconeScanCreator implements ProducerOperator.Creator<PineconeSubScan> {

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                   OperatorContext context,
                                   PineconeSubScan subScan) throws ExecutionSetupException {
        PineconeStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
        PineconeScanSpec spec = subScan.getScanSpec();

        // Resolve host from spec; fall back to plugin's cached host if spec host is empty
        String host = spec.getHost();
        if (host == null || host.isEmpty()) {
            host = plugin.getIndexHost(spec.getTable());
            if (host == null) host = "";
            spec = new PineconeScanSpec(spec.getTable(), host, spec.getEstimatedRowCount());
        }

        List<RecordReader> readers = new ArrayList<>();
        readers.add(new PineconeRecordReader(
                context,
                plugin.getConnection(),
                spec,
                subScan.getFullSchema(),
                plugin.getConf()));

        return new ScanOperator(fec, subScan, context,
                RecordReaderIterator.from(readers.iterator()));
    }
}
