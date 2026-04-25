/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.cosmosdb;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

import java.util.Collections;

public class CosmosScanCreator implements ProducerOperator.Creator<CosmosSubScan> {

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                    OperatorContext context,
                                    CosmosSubScan subScan) throws ExecutionSetupException {
        CosmosStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
        RecordReader reader = new CosmosRecordReader(
                context,
                plugin.getConnection(),
                subScan.getScanSpec(),
                subScan.getFullSchema());
        return new ScanOperator(fec, subScan, context,
                RecordReaderIterator.from(Collections.singletonList(reader).iterator()));
    }
}
