/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.hubspot;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HubSpotScanCreator implements ProducerOperator.Creator<HubSpotSubScan> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                   OperatorContext context,
                                   HubSpotSubScan subScan) throws ExecutionSetupException {
        HubSpotStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
        HubSpotScanSpec spec = subScan.getScanSpec();

        List<RecordReader> readers = new ArrayList<>();
        readers.add(new HubSpotRecordReader(
                context,
                plugin.getConnection(),
                spec,
                subScan.getFullSchema(),
                plugin.getConf()));

        return new ScanOperator(fec, subScan, context,
                RecordReaderIterator.from(readers.iterator()));
    }
}
