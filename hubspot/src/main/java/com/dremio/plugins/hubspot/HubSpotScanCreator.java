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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Execution-side factory for HubSpot scan operators.
 *
 * <p>Deserializes the HubSpotScanSpec from the split bytes so the RecordReader
 * gets the full property list that was resolved at metadata time.
 */
public class HubSpotScanCreator implements ProducerOperator.Creator<HubSpotSubScan> {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotScanCreator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                   OperatorContext context,
                                   HubSpotSubScan subScan) throws ExecutionSetupException {
        HubSpotStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

        // Prefer the spec from the sub-scan (which may have been decoded from split bytes)
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
