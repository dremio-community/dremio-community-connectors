/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution-side factory for Salesforce scan operators.
 *
 * <p>Discovered by Dremio's {@code OperatorCreatorRegistry} via classpath scanning
 * (package registered in sabot-module.conf). Maps to {@link SalesforceSubScan} via
 * the generic type parameter {@code ProducerOperator.Creator<SalesforceSubScan>}.
 */
public class SalesforceScanCreator implements ProducerOperator.Creator<SalesforceSubScan> {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceScanCreator.class);

    @Override
    public ProducerOperator create(FragmentExecutionContext fec,
                                    OperatorContext context,
                                    SalesforceSubScan subScan) throws ExecutionSetupException {
        SalesforceStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());

        List<RecordReader> readers = new ArrayList<>();
        for (SalesforceScanSpec spec : subScan.getScanSpecs()) {
            readers.add(new SalesforceRecordReader(
                    context,
                    plugin.getConnection(),
                    spec,
                    subScan.getFullSchema(),
                    plugin.getConf()));
        }

        return new ScanOperator(fec, subScan, context,
                RecordReaderIterator.from(readers.iterator()));
    }
}
