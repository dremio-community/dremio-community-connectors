/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.stripe;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/** Converts a {@link StripeScanDrel} (logical) into a {@link StripeScanPrel} (physical). */
public class StripeScanPrule extends RelOptRule {

    public static final StripeScanPrule INSTANCE = new StripeScanPrule();

    private StripeScanPrule() {
        super(RelOptHelper.any(StripeScanDrel.class, Rel.LOGICAL), "StripeScanPrule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return call.rel(0) instanceof StripeScanDrel;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StripeScanDrel drel = call.rel(0);
        StripeGroupScan groupScan = new StripeGroupScan(
                OpProps.prototype(0),
                drel.getTableMetadata(),
                drel.getProjectedColumns(),
                drel.getPluginId(),
                drel.getScanSpec());

        call.transformTo(new StripeScanPrel(
                drel.getCluster(),
                drel.getTraitSet().replace(Prel.PHYSICAL),
                drel.getTable(),
                drel.getPluginId(),
                drel.getTableMetadata(),
                drel.getProjectedColumns(),
                drel.getCostAdjustmentFactor(),
                drel.getHints(),
                groupScan,
                drel.getScanSpec()));
    }
}
