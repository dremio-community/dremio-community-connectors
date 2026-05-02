/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

public class DataverseRulesFactory extends StoragePluginTypeRulesFactory {

    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                     PlannerPhase phase,
                                     SourceType pluginType) {
        switch (phase) {
            case LOGICAL:
                return ImmutableSet.of(DataverseScanRule.INSTANCE, DataverseFilterRule.INSTANCE);
            case PHYSICAL:
                return ImmutableSet.of(DataverseScanPrule.INSTANCE);
            default:
                return ImmutableSet.of();
        }
    }
}
