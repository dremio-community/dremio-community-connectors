/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.prometheus;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

@SourceType(value = "PROMETHEUS", label = "Prometheus", uiConfig = "prometheus-layout.json")
public class PrometheusConf extends ConnectionConf<PrometheusConf, PrometheusStoragePlugin> {

    /** Prometheus base URL, e.g. http://localhost:9090 */
    @Tag(1)
    public String url;

    /** Username for Basic auth (optional). */
    @Tag(2)
    public String username;

    /** Password for Basic auth (optional). */
    @Tag(3)
    @Secret
    public String password;

    /** Bearer token for Grafana Cloud / Prometheus with token auth (optional). */
    @Tag(4)
    @Secret
    public String bearerToken;

    /** PromQL expression used to populate the samples table. Default fetches the "up" metric. */
    @Tag(5)
    @NotMetadataImpacting
    public String samplesQuery = "up";

    /** Number of hours of history to fetch for the samples table. */
    @Tag(6)
    @NotMetadataImpacting
    public int lookbackHours = 1;

    /** Step interval in seconds for range queries (samples table). */
    @Tag(7)
    @NotMetadataImpacting
    public int stepSeconds = 60;

    /** HTTP connect/read timeout in seconds. */
    @Tag(8)
    @NotMetadataImpacting
    public int timeoutSeconds = 30;

    @Override
    public PrometheusStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new PrometheusStoragePlugin(this, context, name);
    }
}
