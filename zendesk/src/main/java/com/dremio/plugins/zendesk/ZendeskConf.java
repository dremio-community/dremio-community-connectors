/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.zendesk;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Zendesk source configuration.
 *
 * <p>Authenticates via HTTP Basic Auth using email + API token.
 * Queries via Zendesk REST API v2.
 */
@SourceType(value = "ZENDESK_REST", label = "Zendesk (REST)", uiConfig = "zendesk-layout.json")
public class ZendeskConf extends ConnectionConf<ZendeskConf, ZendeskStoragePlugin> {

    /** Zendesk subdomain (e.g. "mycompany" for mycompany.zendesk.com). */
    @Tag(1)
    public String subdomain;

    /** Zendesk agent email address used for authentication. */
    @Tag(2)
    public String email;

    /** Zendesk API token (generated under Admin → Apps & Integrations → Zendesk API). */
    @Tag(3)
    @Secret
    public String apiToken;

    /** Number of records per API page (max 100 for most endpoints). */
    @Tag(4)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP request timeout in seconds. */
    @Tag(5)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    @Override
    public ZendeskStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new ZendeskStoragePlugin(this, context, name);
    }
}
