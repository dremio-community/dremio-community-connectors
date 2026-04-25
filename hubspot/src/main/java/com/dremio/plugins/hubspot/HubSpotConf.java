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

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * HubSpot source configuration.
 *
 * <p>Authenticates via a HubSpot Private App access token. Create one in
 * HubSpot → Settings → Integrations → Private Apps. The token must have
 * CRM read scopes for the object types you want to query.
 */
@SourceType(value = "HUBSPOT", label = "HubSpot (CRM)", uiConfig = "hubspot-layout.json")
public class HubSpotConf extends ConnectionConf<HubSpotConf, HubSpotStoragePlugin> {

    /** HubSpot Private App access token (pat-na1-...). */
    @Tag(1)
    @Secret
    public String accessToken;

    /** HubSpot API base URL. Override for EU data residency: https://api.hubapi.com (same). */
    @Tag(2)
    @NotMetadataImpacting
    public String baseUrl = "https://api.hubapi.com";

    /** Records fetched per API call. HubSpot maximum is 100. */
    @Tag(3)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP timeout in seconds for calls to HubSpot. */
    @Tag(4)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 60;

    /** When true, archived (soft-deleted) records are included in results. */
    @Tag(5)
    @NotMetadataImpacting
    public boolean includeArchived = false;

    @Override
    public HubSpotStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new HubSpotStoragePlugin(this, context, name);
    }
}
