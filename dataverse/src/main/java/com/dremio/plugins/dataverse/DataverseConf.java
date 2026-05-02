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

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Microsoft Dataverse source configuration.
 *
 * <p>Authenticates via Azure AD OAuth2 client credentials flow.
 * Queries via OData v4 REST API.
 */
@SourceType(value = "DATAVERSE", label = "Microsoft Dataverse", uiConfig = "dataverse-layout.json")
public class DataverseConf extends ConnectionConf<DataverseConf, DataverseStoragePlugin> {

    /** Organization URL, e.g. https://myorg.api.crm.dynamics.com */
    @Tag(1)
    public String organizationUrl;

    /** Azure AD tenant ID (Directory ID). */
    @Tag(2)
    public String tenantId;

    /** Azure AD app registration client ID. */
    @Tag(3)
    public String clientId;

    /** Azure AD app registration client secret. */
    @Tag(4)
    @Secret
    public String clientSecret;

    /** Dataverse Web API version. */
    @Tag(5)
    @NotMetadataImpacting
    public String apiVersion = "9.2";

    /** Max records per OData page ($top parameter). */
    @Tag(6)
    @NotMetadataImpacting
    public int recordsPerPage = 5000;

    /** Query timeout in seconds. */
    @Tag(7)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    /**
     * Comma-separated list of entity logical names to hide.
     * Example: "activitypointer,asyncoperation"
     */
    @Tag(8)
    @NotMetadataImpacting
    public String excludedEntities = "";

    @Override
    public DataverseStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new DataverseStoragePlugin(this, context, name);
    }
}
