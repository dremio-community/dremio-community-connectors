/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.servicenow;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * ServiceNow source configuration.
 *
 * <p>Authenticates via HTTP Basic Auth using email + API token.
 * Queries via ServiceNow REST API v2.
 */
@SourceType(value = "SERVICENOW_REST", label = "ServiceNow (REST)", uiConfig = "servicenow-layout.json")
public class ServiceNowConf extends ConnectionConf<ServiceNowConf, ServiceNowStoragePlugin> {

    /** ServiceNow Instance URL (e.g. "https://dev12345.service-now.com"). */
    @Tag(1)
    public String instanceUrl;

    /** ServiceNow username used for Basic Auth. */
    @Tag(2)
    public String username;

    /** ServiceNow password used for Basic Auth. */
    @Tag(3)
    @Secret
    public String password;

    /** Number of records per API page (max 100 for most endpoints). */
    @Tag(4)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP request timeout in seconds. */
    @Tag(5)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    @Override
    public ServiceNowStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new ServiceNowStoragePlugin(this, context, name);
    }
}
