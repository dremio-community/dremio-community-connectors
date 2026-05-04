/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.jira;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

@SourceType(value = "JIRA", label = "Jira", uiConfig = "jira-layout.json")
public class JiraConf extends ConnectionConf<JiraConf, JiraStoragePlugin> {

    /** Jira Cloud domain — just the subdomain, e.g. mycompany (for mycompany.atlassian.net). */
    @Tag(1)
    public String domain;

    /** Account email address used for Basic authentication. */
    @Tag(2)
    public String email;

    /** Jira API token. Generate at id.atlassian.com → Security → API tokens. */
    @Tag(3)
    @Secret
    public String apiToken;

    /** JQL filter applied to the issues table. Default returns all issues. */
    @Tag(4)
    @NotMetadataImpacting
    public String issueJql = "project IS NOT EMPTY ORDER BY created ASC";

    /** Records per API page for paginated tables (max 100 for issues). */
    @Tag(5)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP connect/read timeout in seconds. */
    @Tag(6)
    @NotMetadataImpacting
    public int timeoutSeconds = 60;

    @Override
    public JiraStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new JiraStoragePlugin(this, context, name);
    }
}
