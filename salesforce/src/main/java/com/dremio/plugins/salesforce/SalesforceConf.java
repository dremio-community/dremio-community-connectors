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

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Salesforce source configuration.
 *
 * <p>Authenticates via OAuth2 password flow (Connected App credentials + username/password).
 * Queries via SOQL over the Salesforce REST API.
 */
@SourceType(value = "SALESFORCE_REST", label = "Salesforce (REST)", uiConfig = "salesforce-layout.json")
public class SalesforceConf extends ConnectionConf<SalesforceConf, SalesforceStoragePlugin> {

    /** OAuth2 login endpoint. Use https://test.salesforce.com for sandboxes. */
    @Tag(1)
    public String loginUrl = "https://login.salesforce.com";

    /** Salesforce username (email format). */
    @Tag(2)
    public String username;

    /** Salesforce password. */
    @Tag(3)
    @Secret
    public String password;

    /**
     * Salesforce security token. Appended to password in the OAuth call.
     * Leave blank if your org's IP is trusted.
     */
    @Tag(4)
    @Secret
    public String securityToken = "";

    /** Connected App consumer key (client_id). */
    @Tag(5)
    public String clientId;

    /** Connected App consumer secret (client_secret). */
    @Tag(6)
    @Secret
    public String clientSecret;

    /** Salesforce REST API version (e.g. "59.0"). */
    @Tag(7)
    @NotMetadataImpacting
    public String apiVersion = "59.0";

    /** Number of records per Salesforce query page. Max 2000. */
    @Tag(8)
    @NotMetadataImpacting
    public int recordsPerPage = 2000;

    /** Number of parallel reader splits. */
    @Tag(9)
    @NotMetadataImpacting
    public int splitParallelism = 4;

    /** Query timeout in seconds for HTTP calls to Salesforce. */
    @Tag(10)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    /**
     * Comma-separated list of SObject API names to hide from the dataset listing.
     * Example: "ContentVersion,FeedItem,CollaborationGroup"
     */
    @Tag(11)
    @NotMetadataImpacting
    public String excludedObjects = "";

    @Override
    public SalesforceStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new SalesforceStoragePlugin(this, context, name);
    }
}
