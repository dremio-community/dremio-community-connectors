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

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Stripe source configuration.
 *
 * <p>Authenticates via a Stripe secret API key (sk_live_... or sk_test_...).
 * Find or create keys in the Stripe Dashboard → Developers → API keys.
 */
@SourceType(value = "STRIPE", label = "Stripe", uiConfig = "stripe-layout.json")
public class StripeConf extends ConnectionConf<StripeConf, StripeStoragePlugin> {

    /** Stripe secret API key (sk_live_... or sk_test_...). */
    @Tag(1)
    @Secret
    public String apiKey;

    /** Stripe API base URL. Override to http://stripe-mock:12111 for local testing. */
    @Tag(2)
    @NotMetadataImpacting
    public String baseUrl = "https://api.stripe.com";

    /** Records fetched per API call. Stripe maximum is 100. */
    @Tag(3)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP timeout in seconds for calls to Stripe. */
    @Tag(4)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 60;

    @Override
    public StripeStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new StripeStoragePlugin(this, context, name);
    }
}
