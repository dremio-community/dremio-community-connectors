package com.dremio.plugins.pagerduty;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * PagerDuty source configuration.
 *
 * <p>Authenticates via PagerDuty REST API v2 API Token.
 */
@SourceType(value = "PAGERDUTY_REST", label = "PagerDuty (REST)", uiConfig = "pagerduty-layout.json")
public class PagerDutyConf extends ConnectionConf<PagerDutyConf, PagerDutyStoragePlugin> {

    /** PagerDuty REST API token (v2). */
    @Tag(1)
    @Secret
    public String apiToken;

    /** Whether PagerDuty account is in EU region (uses api.eu.pagerduty.com). */
    @Tag(2)
    public boolean euRegion = false;

    /** Number of records per API page (max 100). */
    @Tag(3)
    @NotMetadataImpacting
    public int pageSize = 100;

    /** HTTP request timeout in seconds. */
    @Tag(4)
    @NotMetadataImpacting
    public int queryTimeoutSeconds = 120;

    @Override
    public PagerDutyStoragePlugin newPlugin(PluginSabotContext context, String name,
            Provider<StoragePluginId> pluginIdProvider) {
        return new PagerDutyStoragePlugin(this, context, name);
    }
}
