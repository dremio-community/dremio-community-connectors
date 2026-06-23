package com.dremio.plugins.googleads;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

@SourceType(value = "GOOGLE_ADS", label = "Google Ads", uiConfig = "googleads-layout.json")
public class GoogleAdsConf extends ConnectionConf<GoogleAdsConf, GoogleAdsStoragePlugin> {

  /** Google Ads developer token (from API Center in your manager account). */
  @Tag(1)
  public String developerToken = "";

  /** OAuth2 client ID from Google Cloud Console. */
  @Tag(2)
  public String clientId = "";

  /** OAuth2 client secret from Google Cloud Console. */
  @Tag(3)
  public String clientSecret = "";

  /** OAuth2 refresh token (long-lived, obtained via consent flow). */
  @Tag(4)
  public String refreshToken = "";

  /** Google Ads customer ID (10 digits, no dashes). */
  @Tag(5)
  public String customerId = "";

  /** MCC / manager account ID (optional). */
  @Tag(6)
  public String loginCustomerId = "";

  /** How many days back to pull for performance report tables (default 30). */
  @Tag(7)
  public int dateRangeDays = 30;

  @Override
  public GoogleAdsStoragePlugin newPlugin(PluginSabotContext context, String name,
                                          Provider<StoragePluginId> pluginIdProvider) {
    return new GoogleAdsStoragePlugin(this, context, name);
  }
}
