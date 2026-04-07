package com.dremio.plugins.splunk;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

import javax.inject.Provider;

/**
 * Dremio source configuration for Splunk.
 *
 * Splunk indexes are exposed as tables. Queries are translated to SPL
 * (Splunk Processing Language) with time-range and field-equality pushdown.
 *
 * Supports Splunk on-prem (default port 8089) and Splunk Cloud (port 443).
 * Authentication: username/password (session key) or bearer token (JWT).
 */
@SourceType(value = "SPLUNK", label = "Splunk", uiConfig = "splunk-layout.json")
public class SplunkConf extends ConnectionConf<SplunkConf, SplunkStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /** Splunk server hostname or IP. */
  @Tag(1)
  @DisplayMetadata(label = "Hostname")
  public String hostname = "localhost";

  /**
   * Splunk management REST API port.
   * Default: 8089 (on-prem). Splunk Cloud uses 443.
   * Overridden automatically when splunkCloud = true.
   */
  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 8089;

  /** Enable HTTPS. Always true for Splunk Cloud. Recommended for on-prem. */
  @Tag(3)
  @DisplayMetadata(label = "Use SSL / HTTPS")
  public boolean useSsl = true;

  /**
   * Skip SSL certificate verification.
   * Useful for self-signed certs in development. Do NOT enable in production.
   */
  @Tag(4)
  @DisplayMetadata(label = "Disable SSL Certificate Verification")
  public boolean disableSslVerification = false;

  // -----------------------------------------------------------------------
  // Authentication
  // -----------------------------------------------------------------------

  /**
   * Splunk username. Used with password auth.
   * Leave blank when using authToken.
   */
  @Tag(5)
  @DisplayMetadata(label = "Username")
  public String username = "";

  /**
   * Splunk password. Used with username auth.
   * Dremio obtains a session key via POST /services/auth/login.
   */
  @Tag(6)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password = "";

  /**
   * Bearer token for authentication (Splunk Cloud JWT tokens or on-prem tokens).
   * When set, takes priority over username/password.
   * Format: the raw token string (without "Bearer " prefix).
   */
  @Tag(7)
  @Secret
  @DisplayMetadata(label = "Auth Token (optional — overrides username/password)")
  public String authToken = "";

  // -----------------------------------------------------------------------
  // Splunk Cloud
  // -----------------------------------------------------------------------

  /**
   * Enable Splunk Cloud mode. Forces port=443 and SSL=true.
   * Use bearer token authentication for Splunk Cloud.
   */
  @Tag(8)
  @DisplayMetadata(label = "Splunk Cloud")
  public boolean splunkCloud = false;

  // -----------------------------------------------------------------------
  // Scan behavior
  // -----------------------------------------------------------------------

  /**
   * Default time window for unfiltered scans (no WHERE _time clause).
   * Uses Splunk relative time syntax: -24h, -7d, -30d@d, etc.
   * Prevents full-index scans on large deployments.
   */
  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default Earliest Time (for unfiltered scans)")
  public String defaultEarliest = "-24h";

  /**
   * Maximum events to return per query when no LIMIT is specified.
   * Splunk truncates silently at job max_count; always set explicitly.
   */
  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default Max Events Per Query")
  public int defaultMaxEvents = 50000;

  // -----------------------------------------------------------------------
  // Schema inference
  // -----------------------------------------------------------------------

  /**
   * Number of recent events to sample per index for schema inference.
   * Higher values give more accurate type inference but slower metadata refresh.
   */
  @Tag(11)
  @DisplayMetadata(label = "Sample Events For Schema Inference")
  public int sampleEventsForSchema = 200;

  /**
   * How long to cache index list and inferred schemas (seconds).
   * Set to 0 to always fetch fresh (slower but always up-to-date).
   */
  @Tag(12)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Metadata Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 300;

  // -----------------------------------------------------------------------
  // Index filtering
  // -----------------------------------------------------------------------

  /**
   * Java regex for Splunk indexes to hide from the Dremio catalog.
   * Default hides Splunk internal indexes (starting with _ or named "history"/"fishbucket").
   */
  @Tag(13)
  @DisplayMetadata(label = "Index Exclude Pattern (regex)")
  public String indexExcludePattern = "^(_.*|history|fishbucket|lastchanceindex|cim_modactions)$";

  /**
   * Optional Java regex. When set, only matching indexes appear in Dremio.
   * Leave blank to include all non-excluded indexes.
   */
  @Tag(14)
  @DisplayMetadata(label = "Index Include Pattern (regex)")
  public String indexIncludePattern = "";

  // -----------------------------------------------------------------------
  // Performance
  // -----------------------------------------------------------------------

  /** HTTP connect timeout in seconds. */
  @Tag(15)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Connection Timeout (seconds)")
  public int connectionTimeoutSeconds = 30;

  /** HTTP read/socket timeout in seconds. For large queries, increase this. */
  @Tag(16)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Read Timeout (seconds)")
  public int readTimeoutSeconds = 300;

  /**
   * Splunk search mode: normal, fast, or verbose.
   * fast: disables field extraction (faster for large scans).
   * normal: balanced (default).
   * verbose: all fields extracted (slower).
   */
  @Tag(17)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Search Mode (normal / fast / verbose)")
  public String searchMode = "normal";

  /** Number of events per results page when paginating through job results. */
  @Tag(18)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Results Page Size")
  public int resultsPageSize = 5000;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public SplunkStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new SplunkStoragePlugin(this, context, name);
  }
}
