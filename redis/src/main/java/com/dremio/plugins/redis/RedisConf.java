package com.dremio.plugins.redis;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import io.protostuff.Tag;
import javax.inject.Provider;

@SourceType(value = "REDIS", label = "Redis", uiConfig = "redis-layout.json")
public class RedisConf extends ConnectionConf<RedisConf, RedisPlugin> {

  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String host = "localhost";

  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 6379;

  @Tag(3)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password = "";

  @Tag(4)
  @DisplayMetadata(label = "Database Index")
  public int database = 0;

  @Tag(5)
  @DisplayMetadata(label = "Key Delimiter")
  public String keyDelimiter = ":";

  @Tag(6)
  @DisplayMetadata(label = "Schema Sample Size")
  public int sampleSize = 50;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Scan Count (keys per SCAN batch)")
  public int scanCount = 100;

  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Connection Timeout (ms)")
  public int connectTimeoutMs = 5000;

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Socket Timeout (ms)")
  public int socketTimeoutMs = 30000;

  @Override
  public RedisPlugin newPlugin(PluginSabotContext context, String name,
                                Provider<StoragePluginId> pluginIdProvider) {
    return new RedisPlugin(this, context, name);
  }
}
