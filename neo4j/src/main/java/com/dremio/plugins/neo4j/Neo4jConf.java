package com.dremio.plugins.neo4j;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import io.protostuff.Tag;
import javax.inject.Provider;

@SourceType(value = "NEO4J", label = "Neo4j", uiConfig = "neo4j-layout.json")
public class Neo4jConf extends ConnectionConf<Neo4jConf, Neo4jStoragePlugin> {

  @Tag(1)
  @DisplayMetadata(label = "Bolt URI")
  public String boltUri = "bolt://localhost:7687";

  @Tag(2)
  @DisplayMetadata(label = "Username")
  public String username = "neo4j";

  @Tag(3)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;

  @Tag(4)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Database")
  public String database = "neo4j";

  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Schema Sample Size")
  public int sampleSize = 50;

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Fetch Batch Size")
  public int fetchBatchSize = 500;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Connection Timeout (seconds)")
  public int connectionTimeoutSeconds = 30;

  @Override
  public Neo4jStoragePlugin newPlugin(PluginSabotContext context, String name,
                                      Provider<StoragePluginId> pluginIdProvider) {
    return new Neo4jStoragePlugin(this, context, name);
  }
}
