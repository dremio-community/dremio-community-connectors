package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Lifecycle wrapper around a DataStax {@link CqlSession}.
 *
 * One CassandraConnection is created per plugin instance (per Dremio node).
 * The session is thread-safe and reused across all queries from this source.
 *
 * <h3>Multi-datacenter routing</h3>
 * <ul>
 *   <li><b>DC auto-detection</b>: if {@code datacenter} is blank, a lightweight bootstrap
 *       session queries {@code system.local} to discover the local datacenter name.</li>
 *   <li><b>Fallback DCs</b>: if {@code fallbackDatacenters} is non-empty, {@code LOCAL_}
 *       consistency levels are automatically relaxed ({@code LOCAL_ONE → ONE},
 *       {@code LOCAL_QUORUM → QUORUM}) so the DataStax driver's built-in remote-DC
 *       fallback path is unlocked.</li>
 * </ul>
 *
 * <h3>SSL / TLS and mTLS</h3>
 * <ul>
 *   <li><b>One-way TLS</b>: set {@code sslEnabled=true}. The JVM default trust store is
 *       used unless {@code sslTruststorePath} points to a custom JKS/PKCS12 file.</li>
 *   <li><b>mTLS</b>: additionally set {@code sslKeystorePath} to a keystore containing
 *       the client certificate and private key. The same password is used for the
 *       keystore and the key entry.</li>
 *   <li><b>Hostname verification</b>: enabled by default; set
 *       {@code sslHostnameVerification=false} only for self-signed certs in dev/test.</li>
 *   <li>SSL is also applied to the lightweight DC auto-detection bootstrap session so
 *       clusters that mandate TLS for all connections are handled correctly.</li>
 * </ul>
 */
public class CassandraConnection implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

  private final CqlSession session;

  public CassandraConnection(CassandraStoragePluginConfig config) {

    List<InetSocketAddress> contactPoints = Arrays.stream(config.host.split(","))
        .map(String::trim)
        .filter(h -> !h.isEmpty())
        .map(h -> new InetSocketAddress(h, config.port))
        .collect(Collectors.toList());

    // --- 0. Validate contact points --------------------------------------
    // InetSocketAddress(hostname, port) attempts DNS resolution immediately.
    // If a hostname does not resolve, isUnresolved() == true and the DataStax
    // driver will skip that node.  Warn per bad entry; fail fast if none resolve.
    if (contactPoints.isEmpty()) {
      throw new RuntimeException(
          "No contact points configured — 'host' field is blank or contains only whitespace. " +
          "Raw value: '" + config.host + "'");
    }

    int resolvedCount = 0;
    for (InetSocketAddress addr : contactPoints) {
      if (addr.isUnresolved()) {
        logger.warn("Contact point '{}:{}' could not be resolved (DNS lookup failed). " +
                    "Verify the hostname is correct and reachable from this Dremio node. " +
                    "The DataStax driver will skip this contact point.",
                    addr.getHostString(), addr.getPort());
      } else {
        logger.debug("Contact point '{}' resolved → {}:{}",
                     addr.getHostString(),
                     addr.getAddress().getHostAddress(),
                     addr.getPort());
        resolvedCount++;
      }
    }

    if (resolvedCount == 0) {
      throw new RuntimeException(
          "None of the " + contactPoints.size() + " configured contact point(s) could be resolved: " +
          "[" + config.host + "]. " +
          "Check hostnames/IPs, DNS configuration, and network connectivity from this Dremio node.");
    }

    if (resolvedCount < contactPoints.size()) {
      logger.warn("{}/{} contact point(s) resolved successfully — proceeding with reachable nodes only.",
                  resolvedCount, contactPoints.size());
    } else {
      logger.info("All {} contact point(s) resolved successfully.", resolvedCount);
    }

    // --- 1. Resolve datacenter -------------------------------------------
    // If datacenter is blank, bootstrap-detect from system.local.
    String resolvedDc = (config.datacenter == null || config.datacenter.isBlank())
        ? detectDatacenter(contactPoints, config)
        : config.datacenter;

    // --- 2. Resolve consistency level ------------------------------------
    // Relax LOCAL_ variants when fallback DCs are configured so the driver
    // can route to remote nodes without throwing an unavailable error.
    String resolvedConsistency = resolveConsistency(config);

    logger.info("Opening Cassandra connection to {} (datacenter='{}', consistency={}, fallbackDCs='{}')",
        config.host, resolvedDc, resolvedConsistency,
        config.fallbackDatacenters != null ? config.fallbackDatacenters : "");

    // --- 3. Build the real session ----------------------------------------
    ProgrammaticDriverConfigLoaderBuilder configBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofMillis(config.readTimeoutMs))
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, config.fetchSize)
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, resolvedConsistency);

    // Protocol-level compression (LZ4 or SNAPPY). Reduces wire bandwidth at
    // the cost of CPU. Recommended for WAN / cross-AZ deployments; leave NONE
    // on fast local networks.
    if (config.compressionAlgorithm != null
        && !config.compressionAlgorithm.isBlank()
        && !config.compressionAlgorithm.equalsIgnoreCase("NONE")) {
      String compression = config.compressionAlgorithm.toUpperCase();
      configBuilder.withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression);
      logger.info("CQL protocol compression enabled: {}", compression);
    }

    if (config.speculativeExecutionEnabled) {
      configBuilder
          .withString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
              "com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy")
          .withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, 2)
          .withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
              Duration.ofMillis(config.speculativeExecutionDelayMs));
    }

    // Build the CqlSessionBuilder without a config loader yet — SSL setup may
    // need to add SSL_HOSTNAME_VALIDATION to configBuilder before we seal it.
    CqlSessionBuilder builder = CqlSession.builder()
        .addContactPoints(contactPoints)
        .withLocalDatacenter(resolvedDc);

    if (config.username != null && !config.username.isEmpty()) {
      builder.withAuthCredentials(config.username,
          config.password != null ? config.password : "");
    }

    if (config.sslEnabled) {
      try {
        applySslToBuilder(builder, configBuilder, config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize SSL context for Cassandra connection", e);
      }
    }

    // Seal the driver config (includes SSL_HOSTNAME_VALIDATION if set above) and apply it.
    builder.withConfigLoader(configBuilder.build());

    this.session = builder.build();
    logger.info("Cassandra session established (cluster={}, localDc='{}')",
        session.getMetadata().getClusterName().orElse("unknown"), resolvedDc);
  }

  /**
   * Returns the live CQL session. Thread-safe; reuse freely.
   */
  public CqlSession getSession() {
    return session;
  }

  @Override
  public void close() {
    if (session != null && !session.isClosed()) {
      logger.info("Closing Cassandra session");
      session.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Multi-DC helpers
  // ---------------------------------------------------------------------------

  /**
   * Opens a short-lived bootstrap CQL session to auto-detect the best local datacenter
   * for the configured contact points.
   *
   * <h4>Algorithm</h4>
   * <ol>
   *   <li>Query {@code system.local} — get the connecting node's DC and its IP addresses
   *       ({@code broadcast_address}, {@code rpc_address}, {@code listen_address}).</li>
   *   <li>Query {@code system.peers} — get every other node's DC and its peer/rpc IPs.</li>
   *   <li>Build an {@code IP → DC} map from both result sets, recording every reachable
   *       address for each node (peer addr, rpc_address, etc.).</li>
   *   <li>For each configured contact point, resolve its IP and look it up in the map.
   *       Tally a score per DC.</li>
   *   <li>Return the DC with the highest score (tie-break: prefer the local node's DC).</li>
   *   <li>If no contact point matched any known node (single-node cluster, NAT, hostnames
   *       not yet resolvable, etc.), fall back to the local node's DC — still better than
   *       a hard-coded default.</li>
   *   <li>Fall back to {@code "datacenter1"} only if the bootstrap session itself fails.</li>
   * </ol>
   *
   * <p>The bootstrap session uses {@code "datacenter1"} as a placeholder local-DC name.
   * The DataStax driver always connects to explicitly-provided contact points regardless
   * of their actual DC, so the bootstrap query always succeeds as long as at least one
   * contact point is reachable.
   *
   * @param contactPoints  the same contact points used for the real session
   * @param config         plugin config (auth, SSL, timeout)
   * @return               the best datacenter name; never null
   */
  static String detectDatacenter(List<InetSocketAddress> contactPoints,
                                  CassandraStoragePluginConfig config) {
    logger.info("Local datacenter not configured — auto-detecting from cluster topology");

    // Use ONE consistency so the bootstrap queries can reach any node regardless of DC.
    ProgrammaticDriverConfigLoaderBuilder bootstrapConfig =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE");

    // Defer withConfigLoader until after SSL (which may add SSL_HOSTNAME_VALIDATION).
    CqlSessionBuilder tempBuilder = CqlSession.builder()
        .addContactPoints(contactPoints)
        .withLocalDatacenter("datacenter1");   // placeholder — driver still uses explicit contact points

    if (config.username != null && !config.username.isEmpty()) {
      tempBuilder.withAuthCredentials(config.username,
          config.password != null ? config.password : "");
    }

    if (config.sslEnabled) {
      try {
        applySslToBuilder(tempBuilder, bootstrapConfig, config);
      } catch (Exception e) {
        logger.warn("SSL setup failed for bootstrap session ({}), trying without SSL", e.getMessage());
      }
    }

    tempBuilder.withConfigLoader(bootstrapConfig.build());

    try (CqlSession temp = tempBuilder.build()) {
      return detectDatacenterFromSession(temp, contactPoints);
    } catch (Exception e) {
      logger.warn("Could not auto-detect datacenter ({}), defaulting to 'datacenter1'",
          e.getMessage());
      return "datacenter1";
    }
  }

  /**
   * Performs the actual DC-detection queries against an open {@link CqlSession}.
   * Separated from {@link #detectDatacenter} to keep bootstrap session handling clean.
   */
  static String detectDatacenterFromSession(CqlSession session,
                                             List<InetSocketAddress> contactPoints) {
    // ---- Step 1: read system.local -------------------------------------------
    // Defensive SELECT: broadcast_address / rpc_address / listen_address all
    // represent the local node from different perspectives; we collect all of them.
    Row localRow = session.execute(
        "SELECT data_center, broadcast_address, rpc_address, listen_address FROM system.local"
    ).one();

    if (localRow == null) {
      logger.warn("system.local returned no rows — falling back to 'datacenter1'");
      return "datacenter1";
    }

    String localDc = localRow.getString("data_center");

    // ---- Step 2: build IP → DC map from system.local + system.peers ----------
    // We store EVERY IP variant we can find for each node so that contact-point
    // IPs (which might be broadcast_address, rpc_address, or listen_address
    // depending on the cluster's network topology) all map to the right DC.
    Map<InetAddress, String> ipToDc = new HashMap<>();

    addIfNotNull(ipToDc, safeGetInet(localRow, "broadcast_address"), localDc);
    addIfNotNull(ipToDc, safeGetInet(localRow, "rpc_address"),       localDc);
    addIfNotNull(ipToDc, safeGetInet(localRow, "listen_address"),     localDc);

    ResultSet peers = session.execute(
        "SELECT peer, rpc_address, data_center FROM system.peers"
    );
    int peerCount = 0;
    for (Row peer : peers) {
      String peerDc = peer.getString("data_center");
      if (peerDc == null) continue;
      addIfNotNull(ipToDc, safeGetInet(peer, "peer"),        peerDc);
      addIfNotNull(ipToDc, safeGetInet(peer, "rpc_address"), peerDc);
      peerCount++;
    }
    logger.debug("Cluster topology: {} peers discovered, {} IP→DC mappings built",
        peerCount, ipToDc.size());

    // ---- Step 3: score each DC by how many contact points live in it ----------
    Map<String, Integer> dcScore = new LinkedHashMap<>();
    int matched = 0;
    for (InetSocketAddress cp : contactPoints) {
      InetAddress addr = cp.getAddress();   // null if address is unresolved
      if (addr == null) continue;
      String dc = ipToDc.get(addr);
      if (dc != null) {
        dcScore.merge(dc, 1, Integer::sum);
        matched++;
      }
    }

    if (matched > 0) {
      // Pick DC with highest score; tie-break in favour of the local DC.
      String bestDc = dcScore.entrySet().stream()
          .max(Map.Entry.<String, Integer>comparingByValue()
              .thenComparing(e -> e.getKey().equals(localDc) ? 1 : 0))
          .get().getKey();

      long totalNodes = dcScore.values().stream().mapToInt(Integer::intValue).sum();
      logger.info(
          "Auto-detected datacenter '{}' — {}/{} contact points matched known nodes (DC scores: {})",
          bestDc, matched, contactPoints.size(), dcScore);
      return bestDc;
    }

    // ---- Step 4: no contact points matched — use the local node's DC ---------
    // This happens on single-node clusters, when contact points are given as
    // hostnames that resolved to a different IP than system.peers records, or
    // when behind NAT.  The local DC is still the right answer in all these cases.
    logger.info(
        "Auto-detected datacenter '{}' from system.local " +
        "(none of the {} contact point(s) matched known peer IPs — single node or NAT?)",
        localDc, contactPoints.size());
    return localDc;
  }

  /** Adds {@code ip → dc} to the map, silently ignoring null values. */
  private static void addIfNotNull(Map<InetAddress, String> map, InetAddress ip, String dc) {
    if (ip != null && dc != null) {
      map.put(ip, dc);
    }
  }

  /**
   * Safely retrieves an {@link InetAddress} column from a {@link Row}.
   * Returns {@code null} if the column doesn't exist on this Cassandra version
   * or if the value is null/0.0.0.0 (some nodes leave {@code rpc_address} unset).
   */
  private static InetAddress safeGetInet(Row row, String column) {
    try {
      InetAddress addr = row.getInetAddress(column);
      // 0.0.0.0 means "not configured" — treat as absent
      if (addr != null && addr.isAnyLocalAddress()) return null;
      return addr;
    } catch (Exception e) {
      return null;   // column absent on this Cassandra version
    }
  }

  // ---------------------------------------------------------------------------
  // SSL / mTLS helpers
  // ---------------------------------------------------------------------------

  /**
   * Applies SSL/TLS configuration to a {@link CqlSessionBuilder}.
   *
   * <p>Behaviour:
   * <ol>
   *   <li>Builds an {@link SSLContext} via {@link #buildSslContext(CassandraStoragePluginConfig)}.
   *       The context honours custom truststore / keystore paths if configured, or
   *       falls back to the JVM default trust store / no client certificate.</li>
   *   <li>Passes the context to {@link CqlSessionBuilder#withSslContext(SSLContext)}.</li>
   *   <li>If {@code sslHostnameVerification} is {@code false}, disables the DataStax
   *       driver's built-in hostname check via
   *       {@link DefaultDriverOption#SSL_HOSTNAME_VALIDATION}.</li>
   * </ol>
   *
   * @param builder        session builder to configure
   * @param configBuilder  programmatic driver config builder (hostname-validation flag)
   * @param config         plugin configuration
   * @throws Exception if the SSLContext cannot be built (bad path, wrong password, etc.)
   */
  static void applySslToBuilder(CqlSessionBuilder builder,
                                 ProgrammaticDriverConfigLoaderBuilder configBuilder,
                                 CassandraStoragePluginConfig config) throws Exception {
    SSLContext sslContext = buildSslContext(config);
    builder.withSslContext(sslContext);

    // Hostname verification: default true; only disable when the user explicitly opts out.
    if (!config.sslHostnameVerification) {
      configBuilder.withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false);
      logger.warn("TLS hostname verification is DISABLED — only acceptable for dev/test environments");
    }

    boolean hasTruststore = config.sslTruststorePath != null && !config.sslTruststorePath.isBlank();
    boolean hasKeystore   = config.sslKeystorePath   != null && !config.sslKeystorePath.isBlank();
    logger.info("SSL/TLS enabled (truststore={}, mTLS={}, hostnameVerification={})",
        hasTruststore ? config.sslTruststorePath : "JVM-default",
        hasKeystore   ? config.sslKeystorePath   : "none",
        config.sslHostnameVerification);
  }

  /**
   * Builds an {@link SSLContext} from the plugin configuration.
   *
   * <h4>Trust managers</h4>
   * <ul>
   *   <li>If {@code sslTruststorePath} is set, the file is loaded as a JKS or PKCS12
   *       KeyStore and used to construct a {@link TrustManagerFactory}.</li>
   *   <li>Otherwise {@code null} trust managers are passed to
   *       {@link SSLContext#init(KeyManager[], TrustManager[], java.security.SecureRandom)},
   *       which causes the JVM default trust store ({@code $JAVA_HOME/lib/security/cacerts})
   *       to be used — suitable for clusters with certificates signed by well-known CAs.</li>
   * </ul>
   *
   * <h4>Key managers (mTLS)</h4>
   * <ul>
   *   <li>If {@code sslKeystorePath} is set, the keystore is loaded and a
   *       {@link KeyManagerFactory} is initialised with it.  The keystore password is
   *       also used as the key-entry password.</li>
   *   <li>Otherwise {@code null} key managers are passed, meaning no client certificate
   *       is presented (standard one-way TLS).</li>
   * </ul>
   *
   * @param config  plugin configuration
   * @return        a fully initialised {@link SSLContext}
   * @throws Exception if a store file cannot be read or the password is wrong
   */
  static SSLContext buildSslContext(CassandraStoragePluginConfig config) throws Exception {

    // ---- Trust managers --------------------------------------------------
    TrustManager[] trustManagers = null;  // null → JVM default
    if (config.sslTruststorePath != null && !config.sslTruststorePath.isBlank()) {
      String tsType = (config.sslTruststoreType != null && !config.sslTruststoreType.isBlank())
          ? config.sslTruststoreType.toUpperCase() : "JKS";
      char[] tsPass = (config.sslTruststorePassword != null)
          ? config.sslTruststorePassword.toCharArray() : new char[0];

      KeyStore ts = KeyStore.getInstance(tsType);
      try (FileInputStream fis = new FileInputStream(config.sslTruststorePath)) {
        ts.load(fis, tsPass);
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
      trustManagers = tmf.getTrustManagers();
      logger.debug("Loaded truststore '{}' ({}, {} entries)",
          config.sslTruststorePath, tsType, ts.size());
    }

    // ---- Key managers (mTLS) ---------------------------------------------
    KeyManager[] keyManagers = null;  // null → no client certificate
    if (config.sslKeystorePath != null && !config.sslKeystorePath.isBlank()) {
      String ksType = (config.sslKeystoreType != null && !config.sslKeystoreType.isBlank())
          ? config.sslKeystoreType.toUpperCase() : "JKS";
      char[] ksPass = (config.sslKeystorePassword != null)
          ? config.sslKeystorePassword.toCharArray() : new char[0];

      KeyStore ks = KeyStore.getInstance(ksType);
      try (FileInputStream fis = new FileInputStream(config.sslKeystorePath)) {
        ks.load(fis, ksPass);
      }
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, ksPass);  // key-entry password == keystore password
      keyManagers = kmf.getKeyManagers();
      logger.debug("Loaded mTLS keystore '{}' ({}, {} entries)",
          config.sslKeystorePath, ksType, ks.size());
    }

    // ---- Assemble SSLContext ---------------------------------------------
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * Resolves the effective CQL consistency level.
   *
   * When {@code fallbackDatacenters} is configured, {@code LOCAL_} consistency
   * levels are relaxed so the DataStax driver can reach remote-DC nodes when
   * local-DC nodes are unavailable:
   * <ul>
   *   <li>{@code LOCAL_ONE}    → {@code ONE}</li>
   *   <li>{@code LOCAL_QUORUM} → {@code QUORUM}</li>
   *   <li>{@code LOCAL_SERIAL} → {@code SERIAL}</li>
   * </ul>
   * Other levels ({@code ONE}, {@code QUORUM}, {@code ALL}, etc.) are returned
   * unchanged regardless of whether fallback DCs are configured.
   *
   * @param config  plugin config
   * @return        the consistency level string to pass to the driver
   */
  static String resolveConsistency(CassandraStoragePluginConfig config) {
    String raw = (config.consistencyLevel != null && !config.consistencyLevel.isBlank())
        ? config.consistencyLevel.toUpperCase()
        : "LOCAL_ONE";

    boolean hasFallback = config.fallbackDatacenters != null
        && !config.fallbackDatacenters.isBlank();

    if (hasFallback) {
      switch (raw) {
        case "LOCAL_ONE":    return "ONE";
        case "LOCAL_QUORUM": return "QUORUM";
        case "LOCAL_SERIAL": return "SERIAL";
        default:             return raw; // already cross-DC or explicit ALL/EACH_QUORUM
      }
    }
    return raw;
  }
}
