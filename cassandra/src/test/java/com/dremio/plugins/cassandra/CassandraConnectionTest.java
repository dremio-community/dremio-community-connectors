package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit tests for static helpers on {@link CassandraConnection}:
 *
 * <dl>
 *   <dt>{@code resolveConsistency(config)}</dt>
 *   <dd>Pure logic — no mocks needed. Verifies LOCAL_ relaxation when fallback
 *       DCs are configured, and pass-through for cross-DC levels.</dd>
 *
 *   <dt>{@code detectDatacenterFromSession(session, contactPoints)}</dt>
 *   <dd>Mocks CqlSession / ResultSet / Row to test the three decision paths:
 *       (1) contact-point IP scoring, (2) fall back to local DC when no IPs match,
 *       (3) fall back to "datacenter1" when system.local returns no row.</dd>
 * </dl>
 */
class CassandraConnectionTest {

  // ============================================================================
  // resolveConsistency() — pure logic tests
  // ============================================================================

  /** Null consistencyLevel defaults to "LOCAL_ONE". */
  @Test
  void resolveConsistency_nullLevel_defaultsToLocalOne() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = null;
    cfg.fallbackDatacenters = null;
    assertEquals("LOCAL_ONE", CassandraConnection.resolveConsistency(cfg));
  }

  /** Blank consistencyLevel defaults to "LOCAL_ONE". */
  @Test
  void resolveConsistency_blankLevel_defaultsToLocalOne() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "   ";
    cfg.fallbackDatacenters = null;
    assertEquals("LOCAL_ONE", CassandraConnection.resolveConsistency(cfg));
  }

  /** Input is normalized to upper case. */
  @Test
  void resolveConsistency_lowercaseInput_normalizedToUppercase() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "local_one";
    cfg.fallbackDatacenters = null;
    assertEquals("LOCAL_ONE", CassandraConnection.resolveConsistency(cfg));
  }

  /** No fallback DCs configured — LOCAL_ONE is returned unchanged. */
  @Test
  void resolveConsistency_localOne_noFallback_returnsLocalOne() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "LOCAL_ONE";
    cfg.fallbackDatacenters = null;
    assertEquals("LOCAL_ONE", CassandraConnection.resolveConsistency(cfg));
  }

  /** Fallback DCs configured — LOCAL_ONE relaxes to ONE so remote DCs are reachable. */
  @Test
  void resolveConsistency_localOne_withFallback_relaxesToOne() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "LOCAL_ONE";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("ONE", CassandraConnection.resolveConsistency(cfg));
  }

  /** Fallback DCs configured — LOCAL_QUORUM relaxes to QUORUM. */
  @Test
  void resolveConsistency_localQuorum_withFallback_relaxesToQuorum() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "LOCAL_QUORUM";
    cfg.fallbackDatacenters = "dc2,dc3";
    assertEquals("QUORUM", CassandraConnection.resolveConsistency(cfg));
  }

  /** Fallback DCs configured — LOCAL_SERIAL relaxes to SERIAL. */
  @Test
  void resolveConsistency_localSerial_withFallback_relaxesToSerial() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "LOCAL_SERIAL";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("SERIAL", CassandraConnection.resolveConsistency(cfg));
  }

  /** Non-LOCAL levels pass through unchanged regardless of fallback config. */
  @Test
  void resolveConsistency_one_withFallback_returnedUnchanged() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "ONE";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("ONE", CassandraConnection.resolveConsistency(cfg));
  }

  @Test
  void resolveConsistency_quorum_withFallback_returnedUnchanged() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "QUORUM";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("QUORUM", CassandraConnection.resolveConsistency(cfg));
  }

  @Test
  void resolveConsistency_all_withFallback_returnedUnchanged() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "ALL";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("ALL", CassandraConnection.resolveConsistency(cfg));
  }

  @Test
  void resolveConsistency_eachQuorum_withFallback_returnedUnchanged() {
    CassandraStoragePluginConfig cfg = new CassandraStoragePluginConfig();
    cfg.consistencyLevel   = "EACH_QUORUM";
    cfg.fallbackDatacenters = "dc2";
    assertEquals("EACH_QUORUM", CassandraConnection.resolveConsistency(cfg));
  }

  // ============================================================================
  // detectDatacenterFromSession() — Mockito-based tests
  // ============================================================================

  /**
   * Helper: create a mock Row for system.local with the given DC and broadcast IP.
   * rpc_address and listen_address are unstubbed (return null from Mockito).
   */
  private static Row mockLocalRow(String dc, InetAddress broadcastAddr) {
    Row row = mock(Row.class);
    when(row.getString("data_center")).thenReturn(dc);
    when(row.getInetAddress("broadcast_address")).thenReturn(broadcastAddr);
    // Leave rpc_address and listen_address returning null (Mockito default)
    return row;
  }

  /**
   * Helper: create a mock Row for system.peers.
   */
  private static Row mockPeerRow(String dc, InetAddress peerAddr) {
    Row row = mock(Row.class);
    when(row.getString("data_center")).thenReturn(dc);
    when(row.getInetAddress("peer")).thenReturn(peerAddr);
    return row;
  }

  /**
   * Helper: build a CqlSession mock that returns a local row and a list of peer rows.
   */
  private static CqlSession mockSession(Row localRow, List<Row> peerRows) {
    CqlSession session = mock(CqlSession.class);

    // system.local query
    ResultSet localRs = mock(ResultSet.class);
    when(localRs.one()).thenReturn(localRow);
    when(session.execute(
        "SELECT data_center, broadcast_address, rpc_address, listen_address FROM system.local"))
        .thenReturn(localRs);

    // system.peers query
    ResultSet peersRs = mock(ResultSet.class);
    when(peersRs.iterator()).thenReturn(peerRows.iterator());
    when(session.execute("SELECT peer, rpc_address, data_center FROM system.peers"))
        .thenReturn(peersRs);

    return session;
  }

  // --------------------------------------------------------------------------

  /**
   * Single-node cluster: the contact point's IP matches the local node's broadcast_address.
   * Expected: the local DC is returned.
   */
  @Test
  void detectDc_contactPointMatchesLocalNode_returnsLocalDc() throws UnknownHostException {
    InetAddress nodeIp = InetAddress.getByName("10.0.0.1");

    Row localRow = mockLocalRow("dc1", nodeIp);
    CqlSession session = mockSession(localRow, Collections.emptyList());

    InetSocketAddress cp = new InetSocketAddress(nodeIp, 9042);
    String dc = CassandraConnection.detectDatacenterFromSession(session, Collections.singletonList(cp));
    assertEquals("dc1", dc);
  }

  /**
   * Multi-DC: two contact points are in dc2 (both matching peers), one contact point is in dc1.
   * Expected: dc2 wins because more contact points matched it.
   */
  @Test
  void detectDc_multiDc_returnsHighestScoringDc() throws UnknownHostException {
    InetAddress localIp = InetAddress.getByName("10.0.0.1");
    InetAddress peer1   = InetAddress.getByName("10.0.1.1");
    InetAddress peer2   = InetAddress.getByName("10.0.1.2");

    Row localRow = mockLocalRow("dc1", localIp);
    Row peerRow1 = mockPeerRow("dc2", peer1);
    Row peerRow2 = mockPeerRow("dc2", peer2);

    CqlSession session = mockSession(localRow, Arrays.asList(peerRow1, peerRow2));

    // cp1 → dc1, cp2 → dc2, cp3 → dc2
    List<InetSocketAddress> contactPoints = Arrays.asList(
        new InetSocketAddress(localIp, 9042),
        new InetSocketAddress(peer1,   9042),
        new InetSocketAddress(peer2,   9042));

    String dc = CassandraConnection.detectDatacenterFromSession(session, contactPoints);
    assertEquals("dc2", dc);
  }

  /**
   * No contact point IPs appear in the topology map (e.g., NAT or hostnames).
   * Expected: fall back to the local node's DC from system.local.
   */
  @Test
  void detectDc_noContactPointsMatchTopology_fallsBackToLocalDc() throws UnknownHostException {
    InetAddress localIp     = InetAddress.getByName("10.0.0.1");
    InetAddress unrelatedIp = InetAddress.getByName("192.168.99.1"); // not in topology

    Row localRow = mockLocalRow("dc1", localIp);
    CqlSession session = mockSession(localRow, Collections.emptyList());

    // Contact point with an IP not found in the topology
    InetSocketAddress cp = new InetSocketAddress(unrelatedIp, 9042);
    String dc = CassandraConnection.detectDatacenterFromSession(session, Collections.singletonList(cp));
    assertEquals("dc1", dc);
  }

  /**
   * Empty contact points list: no scoring possible.
   * Expected: fall back to the local node's DC.
   */
  @Test
  void detectDc_emptyContactPoints_fallsBackToLocalDc() throws UnknownHostException {
    InetAddress localIp = InetAddress.getByName("10.0.0.1");
    Row localRow = mockLocalRow("dc-east", localIp);
    CqlSession session = mockSession(localRow, Collections.emptyList());

    String dc = CassandraConnection.detectDatacenterFromSession(session, Collections.emptyList());
    assertEquals("dc-east", dc);
  }

  /**
   * system.local returns no row (extremely unexpected but must not NPE).
   * Expected: fall back to the hardcoded default "datacenter1".
   */
  @Test
  void detectDc_systemLocalReturnsNoRow_fallsBackToDatacenter1() {
    CqlSession session = mock(CqlSession.class);

    ResultSet localRs = mock(ResultSet.class);
    when(localRs.one()).thenReturn(null); // no row
    when(session.execute(
        "SELECT data_center, broadcast_address, rpc_address, listen_address FROM system.local"))
        .thenReturn(localRs);

    // system.peers should not be queried in this path, but mock it defensively
    ResultSet peersRs = mock(ResultSet.class);
    when(peersRs.iterator()).thenReturn(Collections.<Row>emptyList().iterator());
    when(session.execute("SELECT peer, rpc_address, data_center FROM system.peers"))
        .thenReturn(peersRs);

    String dc = CassandraConnection.detectDatacenterFromSession(session, Collections.emptyList());
    assertEquals("datacenter1", dc);
  }

  /**
   * A peer row with a null DC (e.g., decommissioning node) must be skipped gracefully.
   * Expected: does not throw, returns local DC.
   */
  @Test
  void detectDc_peerRowWithNullDc_skippedGracefully() throws UnknownHostException {
    InetAddress localIp = InetAddress.getByName("10.0.0.1");
    Row localRow = mockLocalRow("dc1", localIp);

    // Peer with null DC
    Row badPeer = mock(Row.class);
    when(badPeer.getString("data_center")).thenReturn(null);
    when(badPeer.getInetAddress("peer")).thenReturn(InetAddress.getByName("10.0.0.99"));

    CqlSession session = mockSession(localRow, Collections.singletonList(badPeer));

    InetSocketAddress cp = new InetSocketAddress(localIp, 9042);
    // Should not throw; the badPeer is silently skipped
    String dc = CassandraConnection.detectDatacenterFromSession(session, Collections.singletonList(cp));
    assertEquals("dc1", dc);
  }
}
