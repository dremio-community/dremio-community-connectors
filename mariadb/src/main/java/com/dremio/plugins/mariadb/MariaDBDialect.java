package com.dremio.plugins.mariadb;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;

/**
 * MariaDB ARP dialect.
 *
 * <p>All SQL translation, type mapping, and function support is driven by
 * {@code arp/implementation/mariadb-arp.yaml}. This class is a minimal
 * extension point — the ARP framework handles everything declaratively.</p>
 *
 * <p>MariaDB is highly compatible with ANSI SQL and supports standard
 * {@code DATE '...'} and {@code TIMESTAMP '...'} literals, standard CAST syntax,
 * and all common SQL functions, so no dialect overrides are needed.</p>
 */
public class MariaDBDialect extends ArpDialect {

  public MariaDBDialect(ArpYaml yaml) {
    super(yaml);
  }
}
