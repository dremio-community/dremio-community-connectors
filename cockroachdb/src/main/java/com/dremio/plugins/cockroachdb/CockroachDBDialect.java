package com.dremio.plugins.cockroachdb;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;

public class CockroachDBDialect extends ArpDialect {

  public CockroachDBDialect(ArpYaml yaml) {
    super(yaml);
  }
}
