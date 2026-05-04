package com.dremio.plugins.singlestore;

import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;

/**
 * ARP SQL dialect for SingleStore.
 *
 * <p>SingleStore reports as MySQL 5.7-compatible and uses backtick identifier quoting.
 * It does not support ANSI SQL date/time literal syntax ({@code DATE '...'} /
 * {@code TIMESTAMP '...'}), so those are rewritten to plain string literals.</p>
 */
public class SingleStoreDialect extends ArpDialect {

  public SingleStoreDialect(ArpYaml yaml) {
    super(yaml);
  }

  /**
   * SingleStore does not support ANSI date/time literal syntax.
   * Rewrite {@code DATE '...'} → {@code '...'} and
   * {@code TIMESTAMP '...'} → {@code '...'} so pushed-down date filters work.
   */
  @Override
  public void unparseDateTimeLiteral(
      SqlWriter writer,
      SqlAbstractDateTimeLiteral literal,
      int leftPrec,
      int rightPrec) {
    SqlTypeName typeName = literal.getTypeName();
    if (typeName == SqlTypeName.DATE
        || typeName == SqlTypeName.TIMESTAMP
        || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      String val = literal.toFormattedString();
      // Strip fractional seconds if present — SingleStore DATETIME accepts HH:MM:SS only
      int dot = val.indexOf('.');
      if (dot >= 0) {
        val = val.substring(0, dot);
      }
      writer.literal("'" + val + "'");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }
}
