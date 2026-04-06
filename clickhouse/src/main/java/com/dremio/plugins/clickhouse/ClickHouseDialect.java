package com.dremio.plugins.clickhouse;

import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;

/**
 * ClickHouse ARP dialect.
 *
 * <p>All SQL translation, type mapping, and function support is driven by
 * {@code arp/implementation/clickhouse-arp.yaml}. This class is a minimal
 * extension point — the ARP framework handles everything declaratively.</p>
 *
 * <h3>ClickHouse type name wrapping</h3>
 * <p>ClickHouse columns can have wrapper types: {@code Nullable(T)} and
 * {@code LowCardinality(T)}. The official JDBC driver ({@code com.clickhouse:clickhouse-jdbc
 * 0.6.x}) returns the inner type name from {@code ResultSetMetaData.getColumnTypeName()},
 * stripping the wrappers automatically. If a future driver version changes this
 * behaviour, override {@code getDataTypeMapper()} here to install a custom
 * {@code ArpTypeMapper} that pre-processes type names before the YAML lookup.</p>
 */
public class ClickHouseDialect extends ArpDialect {

  public ClickHouseDialect(ArpYaml yaml) {
    super(yaml);
  }

  /**
   * ClickHouse does not support ANSI SQL date/time literal syntax
   * ({@code DATE '...'} / {@code TIMESTAMP '...'}). Rewrite to ClickHouse
   * native functions so pushed-down filters on date/timestamp columns work.
   *
   * <ul>
   *   <li>{@code DATE '2024-03-01'}            → {@code toDate('2024-03-01')}
   *   <li>{@code TIMESTAMP '2024-03-01 12:00:00'} → {@code toDateTime('2024-03-01 12:00:00')}
   * </ul>
   */
  @Override
  public void unparseDateTimeLiteral(
      SqlWriter writer,
      SqlAbstractDateTimeLiteral literal,
      int leftPrec,
      int rightPrec) {
    SqlTypeName typeName = literal.getTypeName();
    if (typeName == SqlTypeName.DATE) {
      writer.literal("toDate('" + literal.toFormattedString() + "')");
    } else if (typeName == SqlTypeName.TIMESTAMP
        || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      // toFormattedString() may include fractional seconds (e.g. "2026-03-03 00:00:00.000").
      // ClickHouse's toDateTime() only accepts "YYYY-MM-DD HH:MM:SS" — strip the fraction.
      String ts = literal.toFormattedString();
      int dot = ts.indexOf('.');
      if (dot >= 0) {
        ts = ts.substring(0, dot);
      }
      writer.literal("toDateTime('" + ts + "')");
    } else {
      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
    }
  }

  /**
   * Intercepts {@code TO_CHAR(datetime_expr, format_string)} calls and rewrites
   * them as ClickHouse {@code formatDateTime(expr, ch_format)}.
   *
   * <p>Dremio/Calcite uses Oracle-style format codes; ClickHouse uses C-style
   * strftime codes. The format string is translated at SQL-generation time so
   * the correct ClickHouse SQL is sent over JDBC.
   *
   * <p>Examples:
   * <pre>
   *   TO_CHAR(order_date, 'YYYY-MM-DD')        → formatDateTime(order_date, '%Y-%m-%d')
   *   TO_CHAR(order_date, 'YYYY-MM-DD HH24:MI:SS') → formatDateTime(order_date, '%Y-%m-%d %H:%M:%S')
   *   TO_CHAR(order_date, 'MON YYYY')          → formatDateTime(order_date, '%b %Y')
   * </pre>
   *
   * <p>Falls back to the default ARP unparse for any other function call.
   */
  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if ("TO_CHAR".equalsIgnoreCase(call.getOperator().getName())
        && call.operandCount() == 2) {
      SqlNode fmtNode = call.operand(1);
      if (fmtNode instanceof SqlLiteral) {
        String oracleFmt = ((SqlLiteral) fmtNode).getValueAs(String.class);
        if (oracleFmt != null) {
          String chFmt = translateDateFormat(oracleFmt.toUpperCase());
          writer.print("formatDateTime(");
          call.operand(0).unparse(writer, 0, 0);
          writer.print(", '" + chFmt + "')");
          return;
        }
      }
    }
    super.unparseCall(writer, call, leftPrec, rightPrec);
  }

  /**
   * Translates an Oracle/Dremio-style date format string to ClickHouse's
   * C-style {@code formatDateTime} format codes.
   *
   * <p>Replacements are applied most-specific-first to avoid partial matches
   * (e.g. {@code YYYY} before {@code YY}, {@code MONTH} before {@code MON},
   * {@code HH24}/{@code HH12} before bare {@code HH}).
   *
   * <p>The input is uppercased before matching, so lowercase user input
   * ({@code 'yyyy-mm-dd'}) is handled identically to uppercase.
   */
  private static String translateDateFormat(String fmt) {
    // Year — most specific first
    fmt = fmt.replace("YYYY", "%Y");
    fmt = fmt.replace("YY",   "%y");
    // Month name — before bare MM to avoid partial overlap
    fmt = fmt.replace("MONTH", "%B");
    fmt = fmt.replace("MON",   "%b");
    fmt = fmt.replace("MM",    "%m");
    // Day name — most specific first: DDD before DD before D
    fmt = fmt.replace("DAY",   "%A");
    fmt = fmt.replace("DY",    "%a");
    fmt = fmt.replace("DDD",   "%j");  // day of year — must come before DD and D
    fmt = fmt.replace("DD",    "%d");
    fmt = fmt.replace("D",     "%w");  // day of week (1=Mon in Oracle, 0=Sun in ClickHouse — close enough)
    // Time — HH24 / HH12 before bare HH
    fmt = fmt.replace("HH24",  "%H");
    fmt = fmt.replace("HH12",  "%I");
    fmt = fmt.replace("HH",    "%H");  // bare HH defaults to 24-hour
    fmt = fmt.replace("MI",    "%M");
    fmt = fmt.replace("SS",    "%S");
    // Meridian
    fmt = fmt.replace("AM",    "%p");
    fmt = fmt.replace("PM",    "%p");
    // Timezone
    fmt = fmt.replace("TZ",    "%Z");
    return fmt;
  }
}
