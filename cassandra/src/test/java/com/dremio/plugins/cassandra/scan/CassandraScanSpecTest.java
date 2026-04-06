package com.dremio.plugins.cassandra.scan;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CassandraScanSpec} CQL generation and immutable copy operations.
 *
 * Key correctness properties verified:
 *   1. Column projection (SELECT * vs column list)
 *   2. Token-range WHERE clause construction
 *   3. LIMIT appended after WHERE (token range)
 *   4. LIMIT appended before ALLOW FILTERING (predicate pushdown)
 *   5. {@code withLimit()} is non-mutating (immutable copy)
 *   6. {@code hasLimit()}, {@code hasPredicates()}, {@code hasTokenRange()} flags
 */
class CassandraScanSpecTest {

  private static final String KS  = "my_keyspace";
  private static final String TBL = "my_table";

  // ---------------------------------------------------------------------------
  // toCql() — full table scan variants
  // ---------------------------------------------------------------------------

  @Test
  void toCql_noColumnsNoTokenRange_selectStar() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    assertEquals(
        "SELECT * FROM \"my_keyspace\".\"my_table\"",
        spec.toCql());
  }

  @Test
  void toCql_withColumnList_selectsNamedColumns() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    List<String> cols = Arrays.asList("user_id", "email");
    assertEquals(
        "SELECT \"user_id\", \"email\" FROM \"my_keyspace\".\"my_table\"",
        spec.toCql(cols));
  }

  @Test
  void toCql_emptyColumnList_treatedAsSelectStar() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    assertEquals(
        "SELECT * FROM \"my_keyspace\".\"my_table\"",
        spec.toCql(Collections.emptyList()));
  }

  @Test
  void toCql_withLimit_appendsLimitClause() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL).withLimit(100);
    assertEquals(
        "SELECT * FROM \"my_keyspace\".\"my_table\" LIMIT 100",
        spec.toCql());
  }

  // ---------------------------------------------------------------------------
  // toCql() — token-range variants
  // ---------------------------------------------------------------------------

  @Test
  void toCql_withSinglePartitionKey_tokenRangeWhereClause() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Collections.singletonList("user_id"),
        Long.MIN_VALUE, 0L);
    String cql = spec.toCql();
    assertTrue(cql.contains("WHERE token(\"user_id\") >= " + Long.MIN_VALUE), cql);
    assertTrue(cql.contains("AND token(\"user_id\") <= 0"),                   cql);
  }

  @Test
  void toCql_withCompositePartitionKey_tokenExprIncludesAllKeys() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Arrays.asList("pk1", "pk2"),
        -100L, 100L);
    String cql = spec.toCql();
    assertTrue(cql.contains("token(\"pk1\", \"pk2\")"), cql);
  }

  @Test
  void toCql_withTokenRangeAndLimit_limitAfterWhereClause() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Collections.singletonList("id"),
        -100L, 100L).withLimit(50);
    String cql = spec.toCql();
    // WHERE ... must come before LIMIT
    int wherePos = cql.indexOf("WHERE");
    int limitPos = cql.indexOf("LIMIT");
    assertTrue(wherePos > 0 && limitPos > wherePos,
        "Expected WHERE before LIMIT in: " + cql);
    assertTrue(cql.endsWith("LIMIT 50"), cql);
  }

  @Test
  void toCql_withTokenRangeAndColumns_combinedCorrectly() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Collections.singletonList("id"),
        0L, 1000L);
    String cql = spec.toCql(Collections.singletonList("name"));
    assertTrue(cql.startsWith("SELECT \"name\""), cql);
    assertTrue(cql.contains("WHERE token(\"id\")"), cql);
  }

  // ---------------------------------------------------------------------------
  // toCqlWithPushdown() — predicate pushdown variants
  // ---------------------------------------------------------------------------

  @Test
  void toCqlWithPushdown_singleEqPredicate_buildsWhereClause() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    CassandraPredicate pk = new CassandraPredicate("user_id", CassandraPredicate.Op.EQ, "42", false);
    String cql = spec.toCqlWithPushdown(null,
        Collections.singletonList(pk), Collections.emptyList());
    assertEquals(
        "SELECT * FROM \"my_keyspace\".\"my_table\" WHERE \"user_id\" = 42",
        cql);
  }

  @Test
  void toCqlWithPushdown_pkAndCkPredicates_bothAppended() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    CassandraPredicate pk = new CassandraPredicate("user_id", CassandraPredicate.Op.EQ, "1", false);
    CassandraPredicate ck = new CassandraPredicate("event_time", CassandraPredicate.Op.GT, "1000", false);
    String cql = spec.toCqlWithPushdown(null,
        Collections.singletonList(pk), Collections.singletonList(ck));
    assertTrue(cql.contains("\"user_id\" = 1"), cql);
    assertTrue(cql.contains("AND \"event_time\" > 1000"), cql);
  }

  @Test
  void toCqlWithPushdown_withLimit_limitAppearsInCql() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL).withLimit(25);
    CassandraPredicate pk = new CassandraPredicate("id", CassandraPredicate.Op.EQ, "7", false);
    String cql = spec.toCqlWithPushdown(null,
        Collections.singletonList(pk), Collections.emptyList());
    assertTrue(cql.contains("LIMIT 25"), cql);
  }

  @Test
  void toCqlWithPushdown_withAllowFiltering_appendedAtEnd() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    CassandraPredicate p = new CassandraPredicate("col", CassandraPredicate.Op.EQ, "val", true);
    String cql = spec.toCqlWithPushdown(null,
        Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(p), true);
    assertTrue(cql.endsWith("ALLOW FILTERING"), cql);
  }

  @Test
  void toCqlWithPushdown_limitBeforeAllowFiltering_correctCqlGrammarOrder() {
    // CQL grammar: SELECT … FROM … WHERE … LIMIT N ALLOW FILTERING
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL).withLimit(10);
    CassandraPredicate p = new CassandraPredicate("col", CassandraPredicate.Op.EQ, "x", true);
    String cql = spec.toCqlWithPushdown(null,
        Collections.emptyList(), Collections.emptyList(),
        Collections.singletonList(p), true);

    int limitPos = cql.indexOf("LIMIT");
    int allowPos = cql.indexOf("ALLOW FILTERING");
    assertTrue(limitPos > 0 && allowPos > limitPos,
        "Expected LIMIT before ALLOW FILTERING in: " + cql);
  }

  @Test
  void toCqlWithPushdown_withColumnList_selectsNamedColumns() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    CassandraPredicate pk = new CassandraPredicate("id", CassandraPredicate.Op.EQ, "1", false);
    List<String> cols = Collections.singletonList("name");
    String cql = spec.toCqlWithPushdown(cols,
        Collections.singletonList(pk), Collections.emptyList());
    assertTrue(cql.startsWith("SELECT \"name\""), cql);
  }

  @Test
  void toCqlWithPushdown_noPredicates_noWhereClause() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    String cql = spec.toCqlWithPushdown(null,
        Collections.emptyList(), Collections.emptyList());
    assertFalse(cql.contains("WHERE"), cql);
  }

  // ---------------------------------------------------------------------------
  // withLimit() — immutable copy semantics
  // ---------------------------------------------------------------------------

  @Test
  void withLimit_returnsNewSpecWithLimitSet() {
    CassandraScanSpec original = new CassandraScanSpec(KS, TBL);
    CassandraScanSpec limited  = original.withLimit(200);
    assertEquals(200L, limited.getLimit());
  }

  @Test
  void withLimit_doesNotMutateOriginal() {
    CassandraScanSpec original = new CassandraScanSpec(KS, TBL);
    original.withLimit(200);   // result deliberately discarded
    assertFalse(original.hasLimit(), "withLimit() must not mutate the original spec");
  }

  @Test
  void withLimit_preservesOtherFields() {
    CassandraScanSpec original = new CassandraScanSpec(
        KS, TBL,
        Collections.singletonList("id"),
        -500L, 500L);
    CassandraScanSpec limited = original.withLimit(99);
    assertEquals(KS,    limited.getKeyspace());
    assertEquals(TBL,   limited.getTableName());
    assertEquals(-500L, limited.getTokenRangeStart());
    assertEquals(500L,  limited.getTokenRangeEnd());
    assertEquals(99L,   limited.getLimit());
  }

  // ---------------------------------------------------------------------------
  // Boolean flags — hasLimit, hasPredicates, hasTokenRange
  // ---------------------------------------------------------------------------

  @Test
  void hasLimit_falseForNewSpec() {
    assertFalse(new CassandraScanSpec(KS, TBL).hasLimit());
  }

  @Test
  void hasLimit_trueAfterWithLimit() {
    assertTrue(new CassandraScanSpec(KS, TBL).withLimit(1).hasLimit());
  }

  @Test
  void hasPredicates_falseForFullTableScan() {
    assertFalse(new CassandraScanSpec(KS, TBL).hasPredicates());
  }

  @Test
  void hasPredicates_trueWhenPredicatesPresent() {
    CassandraPredicate p = new CassandraPredicate("id", CassandraPredicate.Op.EQ, "1", false);
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL, null, null, null, Collections.singletonList(p));
    assertTrue(spec.hasPredicates());
  }

  @Test
  void hasTokenRange_falseForFullTableScan() {
    assertFalse(new CassandraScanSpec(KS, TBL).hasTokenRange());
  }

  @Test
  void hasTokenRange_trueWhenAllTokenFieldsPresent() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Collections.singletonList("id"),
        -100L, 100L);
    assertTrue(spec.hasTokenRange());
  }

  @Test
  void hasTokenRange_falseWhenPartitionKeysEmpty() {
    CassandraScanSpec spec = new CassandraScanSpec(
        KS, TBL,
        Collections.emptyList(),   // empty keys → no valid token range
        -100L, 100L);
    assertFalse(spec.hasTokenRange());
  }

  // ---------------------------------------------------------------------------
  // toTablePath() — used by SubScan for metadata tracking
  // ---------------------------------------------------------------------------

  @Test
  void toTablePath_returnsTwoElementList() {
    CassandraScanSpec spec = new CassandraScanSpec(KS, TBL);
    List<String> path = spec.toTablePath();
    assertEquals(Arrays.asList(KS, TBL), path);
  }
}
