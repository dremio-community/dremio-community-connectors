package com.dremio.plugins.cassandra.scan;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link CassandraPredicate} CQL fragment generation and Op flipping.
 *
 * All tests are pure (no mocks needed) — CassandraPredicate is a plain value object.
 */
class CassandraPredicateTest {

  // ---------------------------------------------------------------------------
  // Single-value operators — numeric literals (no quoting)
  // ---------------------------------------------------------------------------

  @Test
  void eq_numericLiteral_noQuotes() {
    CassandraPredicate p = new CassandraPredicate("age", CassandraPredicate.Op.EQ, "30", false);
    assertEquals("\"age\" = 30", p.toCqlFragment());
  }

  @Test
  void gt_numericLiteral() {
    CassandraPredicate p = new CassandraPredicate("score", CassandraPredicate.Op.GT, "100", false);
    assertEquals("\"score\" > 100", p.toCqlFragment());
  }

  @Test
  void gte_numericLiteral() {
    CassandraPredicate p = new CassandraPredicate("score", CassandraPredicate.Op.GTE, "100", false);
    assertEquals("\"score\" >= 100", p.toCqlFragment());
  }

  @Test
  void lt_numericLiteral() {
    CassandraPredicate p = new CassandraPredicate("score", CassandraPredicate.Op.LT, "200", false);
    assertEquals("\"score\" < 200", p.toCqlFragment());
  }

  @Test
  void lte_numericLiteral() {
    CassandraPredicate p = new CassandraPredicate("score", CassandraPredicate.Op.LTE, "200", false);
    assertEquals("\"score\" <= 200", p.toCqlFragment());
  }

  // ---------------------------------------------------------------------------
  // Single-value operators — string literals (single-quoted)
  // ---------------------------------------------------------------------------

  @Test
  void eq_stringLiteral_addsSingleQuotes() {
    CassandraPredicate p = new CassandraPredicate("status", CassandraPredicate.Op.EQ, "active", true);
    assertEquals("\"status\" = 'active'", p.toCqlFragment());
  }

  @Test
  void eq_stringLiteral_escapesSingleQuote() {
    // SQL injection / apostrophe safety: O'Brien → O''Brien
    CassandraPredicate p = new CassandraPredicate("name", CassandraPredicate.Op.EQ, "O'Brien", true);
    assertEquals("\"name\" = 'O''Brien'", p.toCqlFragment());
  }

  @Test
  void gt_stringLiteral_addsSingleQuotes() {
    CassandraPredicate p = new CassandraPredicate("created", CassandraPredicate.Op.GT, "2024-01-01", true);
    assertEquals("\"created\" > '2024-01-01'", p.toCqlFragment());
  }

  // ---------------------------------------------------------------------------
  // IN operator — multiple literals
  // ---------------------------------------------------------------------------

  @Test
  void in_stringLiterals_producesInList() {
    CassandraPredicate p = CassandraPredicate.in(
        "status", Arrays.asList("active", "pending"), true);
    assertEquals("\"status\" IN ('active', 'pending')", p.toCqlFragment());
  }

  @Test
  void in_numericLiterals_noQuotes() {
    CassandraPredicate p = CassandraPredicate.in(
        "user_id", Arrays.asList("1", "2", "3"), false);
    assertEquals("\"user_id\" IN (1, 2, 3)", p.toCqlFragment());
  }

  @Test
  void in_singleStringLiteral() {
    CassandraPredicate p = CassandraPredicate.in(
        "region", Collections.singletonList("us-east-1"), true);
    assertEquals("\"region\" IN ('us-east-1')", p.toCqlFragment());
  }

  @Test
  void in_stringWithSingleQuote_escapesEachLiteral() {
    CassandraPredicate p = CassandraPredicate.in(
        "name", Arrays.asList("O'Brien", "Smith"), true);
    assertEquals("\"name\" IN ('O''Brien', 'Smith')", p.toCqlFragment());
  }

  // ---------------------------------------------------------------------------
  // Column quoting — ensures column names are always double-quoted
  // ---------------------------------------------------------------------------

  @Test
  void columnNameIsDoubleQuoted() {
    CassandraPredicate p = new CassandraPredicate("user_id", CassandraPredicate.Op.EQ, "abc", true);
    // Column name must be double-quoted in CQL
    assertEquals("\"user_id\" = 'abc'", p.toCqlFragment());
  }

  @Test
  void toString_delegatesToToCqlFragment() {
    CassandraPredicate p = new CassandraPredicate("x", CassandraPredicate.Op.EQ, "1", false);
    assertEquals(p.toCqlFragment(), p.toString());
  }

  // ---------------------------------------------------------------------------
  // Op.flip() — used when literal is on the left side of the condition
  // ---------------------------------------------------------------------------

  @Test
  void op_flip_gtBecomesLt() {
    assertEquals(CassandraPredicate.Op.LT, CassandraPredicate.Op.GT.flip());
  }

  @Test
  void op_flip_gteBecomesLte() {
    assertEquals(CassandraPredicate.Op.LTE, CassandraPredicate.Op.GTE.flip());
  }

  @Test
  void op_flip_ltBecomesGt() {
    assertEquals(CassandraPredicate.Op.GT, CassandraPredicate.Op.LT.flip());
  }

  @Test
  void op_flip_lteBecomesGte() {
    assertEquals(CassandraPredicate.Op.GTE, CassandraPredicate.Op.LTE.flip());
  }

  @Test
  void op_flip_eqIsSymmetric() {
    assertEquals(CassandraPredicate.Op.EQ, CassandraPredicate.Op.EQ.flip());
  }

  @Test
  void op_flip_inIsSymmetric() {
    assertEquals(CassandraPredicate.Op.IN, CassandraPredicate.Op.IN.flip());
  }
}
