package com.dremio.udf.vector;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for VectorUtils — runs without a Dremio instance.
 */
public class VectorDistanceFunctionsTest {

  private static final double EPS = 1e-9;

  // ── parseVector ────────────────────────────────────────────────────────────

  @Test
  public void testParseBasic() {
    double[] v = VectorUtils.parseVector("[1.0, 2.0, 3.0]");
    assertArrayEquals(new double[]{1.0, 2.0, 3.0}, v, EPS);
  }

  @Test
  public void testParseNegatives() {
    double[] v = VectorUtils.parseVector("[-0.5, 0.0, 1.5]");
    assertArrayEquals(new double[]{-0.5, 0.0, 1.5}, v, EPS);
  }

  @Test
  public void testParseNoSpaces() {
    double[] v = VectorUtils.parseVector("[1.0,2.0,3.0]");
    assertArrayEquals(new double[]{1.0, 2.0, 3.0}, v, EPS);
  }

  // ── cosineSimilarity ───────────────────────────────────────────────────────

  @Test
  public void testCosineIdentical() {
    double[] v = {1.0, 2.0, 3.0};
    assertEquals(1.0, VectorUtils.cosineSimilarity(v, v), EPS);
  }

  @Test
  public void testCosineOrthogonal() {
    double[] a = {1.0, 0.0};
    double[] b = {0.0, 1.0};
    assertEquals(0.0, VectorUtils.cosineSimilarity(a, b), EPS);
  }

  @Test
  public void testCosineOpposite() {
    double[] a = {1.0, 0.0};
    double[] b = {-1.0, 0.0};
    assertEquals(-1.0, VectorUtils.cosineSimilarity(a, b), EPS);
  }

  @Test
  public void testCosineKnownValue() {
    double[] a = {1.0, 1.0};
    double[] b = {1.0, 0.0};
    // cos(45°) = 1/sqrt(2)
    assertEquals(1.0 / Math.sqrt(2.0), VectorUtils.cosineSimilarity(a, b), 1e-6);
  }

  @Test
  public void testCosineZeroVector() {
    double[] a = {0.0, 0.0};
    double[] b = {1.0, 2.0};
    assertEquals(0.0, VectorUtils.cosineSimilarity(a, b), EPS);
  }

  // ── cosineDistance ─────────────────────────────────────────────────────────

  @Test
  public void testCosineDistanceIdentical() {
    double[] v = {1.0, 2.0, 3.0};
    assertEquals(0.0, VectorUtils.cosineDistance(v, v), EPS);
  }

  @Test
  public void testCosineDistanceOpposite() {
    double[] a = {1.0, 0.0};
    double[] b = {-1.0, 0.0};
    assertEquals(2.0, VectorUtils.cosineDistance(a, b), EPS);
  }

  // ── l2Distance ─────────────────────────────────────────────────────────────

  @Test
  public void testL2Same() {
    double[] v = {1.0, 2.0, 3.0};
    assertEquals(0.0, VectorUtils.l2Distance(v, v), EPS);
  }

  @Test
  public void testL2Known() {
    double[] a = {0.0, 0.0};
    double[] b = {3.0, 4.0};
    assertEquals(5.0, VectorUtils.l2Distance(a, b), EPS);
  }

  // ── l2DistanceSquared ──────────────────────────────────────────────────────

  @Test
  public void testL2SquaredKnown() {
    double[] a = {0.0, 0.0};
    double[] b = {3.0, 4.0};
    assertEquals(25.0, VectorUtils.l2DistanceSquared(a, b), EPS);
  }

  // ── dotProduct ─────────────────────────────────────────────────────────────

  @Test
  public void testDotProductKnown() {
    double[] a = {1.0, 2.0, 3.0};
    double[] b = {4.0, 5.0, 6.0};
    assertEquals(32.0, VectorUtils.dotProduct(a, b), EPS);
  }

  @Test
  public void testDotProductOrthogonal() {
    double[] a = {1.0, 0.0};
    double[] b = {0.0, 1.0};
    assertEquals(0.0, VectorUtils.dotProduct(a, b), EPS);
  }

  // ── l1Distance ─────────────────────────────────────────────────────────────

  @Test
  public void testL1Known() {
    double[] a = {1.0, 2.0, 3.0};
    double[] b = {4.0, 6.0, 8.0};
    assertEquals(12.0, VectorUtils.l1Distance(a, b), EPS);
  }

  // ── norm ───────────────────────────────────────────────────────────────────

  @Test
  public void testNormKnown() {
    double[] v = {3.0, 4.0};
    assertEquals(5.0, VectorUtils.norm(v), EPS);
  }

  @Test
  public void testNormUnitVector() {
    double[] v = {1.0 / Math.sqrt(2), 1.0 / Math.sqrt(2)};
    assertEquals(1.0, VectorUtils.norm(v), 1e-9);
  }

  // ── dims ───────────────────────────────────────────────────────────────────

  @Test
  public void testDims() {
    assertEquals(3, VectorUtils.dims("[0.1, 0.2, 0.3]"));
    assertEquals(4, VectorUtils.dims("[0.1,0.2,0.3,0.4]"));
    assertEquals(1536, VectorUtils.dims(buildVec(1536)));
  }

  // ── dispatch ───────────────────────────────────────────────────────────────

  @Test
  public void testDispatchCosine() {
    double[] a = {1.0, 0.0};
    double[] b = {1.0, 0.0};
    assertEquals(1.0, VectorUtils.dispatch(a, b, "cosine"), EPS);
  }

  @Test
  public void testDispatchL2() {
    double[] a = {0.0, 0.0};
    double[] b = {3.0, 4.0};
    assertEquals(5.0, VectorUtils.dispatch(a, b, "l2"), EPS);
  }

  @Test
  public void testDispatchDot() {
    double[] a = {1.0, 2.0};
    double[] b = {3.0, 4.0};
    assertEquals(11.0, VectorUtils.dispatch(a, b, "dot"), EPS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDispatchUnknownMetric() {
    double[] a = {1.0};
    double[] b = {1.0};
    VectorUtils.dispatch(a, b, "jaccard");
  }

  // ── dimension mismatch ─────────────────────────────────────────────────────

  @Test(expected = IllegalArgumentException.class)
  public void testDimensionMismatch() {
    VectorUtils.l2Distance(new double[]{1.0, 2.0}, new double[]{1.0, 2.0, 3.0});
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static String buildVec(int dims) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < dims; i++) {
      if (i > 0) sb.append(",");
      sb.append("0.001");
    }
    sb.append("]");
    return sb.toString();
  }
}
