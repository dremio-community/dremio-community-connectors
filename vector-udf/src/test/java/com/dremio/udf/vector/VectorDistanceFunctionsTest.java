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

  // ── chebyshevDistance ──────────────────────────────────────────────────────

  @Test
  public void testChebyshevKnown() {
    double[] a = {1.0, 5.0, 2.0};
    double[] b = {4.0, 2.0, 3.0};
    // |1-4|=3, |5-2|=3, |2-3|=1 → max = 3
    assertEquals(3.0, VectorUtils.chebyshevDistance(a, b), EPS);
  }

  @Test
  public void testChebyshevSame() {
    double[] v = {1.0, 2.0, 3.0};
    assertEquals(0.0, VectorUtils.chebyshevDistance(v, v), EPS);
  }

  @Test
  public void testChebyshevSingleDim() {
    double[] a = {7.0};
    double[] b = {2.0};
    assertEquals(5.0, VectorUtils.chebyshevDistance(a, b), EPS);
  }

  // ── minkowskiDistance ──────────────────────────────────────────────────────

  @Test
  public void testMinkowskiP1EqualsL1() {
    double[] a = {1.0, 2.0, 3.0};
    double[] b = {4.0, 6.0, 8.0};
    assertEquals(VectorUtils.l1Distance(a, b), VectorUtils.minkowskiDistance(a, b, 1.0), 1e-9);
  }

  @Test
  public void testMinkowskiP2EqualsL2() {
    double[] a = {0.0, 0.0};
    double[] b = {3.0, 4.0};
    assertEquals(5.0, VectorUtils.minkowskiDistance(a, b, 2.0), 1e-9);
  }

  @Test
  public void testMinkowskiKnownP3() {
    double[] a = {0.0};
    double[] b = {8.0};
    // |0-8|^3 = 512; 512^(1/3) = 8
    assertEquals(8.0, VectorUtils.minkowskiDistance(a, b, 3.0), 1e-9);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinkowskiInvalidP() {
    VectorUtils.minkowskiDistance(new double[]{1.0}, new double[]{2.0}, 0.0);
  }

  // ── multiply (Hadamard) ────────────────────────────────────────────────────

  @Test
  public void testMultiplyKnown() {
    double[] a = {1.0, 2.0, 3.0};
    double[] b = {2.0, 3.0, 4.0};
    assertArrayEquals(new double[]{2.0, 6.0, 12.0}, VectorUtils.multiply(a, b), EPS);
  }

  @Test
  public void testMultiplyWithZero() {
    double[] a = {5.0, 3.0};
    double[] b = {0.0, 2.0};
    assertArrayEquals(new double[]{0.0, 6.0}, VectorUtils.multiply(a, b), EPS);
  }

  // ── concat ─────────────────────────────────────────────────────────────────

  @Test
  public void testConcatBasic() {
    double[] a = {1.0, 2.0};
    double[] b = {3.0, 4.0, 5.0};
    assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0, 5.0}, VectorUtils.concat(a, b), EPS);
  }

  @Test
  public void testConcatEmptyLeft() {
    double[] a = {};
    double[] b = {1.0, 2.0};
    assertArrayEquals(new double[]{1.0, 2.0}, VectorUtils.concat(a, b), EPS);
  }

  @Test
  public void testConcatDims() {
    double[] a = new double[768];
    double[] b = new double[512];
    assertEquals(1280, VectorUtils.concat(a, b).length);
  }

  // ── lerp ───────────────────────────────────────────────────────────────────

  @Test
  public void testLerpT0ReturnsA() {
    double[] a = {1.0, 2.0, 3.0};
    double[] b = {7.0, 8.0, 9.0};
    assertArrayEquals(a, VectorUtils.lerp(a, b, 0.0), EPS);
  }

  @Test
  public void testLerpT1ReturnsB() {
    double[] a = {1.0, 2.0};
    double[] b = {5.0, 6.0};
    assertArrayEquals(b, VectorUtils.lerp(a, b, 1.0), EPS);
  }

  @Test
  public void testLerpMidpoint() {
    double[] a = {0.0, 0.0};
    double[] b = {10.0, 10.0};
    assertArrayEquals(new double[]{5.0, 5.0}, VectorUtils.lerp(a, b, 0.5), EPS);
  }

  @Test
  public void testLerpT03() {
    double[] a = {0.0, 0.0};
    double[] b = {10.0, 10.0};
    assertArrayEquals(new double[]{3.0, 3.0}, VectorUtils.lerp(a, b, 0.3), 1e-9);
  }

  // ── sum ────────────────────────────────────────────────────────────────────

  @Test
  public void testSumKnown() {
    assertEquals(10.0, VectorUtils.sum(new double[]{1.0, 2.0, 3.0, 4.0}), EPS);
  }

  @Test
  public void testSumNegatives() {
    assertEquals(0.0, VectorUtils.sum(new double[]{-1.0, 0.0, 1.0}), EPS);
  }

  @Test
  public void testSumEmpty() {
    assertEquals(0.0, VectorUtils.sum(new double[]{}), EPS);
  }

  // ── max / min ──────────────────────────────────────────────────────────────

  @Test
  public void testMaxKnown() {
    assertEquals(5.0, VectorUtils.max(new double[]{1.0, 5.0, 3.0, -2.0}), EPS);
  }

  @Test
  public void testMaxSingleElement() {
    assertEquals(7.0, VectorUtils.max(new double[]{7.0}), EPS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxEmpty() {
    VectorUtils.max(new double[]{});
  }

  @Test
  public void testMinKnown() {
    assertEquals(-2.0, VectorUtils.min(new double[]{1.0, 5.0, 3.0, -2.0}), EPS);
  }

  @Test
  public void testMinSingleElement() {
    assertEquals(-3.0, VectorUtils.min(new double[]{-3.0}), EPS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinEmpty() {
    VectorUtils.min(new double[]{});
  }

  // ── softmax ────────────────────────────────────────────────────────────────

  @Test
  public void testSoftmaxSumsToOne() {
    double[] result = VectorUtils.softmax(new double[]{1.0, 2.0, 3.0});
    double total = 0;
    for (double v : result) total += v;
    assertEquals(1.0, total, 1e-12);
  }

  @Test
  public void testSoftmaxAllSame() {
    // Equal inputs → equal probabilities
    double[] result = VectorUtils.softmax(new double[]{2.0, 2.0, 2.0, 2.0});
    for (double v : result) assertEquals(0.25, v, 1e-12);
  }

  @Test
  public void testSoftmaxKnownValues() {
    double[] result = VectorUtils.softmax(new double[]{1.0, 2.0, 3.0});
    // Known values: e^1/(e^1+e^2+e^3), etc.
    double e1 = Math.exp(1), e2 = Math.exp(2), e3 = Math.exp(3);
    double sum = e1 + e2 + e3;
    assertEquals(e1 / sum, result[0], 1e-12);
    assertEquals(e2 / sum, result[1], 1e-12);
    assertEquals(e3 / sum, result[2], 1e-12);
  }

  @Test
  public void testSoftmaxNumericalStability() {
    // Large values should not overflow with the max-shift trick
    double[] result = VectorUtils.softmax(new double[]{1000.0, 1001.0, 1002.0});
    double total = 0;
    for (double v : result) total += v;
    assertEquals(1.0, total, 1e-12);
    // Largest input should have largest probability
    assertTrue(result[2] > result[1]);
    assertTrue(result[1] > result[0]);
  }

  @Test
  public void testSoftmaxEmpty() {
    assertEquals(0, VectorUtils.softmax(new double[]{}).length);
  }

  // ── clip ───────────────────────────────────────────────────────────────────

  @Test
  public void testClipBasic() {
    double[] result = VectorUtils.clip(new double[]{-2.0, 0.5, 3.0, 1.5}, 0.0, 1.0);
    assertArrayEquals(new double[]{0.0, 0.5, 1.0, 1.0}, result, EPS);
  }

  @Test
  public void testClipAllWithinRange() {
    double[] a = {0.2, 0.5, 0.8};
    assertArrayEquals(a, VectorUtils.clip(a, 0.0, 1.0), EPS);
  }

  @Test
  public void testClipNegativeRange() {
    double[] result = VectorUtils.clip(new double[]{-5.0, 0.0, 5.0}, -2.0, 2.0);
    assertArrayEquals(new double[]{-2.0, 0.0, 2.0}, result, EPS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClipInvalidRange() {
    VectorUtils.clip(new double[]{1.0}, 1.0, 0.0);  // min > max
  }

  // ── dispatch (chebyshev) ───────────────────────────────────────────────────

  @Test
  public void testDispatchChebyshev() {
    double[] a = {1.0, 5.0};
    double[] b = {4.0, 2.0};
    assertEquals(3.0, VectorUtils.dispatch(a, b, "chebyshev"), EPS);
    assertEquals(3.0, VectorUtils.dispatch(a, b, "linf"), EPS);
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
