package com.dremio.udf.vector;

/**
 * Static helper methods for vector math operations.
 *
 * All methods are static so they can be called from Dremio's code-generated
 * UDF eval() context using fully-qualified class names without instantiation.
 *
 * Vector input format: JSON array string, e.g. "[0.1, 0.2, -0.3, 0.9]"
 */
public final class VectorUtils {

  private VectorUtils() {}

  // ---------------------------------------------------------------------------
  // Parsing
  // ---------------------------------------------------------------------------

  /**
   * Parse a JSON-array string into a double[].
   * Accepts:  "[0.1, 0.2, 0.3]"  or  "[0.1,0.2,0.3]"
   * Throws IllegalArgumentException on malformed input.
   */
  public static double[] parseVector(String json) {
    if (json == null) {
      throw new IllegalArgumentException("Vector input is null");
    }
    String s = json.trim();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Vector input is empty");
    }
    if (s.charAt(0) == '[') {
      s = s.substring(1);
    }
    if (s.charAt(s.length() - 1) == ']') {
      s = s.substring(0, s.length() - 1);
    }
    s = s.trim();
    if (s.isEmpty()) {
      return new double[0];
    }
    String[] parts = s.split(",");
    double[] vec = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vec[i] = Double.parseDouble(parts[i].trim());
    }
    return vec;
  }

  // ---------------------------------------------------------------------------
  // Distance / similarity metrics
  // ---------------------------------------------------------------------------

  /**
   * Cosine similarity: dot(a, b) / (|a| * |b|)
   * Range: [-1, 1].  1 = identical direction, 0 = orthogonal, -1 = opposite.
   * Returns 0.0 if either vector has zero magnitude.
   */
  public static double cosineSimilarity(double[] a, double[] b) {
    checkSameDimensions(a, b);
    double dot = 0, normA = 0, normB = 0;
    for (int i = 0; i < a.length; i++) {
      dot   += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    double denom = Math.sqrt(normA) * Math.sqrt(normB);
    return denom == 0.0 ? 0.0 : dot / denom;
  }

  /**
   * Cosine distance: 1 - cosine_similarity(a, b)
   * Range: [0, 2].  0 = identical, 1 = orthogonal, 2 = opposite.
   */
  public static double cosineDistance(double[] a, double[] b) {
    return 1.0 - cosineSimilarity(a, b);
  }

  /**
   * L2 (Euclidean) distance: sqrt( sum( (a_i - b_i)^2 ) )
   * Range: [0, ∞).  0 = identical vectors.
   */
  public static double l2Distance(double[] a, double[] b) {
    checkSameDimensions(a, b);
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  /**
   * Squared L2 distance (avoids sqrt — faster when only ranking is needed).
   */
  public static double l2DistanceSquared(double[] a, double[] b) {
    checkSameDimensions(a, b);
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return sum;
  }

  /**
   * Dot product (inner product): sum( a_i * b_i )
   * For unit-normalized vectors this equals cosine similarity.
   */
  public static double dotProduct(double[] a, double[] b) {
    checkSameDimensions(a, b);
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      sum += a[i] * b[i];
    }
    return sum;
  }

  /**
   * L1 (Manhattan) distance: sum( |a_i - b_i| )
   */
  public static double l1Distance(double[] a, double[] b) {
    checkSameDimensions(a, b);
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      sum += Math.abs(a[i] - b[i]);
    }
    return sum;
  }

  /**
   * L2 norm (magnitude) of a vector: sqrt( sum( a_i^2 ) )
   */
  public static double norm(double[] a) {
    double sum = 0;
    for (double v : a) {
      sum += v * v;
    }
    return Math.sqrt(sum);
  }

  /**
   * Number of dimensions (elements) in a vector string.
   */
  public static int dims(String json) {
    return parseVector(json).length;
  }

  /**
   * Return a unit-normalized copy of the vector (each element divided by its L2 norm).
   * If the vector has zero magnitude, returns a zero vector of the same dimension.
   */
  public static double[] normalize(double[] a) {
    double n = norm(a);
    double[] result = new double[a.length];
    if (n == 0.0) {
      return result; // zero vector — return as-is
    }
    for (int i = 0; i < a.length; i++) {
      result[i] = a[i] / n;
    }
    return result;
  }

  /**
   * Serialize a double[] back to a JSON array string: "[0.12, -0.45, 0.88]"
   */
  public static String toJson(double[] a) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < a.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(a[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Returns true if the string is a valid, parseable, non-empty vector.
   * Safe to call on arbitrary VARCHAR values — never throws.
   */
  public static boolean isValid(String json) {
    if (json == null || json.trim().isEmpty()) return false;
    try {
      double[] v = parseVector(json);
      return v.length > 0;
    } catch (Exception e) {
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Generic dispatcher used by VECTOR_DISTANCE(v1, v2, metric)
  // ---------------------------------------------------------------------------

  /**
   * Dispatch to the requested metric.
   * metric (case-insensitive): cosine, cosine_similarity, cosine_distance,
   *                            l2, euclidean, l2_squared,
   *                            dot, dot_product, inner_product,
   *                            l1, manhattan
   */
  public static double dispatch(double[] a, double[] b, String metric) {
    String m = metric.toLowerCase(java.util.Locale.ROOT).trim();
    switch (m) {
      case "cosine":
      case "cosine_similarity":
        return cosineSimilarity(a, b);
      case "cosine_distance":
        return cosineDistance(a, b);
      case "l2":
      case "euclidean":
        return l2Distance(a, b);
      case "l2_squared":
        return l2DistanceSquared(a, b);
      case "dot":
      case "dot_product":
      case "inner_product":
        return dotProduct(a, b);
      case "l1":
      case "manhattan":
        return l1Distance(a, b);
      default:
        throw new IllegalArgumentException(
            "Unknown vector metric: '" + metric + "'. " +
            "Supported: cosine, cosine_distance, l2, l2_squared, dot, l1");
    }
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  private static void checkSameDimensions(double[] a, double[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: " + a.length + " vs " + b.length);
    }
  }
}
