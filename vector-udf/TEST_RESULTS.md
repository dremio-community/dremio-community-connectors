# Test Results — Dremio Vector Distance UDF

## Build Status

| Item | Result |
|------|--------|
| Maven compilation | ✅ PASS |
| JAR created | ✅ PASS (`dremio-vector-udf-1.0.0-SNAPSHOT.jar`) |
| Unit tests | ✅ **59/59 PASS** |
| Live smoke tests (Dremio) | ✅ **12/12 PASS** |

---

## Environment

| Component | Version |
|-----------|---------|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| Java | 11 (Eclipse Temurin) |
| Maven | 3.9 |
| Test date | 2026-04-09 |

---

## Unit Tests — 59/59 Passing

Unit tests run via `mvn test` against `VectorUtils` directly (no Dremio runtime needed).

| # | Category | Test | Result |
|---|----------|------|--------|
| 1 | Parsing | testParseBasic | ✅ PASS |
| 2 | Parsing | testParseNegatives | ✅ PASS |
| 3 | Parsing | testParseNoSpaces | ✅ PASS |
| 4 | Cosine Similarity | testCosineIdentical | ✅ PASS |
| 5 | Cosine Similarity | testCosineOrthogonal | ✅ PASS |
| 6 | Cosine Similarity | testCosineOpposite | ✅ PASS |
| 7 | Cosine Similarity | testCosineKnownValue | ✅ PASS |
| 8 | Cosine Similarity | testCosineZeroVector | ✅ PASS |
| 9 | Cosine Distance | testCosineDistanceIdentical | ✅ PASS |
| 10 | Cosine Distance | testCosineDistanceOpposite | ✅ PASS |
| 11 | L2 Distance | testL2Same | ✅ PASS |
| 12 | L2 Distance | testL2Known | ✅ PASS |
| 13 | L2 Squared | testL2SquaredKnown | ✅ PASS |
| 14 | Dot Product | testDotProductKnown | ✅ PASS |
| 15 | Dot Product | testDotProductOrthogonal | ✅ PASS |
| 16 | L1 Distance | testL1Known | ✅ PASS |
| 17 | Norm | testNormKnown | ✅ PASS |
| 18 | Norm | testNormUnitVector | ✅ PASS |
| 19 | Dims | testDims | ✅ PASS |
| 20 | Dispatch | testDispatchCosine | ✅ PASS |
| 21 | Dispatch | testDispatchL2 | ✅ PASS |
| 22 | Dispatch | testDispatchDot | ✅ PASS |
| 23 | Dispatch | testDispatchUnknownMetric | ✅ PASS |
| 24 | Error handling | testDimensionMismatch | ✅ PASS |
| 25 | Chebyshev | testChebyshevKnown | ✅ PASS |
| 26 | Chebyshev | testChebyshevSame | ✅ PASS |
| 27 | Chebyshev | testChebyshevSingleDim | ✅ PASS |
| 28 | Minkowski | testMinkowskiP1EqualsL1 | ✅ PASS |
| 29 | Minkowski | testMinkowskiP2EqualsL2 | ✅ PASS |
| 30 | Minkowski | testMinkowskiKnownP3 | ✅ PASS |
| 31 | Minkowski | testMinkowskiInvalidP | ✅ PASS |
| 32 | Multiply | testMultiplyKnown | ✅ PASS |
| 33 | Multiply | testMultiplyWithZero | ✅ PASS |
| 34 | Concat | testConcatBasic | ✅ PASS |
| 35 | Concat | testConcatEmptyLeft | ✅ PASS |
| 36 | Concat | testConcatDims | ✅ PASS |
| 37 | LERP | testLerpT0ReturnsA | ✅ PASS |
| 38 | LERP | testLerpT1ReturnsB | ✅ PASS |
| 39 | LERP | testLerpMidpoint | ✅ PASS |
| 40 | LERP | testLerpT03 | ✅ PASS |
| 41 | Sum | testSumKnown | ✅ PASS |
| 42 | Sum | testSumNegatives | ✅ PASS |
| 43 | Sum | testSumEmpty | ✅ PASS |
| 44 | Max | testMaxKnown | ✅ PASS |
| 45 | Max | testMaxSingleElement | ✅ PASS |
| 46 | Max | testMaxEmpty | ✅ PASS |
| 47 | Min | testMinKnown | ✅ PASS |
| 48 | Min | testMinSingleElement | ✅ PASS |
| 49 | Min | testMinEmpty | ✅ PASS |
| 50 | Softmax | testSoftmaxSumsToOne | ✅ PASS |
| 51 | Softmax | testSoftmaxAllSame | ✅ PASS |
| 52 | Softmax | testSoftmaxKnownValues | ✅ PASS |
| 53 | Softmax | testSoftmaxNumericalStability | ✅ PASS |
| 54 | Softmax | testSoftmaxEmpty | ✅ PASS |
| 55 | Clip | testClipBasic | ✅ PASS |
| 56 | Clip | testClipAllWithinRange | ✅ PASS |
| 57 | Clip | testClipNegativeRange | ✅ PASS |
| 58 | Clip | testClipInvalidRange | ✅ PASS |
| 59 | Dispatch | testDispatchChebyshev | ✅ PASS |

---

## Live Smoke Tests — 12/12 Passing

Tested against Dremio 26.0.5 running in Docker. Each query verified for correct output.

| # | Function | Query | Expected | Result |
|---|----------|-------|----------|--------|
| 1 | `CHEBYSHEV_DISTANCE` | `CHEBYSHEV_DISTANCE('[1.0,5.0,2.0]', '[4.0,2.0,3.0]')` | `3.0` | ✅ PASS |
| 2 | `MINKOWSKI_DISTANCE` p=2 | `MINKOWSKI_DISTANCE('[0.0,0.0]', '[3.0,4.0]', 2.0)` | `5.0` | ✅ PASS |
| 3 | `MINKOWSKI_DISTANCE` p=1 | `MINKOWSKI_DISTANCE('[1.0,2.0,3.0]', '[4.0,6.0,8.0]', 1.0)` | `12.0` | ✅ PASS |
| 4 | `VECTOR_MULTIPLY` | `VECTOR_MULTIPLY('[1.0,2.0,3.0]', '[2.0,3.0,4.0]')` | `[2.0,6.0,12.0]` | ✅ PASS |
| 5 | `VECTOR_CONCAT` | `VECTOR_CONCAT('[1.0,2.0]', '[3.0,4.0,5.0]')` | `[1.0,2.0,3.0,4.0,5.0]` | ✅ PASS |
| 6 | `VECTOR_LERP` t=0.5 | `VECTOR_LERP('[0.0,0.0]', '[10.0,10.0]', 0.5)` | `[5.0,5.0]` | ✅ PASS |
| 7 | `VECTOR_SUM` | `VECTOR_SUM('[1.0,2.0,3.0,4.0]')` | `10.0` | ✅ PASS |
| 8 | `VECTOR_MAX_ELEMENT` | `VECTOR_MAX_ELEMENT('[1.0,-5.0,3.0,2.0]')` | `3.0` | ✅ PASS |
| 9 | `VECTOR_MIN_ELEMENT` | `VECTOR_MIN_ELEMENT('[1.0,-5.0,3.0,2.0]')` | `-5.0` | ✅ PASS |
| 10 | `VECTOR_SOFTMAX` | `VECTOR_SUM(VECTOR_SOFTMAX('[1.0,2.0,3.0]'))` | `1.0` | ✅ PASS |
| 11 | `VECTOR_CLIP` | `VECTOR_CLIP('[-2.0,0.5,3.0,1.5]', 0.0, 1.0)` | `[0.0,0.5,1.0,1.0]` | ✅ PASS |
| 12 | `VECTOR_DISTANCE` chebyshev | `VECTOR_DISTANCE('[1.0,5.0]', '[4.0,2.0]', 'chebyshev')` | `3.0` | ✅ PASS |

---

## Complete Function Inventory (26 total)

### Distance / Similarity (9)
`COSINE_SIMILARITY` · `COSINE_DISTANCE` · `L2_DISTANCE` · `L2_DISTANCE_SQUARED` · `DOT_PRODUCT` · `L1_DISTANCE` · `CHEBYSHEV_DISTANCE` · `MINKOWSKI_DISTANCE` · `VECTOR_DISTANCE`

### Arithmetic — vector → vector (6)
`VECTOR_ADD` · `VECTOR_SUBTRACT` · `VECTOR_MULTIPLY` · `VECTOR_SCALE` · `VECTOR_CONCAT` · `VECTOR_LERP`

### Scalar Reductions — vector → scalar (5)
`VECTOR_NORM` · `VECTOR_DIMS` · `VECTOR_SUM` · `VECTOR_MAX_ELEMENT` · `VECTOR_MIN_ELEMENT`

### Transformations — vector → vector (4)
`VECTOR_NORMALIZE` · `VECTOR_SOFTMAX` · `VECTOR_CLIP` · `VECTOR_SLICE`

### Utility (2)
`VECTOR_ELEMENT_AT` · `VECTOR_IS_VALID`
