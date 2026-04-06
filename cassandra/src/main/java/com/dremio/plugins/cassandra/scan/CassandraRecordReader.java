package com.dremio.plugins.cassandra.scan;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.cassandra.CassandraStoragePlugin;
import com.dremio.plugins.cassandra.CassandraTypeConverter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Reads rows from a Cassandra table and writes them into Dremio Arrow vectors.
 *
 * Scalar types are written directly via setSafe() on the appropriate vector.
 * Collection types (LIST, SET, MAP) and UDTs are written as native Arrow nested
 * vectors (ListVector, MapVector, StructVector) using Arrow's direct write APIs.
 *
 * Predicate pushdown tiers (evaluated at setup time against live schema metadata):
 * <ol>
 *   <li>PK pushdown  — EQ/IN on all partition key columns → direct partition lookup</li>
 *   <li>CK pushdown  — any op on clustering columns, only when full PK is covered</li>
 *   <li>SAI pushdown — any op on SAI-indexed columns (no ALLOW FILTERING required)</li>
 *   <li>Regular index — EQ/IN on secondary-indexed columns + ALLOW FILTERING</li>
 * </ol>
 * The residual Dremio filter always guarantees correctness.
 *
 * Data flow:
 *   setup()  → validate predicates, execute CQL query, allocate Arrow vectors
 *   next()   → fetch up to TARGET_BATCH_SIZE rows, write into vectors
 *   close()  → nothing to close (session owned by plugin)
 */
public class CassandraRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(CassandraRecordReader.class);

  /** Maximum rows per Arrow batch. */
  private static final int TARGET_BATCH_SIZE = 4_000;

  private final CassandraStoragePlugin plugin;
  private final CassandraSubScan subScan;
  /** The specific token-range spec this reader will execute. */
  private final CassandraScanSpec specOverride;

  // Populated in setup()
  private List<ColumnInfo> columns;

  // Synchronous path (asyncPagePrefetch=false)
  private Iterator<Row> rowIterator;

  // Async prefetch path (asyncPagePrefetch=true, default)
  // While next() writes page N's rows into Arrow vectors, the driver is
  // simultaneously fetching page N+1 over the network so there is no
  // per-page stall on large full-table scans.
  private Iterator<Row>                   currentPageIter;
  private CompletionStage<AsyncResultSet> nextPageFuture;

  private boolean exhausted = false;
  private boolean useAsyncPrefetch = false;

  /** Associates each projected column with its Arrow vector and Cassandra type. */
  private static class ColumnInfo {
    final String    name;
    final DataType  cassandraType;
    final ValueVector vector;

    ColumnInfo(String name, DataType cassandraType, ValueVector vector) {
      this.name          = name;
      this.cassandraType = cassandraType;
      this.vector        = vector;
    }
  }

  public CassandraRecordReader(CassandraStoragePlugin plugin,
                                CassandraSubScan subScan,
                                OperatorContext context) {
    this(plugin, subScan, context, null);
  }

  public CassandraRecordReader(CassandraStoragePlugin plugin,
                                CassandraSubScan subScan,
                                OperatorContext context,
                                CassandraScanSpec specOverride) {
    super(context, subScan.getColumns());
    this.plugin       = plugin;
    this.subScan      = subScan;
    this.specOverride = specOverride;
  }

  // ---------------------------------------------------------------------------
  // RecordReader lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    columns = new ArrayList<>();
    CassandraScanSpec spec = (specOverride != null) ? specOverride : subScan.getScanSpec();

    com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata ksm =
        plugin.getConnection().getSession().getMetadata()
            .getKeyspace(spec.getKeyspace())
            .orElseThrow(() -> new ExecutionSetupException(
                "Keyspace not found: " + spec.getKeyspace()));

    com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata =
        ksm.getTable(spec.getTableName())
            .orElseThrow(() -> new ExecutionSetupException(
                "Table not found: " + spec));

    // --- Schema change detection ------------------------------------------
    // Compare the live column fingerprint against the last-seen fingerprint.
    // On a mismatch, invalidate the metadata cache and throw schemaChangeError
    // so Dremio re-plans the query with the updated schema.
    // On first access (no cached hash) we simply seed the cache — no re-plan needed.
    String tableKey  = spec.getKeyspace() + "." + spec.getTableName();
    String liveHash  = computeSchemaHash(tableMetadata);
    String cachedHash = plugin.getSchemaHash(tableKey);

    if (cachedHash != null && !cachedHash.equals(liveHash)) {
      logger.info("Schema change detected for {} — old: [{}], new: [{}]",
          tableKey, cachedHash, liveHash);
      // Update hash BEFORE throwing so the re-planned query sees the new schema.
      plugin.updateSchemaHash(tableKey, liveHash);
      plugin.invalidateMetadataCache();
      throw UserException.schemaChangeError()
          .message("Cassandra schema changed for %s — Dremio will re-plan with the updated schema",
              tableKey)
          .build(logger);
    }
    plugin.updateSchemaHash(tableKey, liveHash);

    List<SchemaPath> requestedCols = subScan.getColumns();
    boolean allColumns = requestedCols == null || requestedCols.equals(GroupScan.ALL_COLUMNS);

    for (java.util.Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier,
        com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata> entry
        : tableMetadata.getColumns().entrySet()) {

      String   colName = entry.getKey().asInternal();
      DataType colType = entry.getValue().getType();

      if (!allColumns && !isProjected(requestedCols, colName)) {
        continue;
      }

      Field field = CassandraTypeConverter.toArrowField(colName, colType);

      try {
        ValueVector vector = allocateVector(field, output);
        columns.add(new ColumnInfo(colName, colType, vector));
      } catch (SchemaChangeException e) {
        throw new ExecutionSetupException(
            "Failed to allocate vector for column: " + colName, e);
      }
    }

    // Build CQL — predicate pushdown validation
    // Priority order:
    //   1. PK pushdown  — EQ/IN on all partition key columns → direct partition lookup
    //   2. CK pushdown  — any op on clustering columns, only when full PK is covered
    //   3. SAI pushdown — any op on SAI-indexed columns (no ALLOW FILTERING needed)
    //   4. Regular index pushdown — EQ/IN on secondary-indexed columns + ALLOW FILTERING
    // Always falls back gracefully: residual Dremio filter guarantees correctness.
    List<String> colNames = columns.stream()
        .map(ci -> ci.name)
        .collect(Collectors.toList());

    String cql;
    List<CassandraPredicate> specPredicates = spec.getPredicates();
    if (!specPredicates.isEmpty()) {
      // Partition key columns
      Set<String> pkColSet = tableMetadata.getPartitionKey().stream()
          .map(col -> col.getName().asInternal())
          .collect(Collectors.toSet());

      // Clustering key columns
      Set<String> ckColSet = tableMetadata.getClusteringColumns().keySet().stream()
          .map(col -> col.getName().asInternal())
          .collect(Collectors.toSet());

      // Index columns — SAI (Storage-Attached Index) and regular secondary indexes
      Set<String> saiCols        = new HashSet<>();
      Set<String> regularIdxCols = new HashSet<>();
      for (IndexMetadata idx : tableMetadata.getIndexes().values()) {
        String target = extractSimpleIndexTarget(idx.getTarget());
        if (target == null) continue;
        if (isSaiIndex(idx)) {
          saiCols.add(target);
        } else {
          regularIdxCols.add(target);
        }
      }

      // --- 1. PK pushdown predicates (EQ/IN on partition key columns) ---
      List<CassandraPredicate> pkPushdownPredicates = specPredicates.stream()
          .filter(p -> (p.getOp() == CassandraPredicate.Op.EQ
                     || p.getOp() == CassandraPredicate.Op.IN)
                    && pkColSet.contains(p.getColumn()))
          .collect(Collectors.toList());
      Set<String> coveredPks = pkPushdownPredicates.stream()
          .map(CassandraPredicate::getColumn)
          .collect(Collectors.toSet());
      boolean fullPkCovered = !pkColSet.isEmpty() && coveredPks.containsAll(pkColSet);

      // --- 2. CK pushdown predicates (any op) — only when full PK is covered ---
      List<CassandraPredicate> ckPredicates = fullPkCovered
          ? specPredicates.stream()
                .filter(p -> ckColSet.contains(p.getColumn()))
                .collect(Collectors.toList())
          : Collections.emptyList();

      // --- 3. SAI predicates (any op) — columns not already handled as PK or CK ---
      List<CassandraPredicate> saiPredicates = specPredicates.stream()
          .filter(p -> saiCols.contains(p.getColumn())
                    && !pkColSet.contains(p.getColumn())
                    && !ckColSet.contains(p.getColumn()))
          .collect(Collectors.toList());

      // --- 4. Regular secondary index predicates (EQ/IN only to avoid costly scans) ---
      List<CassandraPredicate> regularIdxPredicates = specPredicates.stream()
          .filter(p -> (p.getOp() == CassandraPredicate.Op.EQ
                     || p.getOp() == CassandraPredicate.Op.IN)
                    && regularIdxCols.contains(p.getColumn())
                    && !pkColSet.contains(p.getColumn())
                    && !ckColSet.contains(p.getColumn()))
          .collect(Collectors.toList());

      if (fullPkCovered) {
        // Full PK + optional CK + optional index predicates — no ALLOW FILTERING
        List<CassandraPredicate> idxPredicates = new ArrayList<>(saiPredicates);
        idxPredicates.addAll(regularIdxPredicates);
        cql = spec.toCqlWithPushdown(colNames, pkPushdownPredicates, ckPredicates,
            idxPredicates, false);
        logger.debug("PK pushdown (pk={}, ck={}, idx={}): {}",
            pkPushdownPredicates.size(), ckPredicates.size(), idxPredicates.size(), cql);

      } else if (!saiPredicates.isEmpty() || !regularIdxPredicates.isEmpty()) {
        // No full PK, but indexed column predicates exist — push without PK/CK
        // SAI: no ALLOW FILTERING needed.
        // Regular secondary index: ALLOW FILTERING required (Cassandra enforces this).
        boolean needsAllowFiltering = !regularIdxPredicates.isEmpty();
        List<CassandraPredicate> idxPredicates = new ArrayList<>(saiPredicates);
        idxPredicates.addAll(regularIdxPredicates);
        cql = spec.toCqlWithPushdown(colNames, Collections.emptyList(), Collections.emptyList(),
            idxPredicates, needsAllowFiltering);
        logger.debug("Index pushdown (sai={}, regular={}, allowFiltering={}): {}",
            saiPredicates.size(), regularIdxPredicates.size(), needsAllowFiltering, cql);

      } else {
        // No PK coverage and no index predicates — fall back to full table scan
        cql = spec.toCql(colNames);
        logger.debug("No pushable predicates (no PK coverage, no indexes), falling back: {}", cql);
      }
    } else {
      cql = spec.toCql(colNames);
      logger.debug("Executing CQL: {}", cql);
    }

    // --- Adaptive fetch size -------------------------------------------------
    // Cap the CQL page size so that wide-column tables don't produce oversized
    // pages.  Target: at most 2 MB of raw CQL data per page.
    // Never exceeds the user-configured fetchSize; only reduces it.
    // Floor of 50 rows prevents tiny pages even for very large blob columns.
    final int TARGET_PAGE_BYTES = 2 * 1024 * 1024; // 2 MB
    long estRowBytes = CassandraStoragePlugin.estimateRowSizeBytes(tableMetadata);
    int adaptedFetchSize = (int) Math.max(50,
        Math.min(plugin.getConfig().fetchSize,
            TARGET_PAGE_BYTES / Math.max(1L, estRowBytes)));
    if (adaptedFetchSize < plugin.getConfig().fetchSize) {
      logger.debug("Adaptive fetch size for {}.{}: {} → {} rows (~{} B/row, 2 MB/page target)",
          spec.getKeyspace(), spec.getTableName(),
          plugin.getConfig().fetchSize, adaptedFetchSize, estRowBytes);
    }

    // --- Build statement + routing token -------------------------------------
    // setRoutingToken() hints the DataStax driver's token-aware load-balancing
    // policy to route this split directly to the Cassandra node that owns the
    // start of the token range, bypassing coordinator fan-out.
    // Only set for token-range splits — PK-pushdown queries have no token range.
    SimpleStatement simpleStmt = SimpleStatement.newInstance(cql)
        .setPageSize(adaptedFetchSize);

    if (spec.hasTokenRange()) {
      Optional<TokenMap> tokenMapOpt =
          plugin.getConnection().getSession().getMetadata().getTokenMap();
      if (tokenMapOpt.isPresent()) {
        // Murmur3 token = big-endian signed 64-bit long
        ByteBuffer tokenBytes = ByteBuffer.allocate(Long.BYTES);
        tokenBytes.putLong(spec.getTokenRangeStart());
        tokenBytes.flip();
        Token routingToken = tokenMapOpt.get().newToken(tokenBytes);
        simpleStmt = simpleStmt
            .setRoutingToken(routingToken)
            .setRoutingKeyspace(spec.getKeyspace());
        logger.debug("Routing token set for {}.{} split [{}, {}] → direct node routing",
            spec.getKeyspace(), spec.getTableName(),
            spec.getTokenRangeStart(), spec.getTokenRangeEnd());
      }
    }

    Statement<?> stmt = simpleStmt;

    this.useAsyncPrefetch = plugin.getConfig().asyncPagePrefetch;

    if (useAsyncPrefetch) {
      // Async prefetch: fire off the first CQL request now and immediately
      // kick off the second page fetch as soon as the first page arrives.
      // This overlaps network I/O with Arrow vector writes in next().
      try {
        AsyncResultSet firstPage = plugin.getConnection().getSession()
            .executeAsync(stmt).toCompletableFuture().get();
        currentPageIter = firstPage.currentPage().iterator();
        nextPageFuture  = firstPage.hasMorePages() ? firstPage.fetchNextPage() : null;
        logger.debug("Async prefetch enabled (fetchSize={}): first page buffered, "
            + "prefetching next={}", plugin.getConfig().fetchSize, nextPageFuture != null);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ExecutionSetupException("CQL execution interrupted: " + cql, e);
      } catch (ExecutionException e) {
        throw new ExecutionSetupException("CQL execution failed: " + cql, e.getCause());
      }
    } else {
      ResultSet rs = plugin.getConnection().getSession().execute(stmt);
      rowIterator = rs.iterator();
    }
  }

  @Override
  public int next() {
    if (exhausted) {
      return 0;
    }

    for (ColumnInfo ci : columns) {
      ci.vector.reset();
    }

    int rowCount = 0;

    if (useAsyncPrefetch) {
      // --- Async prefetch path ---
      // Drain rows from the current page first; when the page is exhausted,
      // wait for the pre-fetched next page (which has been arriving over the
      // network while we were writing rows) and immediately kick off the
      // fetch of the page after that.
      while (rowCount < TARGET_BATCH_SIZE) {
        // Advance to a non-empty page if needed
        while (currentPageIter == null || !currentPageIter.hasNext()) {
          if (nextPageFuture == null) {
            exhausted = true;
            break;
          }
          try {
            AsyncResultSet nextPage = nextPageFuture.toCompletableFuture().get();
            currentPageIter = nextPage.currentPage().iterator();
            // Immediately kick off the page after next while we process this one
            nextPageFuture  = nextPage.hasMorePages() ? nextPage.fetchNextPage() : null;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exhausted = true;
            break;
          } catch (ExecutionException e) {
            logger.warn("Error fetching next CQL page: {}", e.getCause().getMessage());
            exhausted = true;
            break;
          }
        }
        if (exhausted || currentPageIter == null || !currentPageIter.hasNext()) {
          break;
        }
        writeRow(currentPageIter.next(), rowCount);
        rowCount++;
      }
    } else {
      // --- Synchronous path (asyncPagePrefetch=false) ---
      if (rowIterator == null) {
        return 0;
      }
      while (rowIterator.hasNext() && rowCount < TARGET_BATCH_SIZE) {
        writeRow(rowIterator.next(), rowCount);
        rowCount++;
      }
      if (!rowIterator.hasNext()) {
        exhausted = true;
      }
    }

    for (ColumnInfo ci : columns) {
      ci.vector.setValueCount(rowCount);
    }

    return rowCount;
  }

  @Override
  public void close() throws Exception {
    rowIterator = null;
  }

  // ---------------------------------------------------------------------------
  // Row writing — dispatch to scalar or collection writer
  // ---------------------------------------------------------------------------
  // Index metadata helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns true if the given index is a Storage-Attached Index (SAI).
   *
   * SAI is a Cassandra 4.x feature: a CUSTOM index whose class name contains
   * "StorageAttachedIndex". SAI queries never require ALLOW FILTERING and
   * support EQ, IN, and range predicates on the indexed column.
   */
  private static boolean isSaiIndex(IndexMetadata idx) {
    if (idx.getKind() != IndexKind.CUSTOM) return false;
    String className = idx.getOptions().getOrDefault("class_name", "");
    return className.contains("StorageAttachedIndex");
  }

  /**
   * Extracts the bare column name from a Cassandra index target string.
   *
   * Simple column targets (e.g. {@code "email"}) are returned as-is (lowercased).
   * Collection function targets (e.g. {@code "values(col)"}, {@code "keys(col)"})
   * return null — we don't attempt to push predicates on collection sub-elements.
   */
  private static String extractSimpleIndexTarget(String target) {
    if (target == null || target.contains("(")) return null;
    return target.toLowerCase();
  }

  // ---------------------------------------------------------------------------

  private void writeRow(Row row, int index) {
    for (ColumnInfo ci : columns) {
      try {
        writeCell(ci.vector, ci.cassandraType, row, ci.name, index);
      } catch (Exception e) {
        logger.warn("Failed to read column '{}' at row {}: {}", ci.name, index, e.getMessage());
      }
    }
  }

  private void writeCell(ValueVector vector, DataType type, Row row, String col, int idx) {
    if (row.isNull(col)) {
      return; // leave validity bit unset → null
    }

    // ---- Collection and UDT types ------------------------------------------
    // IMPORTANT: MapVector extends ListVector in Arrow Java, so MapVector must be
    // checked FIRST — otherwise instanceof ListVector matches Map columns too.
    if (vector instanceof MapVector) {
      writeMapCell((MapVector) vector, type, row, col, idx);
      return;
    }
    if (vector instanceof ListVector) {
      writeListOrSetCell((ListVector) vector, type, row, col, idx);
      return;
    }
    if (vector instanceof StructVector) {
      writeStructCell((StructVector) vector, type, row, col, idx);
      return;
    }
    // FixedSizeListVector must be checked after MapVector/ListVector/StructVector
    // because it is an independent hierarchy in Arrow Java (does not extend ListVector).
    if (vector instanceof FixedSizeListVector) {
      writeVectorCell((FixedSizeListVector) vector, type, row, col, idx);
      return;
    }

    // ---- Scalar types -------------------------------------------------------
    if (vector instanceof VarCharVector) {
      String val = getAsString(row, col, type);
      if (val != null) {
        byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) vector).setSafe(idx, bytes, 0, bytes.length);
      }
    } else if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(idx, row.getInt(col));
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(idx, row.getLong(col));
    } else if (vector instanceof SmallIntVector) {
      ((SmallIntVector) vector).setSafe(idx, row.getShort(col));
    } else if (vector instanceof TinyIntVector) {
      ((TinyIntVector) vector).setSafe(idx, row.getByte(col));
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(idx, row.getFloat(col));
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(idx, row.getDouble(col));
    } else if (vector instanceof BitVector) {
      ((BitVector) vector).setSafe(idx, row.getBoolean(col) ? 1 : 0);
    } else if (vector instanceof TimeStampMilliVector) {
      Instant instant = row.getInstant(col);
      if (instant != null) {
        ((TimeStampMilliVector) vector).setSafe(idx, instant.toEpochMilli());
      }
    } else if (vector instanceof DateDayVector) {
      LocalDate date = row.getLocalDate(col);
      if (date != null) {
        ((DateDayVector) vector).setSafe(idx, (int) date.toEpochDay());
      }
    } else if (vector instanceof VarBinaryVector) {
      ByteBuffer buf = row.getByteBuffer(col);
      if (buf != null) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        ((VarBinaryVector) vector).setSafe(idx, bytes, 0, bytes.length);
      }
    } else if (vector instanceof DecimalVector) {
      BigDecimal bd = row.getBigDecimal(col);
      if (bd != null) {
        DecimalVector dv = (DecimalVector) vector;
        BigDecimal scaled = bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP);
        dv.setSafe(idx, scaled);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Collection writers — LIST / SET
  // ---------------------------------------------------------------------------

  /**
   * Writes a CQL LIST or SET value to a ListVector.
   *
   * Uses Arrow's direct write API:
   *   startNewValue(idx) → returns first child index
   *   writePrimitiveToChildVector(childVec, elem, childIdx) for each element
   *   endValue(idx, count)
   *
   * This avoids any allocator dependency: child vector writes use setSafe() directly.
   */
  private void writeListOrSetCell(ListVector lv, DataType type, Row row, String col, int idx) {
    Object raw = row.getObject(col);
    if (raw == null) {
      return;
    }

    DataType elemType;
    Iterable<?> elements;
    if (type instanceof ListType) {
      elemType = ((ListType) type).getElementType();
      elements = (List<?>) raw;
    } else if (type instanceof SetType) {
      elemType = ((SetType) type).getElementType();
      elements = (Set<?>) raw;
    } else {
      return; // unexpected
    }

    ValueVector childVec = lv.getDataVector();
    int startIdx  = lv.startNewValue(idx);
    int childIdx  = startIdx;

    for (Object elem : elements) {
      if (elem != null) {
        writePrimitiveToChildVector(childVec, elemType, elem, childIdx);
      }
      // null element: leave validity bit unset in child vector → Arrow null element
      childIdx++;
    }

    lv.endValue(idx, childIdx - startIdx);
  }

  // ---------------------------------------------------------------------------
  // Collection writers — MAP
  // ---------------------------------------------------------------------------

  /**
   * Writes a CQL MAP value to a MapVector.
   *
   * Arrow MapVector is internally a ListVector whose data vector is a StructVector
   * with "key" and "value" children. We write entries using the same
   * startNewValue / endValue protocol as ListVector, then directly write to
   * the key and value child vectors via setSafe().
   */
  private void writeMapCell(MapVector mv, DataType type, Row row, String col, int idx) {
    if (!(type instanceof MapType)) return;

    Map<?, ?> map = (Map<?, ?>) row.getObject(col);
    if (map == null) {
      return;
    }

    MapType   mt        = (MapType) type;
    DataType  keyType   = mt.getKeyType();
    DataType  valueType = mt.getValueType();

    // MapVector's data vector is the Struct<key,value> StructVector
    StructVector structVec = (StructVector) mv.getDataVector();
    // Children in declaration order: "key" at 0, "value" at 1
    List<org.apache.arrow.vector.FieldVector> children = structVec.getChildrenFromFields();
    ValueVector keyVec   = children.get(0);
    ValueVector valueVec = children.get(1);

    int startIdx = mv.startNewValue(idx);
    int entryIdx = startIdx;

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      // Write key — map keys are never null in Cassandra
      if (entry.getKey() != null) {
        writePrimitiveToChildVector(keyVec, keyType, entry.getKey(), entryIdx);
      }
      // Write value — may be null
      if (entry.getValue() != null) {
        writePrimitiveToChildVector(valueVec, valueType, entry.getValue(), entryIdx);
      }
      // Mark this struct entry as non-null so Arrow knows the slot is occupied
      structVec.setIndexDefined(entryIdx);
      entryIdx++;
    }

    mv.endValue(idx, entryIdx - startIdx);
  }

  // ---------------------------------------------------------------------------
  // Collection writers — UDT (Struct)
  // ---------------------------------------------------------------------------

  /**
   * Writes a CQL UDT value to a StructVector.
   *
   * Each UDT field maps to a named child vector in the StructVector.
   * We mark the struct slot as defined, then write each non-null field value
   * directly to its child vector using setSafe().
   */
  private void writeStructCell(StructVector sv, DataType type, Row row, String col, int idx) {
    if (!(type instanceof UserDefinedType)) return;

    UdtValue udt = (UdtValue) row.getObject(col);
    if (udt == null) {
      return;
    }

    sv.setIndexDefined(idx); // mark this struct slot as non-null

    UserDefinedType udtType = (UserDefinedType) type;
    List<com.datastax.oss.driver.api.core.CqlIdentifier> fieldNames =
        udtType.getFieldNames();
    List<DataType> fieldTypes = udtType.getFieldTypes();

    for (int i = 0; i < fieldNames.size(); i++) {
      String   fieldName = fieldNames.get(i).asInternal();
      DataType fieldType = fieldTypes.get(i);

      // Look up the matching child vector by name
      ValueVector childVec = sv.getChild(fieldName);
      if (childVec == null) {
        continue; // schema mismatch — skip silently
      }

      Object value = udt.getObject(fieldName);
      if (value != null) {
        writePrimitiveToChildVector(childVec, fieldType, value, idx);
      }
      // null field: leave validity bit unset in child vector → Arrow null
    }
  }

  // ---------------------------------------------------------------------------
  // Collection writers — VECTOR<float, N>  (Cassandra 5.x)
  // ---------------------------------------------------------------------------

  /**
   * Writes a Cassandra 5.x {@code VECTOR<float, N>} value into a
   * {@link FixedSizeListVector}.
   *
   * <p>Arrow {@code FixedSizeList(N)} stores all element slots contiguously in
   * a single child {@link Float4Vector}: entry at parent index {@code idx}
   * occupies child slots {@code [idx*N, (idx+1)*N)}.  We call
   * {@code setNotNull(idx)} on the parent to mark the list slot as defined, then
   * write each {@code float} element directly to the child via
   * {@code setSafe(childIdx, value)}.
   *
   * <p>A guard against the driver returning more floats than the declared
   * dimension is included, though in practice the driver always returns exactly
   * {@code N} elements for a well-formed VECTOR column.
   *
   * @param fslv  the {@code FixedSizeListVector} allocated for this column
   * @param type  Cassandra data type (must be {@link VectorType})
   * @param row   current result-set row
   * @param col   column name
   * @param idx   row index within the current Arrow batch
   */
  private void writeVectorCell(FixedSizeListVector fslv, DataType type,
                                Row row, String col, int idx) {
    if (!(type instanceof VectorType)) return;

    @SuppressWarnings("unchecked")
    CqlVector<Float> vec = (CqlVector<Float>) row.getObject(col);
    if (vec == null) return;

    Float4Vector childVec = (Float4Vector) fslv.getDataVector();
    int listSize  = fslv.getListSize();
    int startIdx  = listSize * idx;
    int i = 0;
    for (Float f : vec) {
      if (i >= listSize) break; // guard: never write past declared dimension
      if (f != null) {
        childVec.setSafe(startIdx + i, f);
      }
      // null element: leave validity bit unset in child vector → Arrow null float
      i++;
    }
    fslv.setNotNull(idx); // mark parent slot as non-null
  }

  // ---------------------------------------------------------------------------
  // Primitive writes into child vectors (used by all collection writers)
  // ---------------------------------------------------------------------------

  /**
   * Writes a single non-null primitive value into a child vector at {@code idx}.
   * The {@code javaValue} must match the type that the DataStax driver returns
   * for {@code cassType} (e.g. String for TEXT, Integer for INT, etc.).
   *
   * Uses setSafe() everywhere — reallocates if the vector needs to grow.
   * Does NOT handle nested collections (returns silently for complex types).
   */
  private static void writePrimitiveToChildVector(ValueVector vec, DataType cassType,
                                                   Object javaValue, int idx) {
    try {
      if (vec instanceof VarCharVector) {
        String str;
        if (javaValue instanceof String) {
          str = (String) javaValue;
        } else if (javaValue instanceof UUID) {
          str = javaValue.toString();
        } else if (javaValue instanceof InetAddress) {
          str = ((InetAddress) javaValue).getHostAddress();
        } else if (javaValue instanceof BigInteger) {
          str = javaValue.toString();
        } else {
          str = javaValue.toString();
        }
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) vec).setSafe(idx, bytes, 0, bytes.length);

      } else if (vec instanceof IntVector) {
        ((IntVector) vec).setSafe(idx, ((Number) javaValue).intValue());

      } else if (vec instanceof BigIntVector) {
        ((BigIntVector) vec).setSafe(idx, ((Number) javaValue).longValue());

      } else if (vec instanceof SmallIntVector) {
        ((SmallIntVector) vec).setSafe(idx, ((Number) javaValue).shortValue());

      } else if (vec instanceof TinyIntVector) {
        ((TinyIntVector) vec).setSafe(idx, ((Number) javaValue).byteValue());

      } else if (vec instanceof Float4Vector) {
        ((Float4Vector) vec).setSafe(idx, ((Number) javaValue).floatValue());

      } else if (vec instanceof Float8Vector) {
        ((Float8Vector) vec).setSafe(idx, ((Number) javaValue).doubleValue());

      } else if (vec instanceof BitVector) {
        ((BitVector) vec).setSafe(idx, Boolean.TRUE.equals(javaValue) ? 1 : 0);

      } else if (vec instanceof TimeStampMilliVector) {
        if (javaValue instanceof Instant) {
          ((TimeStampMilliVector) vec).setSafe(idx, ((Instant) javaValue).toEpochMilli());
        }

      } else if (vec instanceof DateDayVector) {
        if (javaValue instanceof LocalDate) {
          ((DateDayVector) vec).setSafe(idx, (int) ((LocalDate) javaValue).toEpochDay());
        }

      } else if (vec instanceof VarBinaryVector) {
        if (javaValue instanceof ByteBuffer) {
          ByteBuffer buf = (ByteBuffer) javaValue;
          byte[] bytes = new byte[buf.remaining()];
          buf.duplicate().get(bytes);
          ((VarBinaryVector) vec).setSafe(idx, bytes, 0, bytes.length);
        }

      } else if (vec instanceof DecimalVector) {
        BigDecimal bd = (javaValue instanceof BigDecimal)
            ? (BigDecimal) javaValue
            : new BigDecimal(javaValue.toString());
        DecimalVector dv = (DecimalVector) vec;
        ((DecimalVector) vec).setSafe(idx,
            bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP));

      } else {
        // Unknown vector type — best effort toString into VarChar fallback
        if (vec instanceof VarCharVector) {
          byte[] bytes = javaValue.toString().getBytes(StandardCharsets.UTF_8);
          ((VarCharVector) vec).setSafe(idx, bytes, 0, bytes.length);
        }
      }
    } catch (Exception e) {
      logger.debug("writePrimitiveToChildVector: col type={}, value={}, err={}",
          cassType, javaValue, e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // String representation for string-typed scalar columns
  // ---------------------------------------------------------------------------

  private String getAsString(Row row, String col, DataType type) {
    try {
      if (type == DataTypes.UUID || type == DataTypes.TIMEUUID) {
        UUID uuid = row.getUuid(col);
        return uuid != null ? uuid.toString() : null;
      } else if (type == DataTypes.INET) {
        InetAddress addr = row.getInetAddress(col);
        return addr != null ? addr.getHostAddress() : null;
      } else if (type == DataTypes.VARINT) {
        BigInteger bi = row.getBigInteger(col);
        return bi != null ? bi.toString() : null;
      } else {
        Object obj = row.getObject(col);
        return obj != null ? obj.toString() : null;
      }
    } catch (Exception e) {
      Object obj = row.getObject(col);
      return obj != null ? obj.toString() : null;
    }
  }

  // ---------------------------------------------------------------------------
  // Vector allocation helpers
  // ---------------------------------------------------------------------------

  private ValueVector allocateVector(Field field, OutputMutator output)
      throws SchemaChangeException {
    Class<? extends ValueVector> vectorClass = arrowTypeToVectorClass(field.getType());
    return output.addField(field, vectorClass);
  }

  /**
   * Maps an Arrow type to its ValueVector class.
   * All Cassandra columns are nullable, so we always use the nullable variant.
   * Complex types (List, Map, Struct) return their corresponding complex vector class.
   */
  @SuppressWarnings("unchecked")
  private static Class<? extends ValueVector> arrowTypeToVectorClass(ArrowType type) {
    if (type instanceof ArrowType.Utf8) {
      return VarCharVector.class;
    } else if (type instanceof ArrowType.Binary) {
      return VarBinaryVector.class;
    } else if (type instanceof ArrowType.Bool) {
      return BitVector.class;
    } else if (type instanceof ArrowType.Int) {
      switch (((ArrowType.Int) type).getBitWidth()) {
        case 8:  return TinyIntVector.class;
        case 16: return SmallIntVector.class;
        case 32: return IntVector.class;
        default: return BigIntVector.class;
      }
    } else if (type instanceof ArrowType.FloatingPoint) {
      if (((ArrowType.FloatingPoint) type).getPrecision()
          == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
        return Float4Vector.class;
      }
      return Float8Vector.class;
    } else if (type instanceof ArrowType.Timestamp) {
      return TimeStampMilliVector.class;
    } else if (type instanceof ArrowType.Date) {
      return DateDayVector.class;
    } else if (type instanceof ArrowType.Decimal) {
      return DecimalVector.class;
    } else if (type instanceof ArrowType.Duration) {
      return org.apache.arrow.vector.DurationVector.class;
    // ---- Native collection types -------------------------------------------
    } else if (type instanceof ArrowType.List) {
      return ListVector.class;
    } else if (type instanceof ArrowType.Map) {
      return MapVector.class;
    } else if (type instanceof ArrowType.Struct) {
      return StructVector.class;
    } else if (type instanceof ArrowType.FixedSizeList) {
      // Cassandra 5.x VECTOR<float, N> — fixed-length float array
      return FixedSizeListVector.class;
    } else {
      return VarCharVector.class; // fallback
    }
  }

  private boolean isProjected(List<SchemaPath> cols, String colName) {
    for (SchemaPath sp : cols) {
      if (sp.getRootSegment().getPath().equalsIgnoreCase(colName)) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  // Schema change detection
  // ---------------------------------------------------------------------------

  /**
   * Computes a stable fingerprint of a Cassandra table's column schema.
   *
   * The fingerprint is a sorted, comma-separated list of {@code "name:type"} pairs —
   * e.g. {@code "age:INT,email:TEXT,user_id:UUID"}.  Columns are sorted alphabetically
   * by name so the string is deterministic regardless of the order Cassandra returns them.
   *
   * Any change to a column name or type changes the fingerprint, triggering a
   * {@link UserException#schemaChangeError()} in {@link #setup(OutputMutator)} so Dremio
   * re-plans the query with the updated schema.
   *
   * @param table  live {@link com.datastax.oss.driver.api.core.metadata.schema.TableMetadata}
   *               from the DataStax driver
   * @return       non-null fingerprint string
   */
  static String computeSchemaHash(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table) {
    return table.getColumns().entrySet().stream()
        .sorted((a, b) -> a.getKey().asInternal().compareTo(b.getKey().asInternal()))
        .map(e -> e.getKey().asInternal() + ":" + e.getValue().getType().toString())
        .collect(Collectors.joining(","));
  }
}
