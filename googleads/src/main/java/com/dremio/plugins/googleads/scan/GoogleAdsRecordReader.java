package com.dremio.plugins.googleads.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.googleads.GoogleAdsClient;
import com.dremio.plugins.googleads.GoogleAdsTableDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads Google Ads data into Arrow vectors.
 *
 * setup(): executes the GAQL query and buffers all rows.
 * next():  drains the buffer into Arrow vectors in batches.
 *
 * Critical: ONLY use output.getVector(name), NEVER output.addField().
 */
public class GoogleAdsRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(GoogleAdsRecordReader.class);
  private static final int TARGET_BATCH_SIZE = 4_000;

  private final GoogleAdsClient   client;
  private final GoogleAdsTableDef tableDef;
  private final int               dateRangeDays;

  private List<Map<String, Object>> rows;
  private int rowIndex = 0;
  private ValueVector[] vectors;

  public GoogleAdsRecordReader(OperatorContext context, GoogleAdsClient client,
                                GoogleAdsTableDef tableDef, int dateRangeDays) {
    super(context, null);
    this.client        = client;
    this.tableDef      = tableDef;
    this.dateRangeDays = dateRangeDays;
  }

  // -----------------------------------------------------------------------
  // Setup
  // -----------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      rows = fetchRows();
    } catch (ExecutionSetupException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionSetupException("Failed to fetch Google Ads data for table: " + tableDef.tableName, e);
    }
    allocateVectors(output);
    logger.info("[google_ads/{}] fetched {} rows", tableDef.tableName, rows.size());
  }

  private List<Map<String, Object>> fetchRows() throws Exception {
    String gaql;
    if (tableDef.isPerformanceTable) {
      LocalDate endDate   = LocalDate.now();
      LocalDate startDate = endDate.minusDays(dateRangeDays);
      DateTimeFormatter fmt = DateTimeFormatter.ISO_LOCAL_DATE;
      gaql = tableDef.buildGaql(startDate.format(fmt), endDate.format(fmt));
    } else {
      gaql = tableDef.gaqlTemplate;
    }

    List<JsonNode> results = client.search(gaql);
    List<Map<String, Object>> mapped = new ArrayList<>(results.size());
    for (JsonNode r : results) {
      mapped.add(tableDef.mapper.apply(r));
    }
    return mapped;
  }

  private void allocateVectors(OutputMutator output) {
    List<Field> fields = tableDef.fields;
    vectors = new ValueVector[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      vectors[i] = output.getVector(fields.get(i).getName());
    }
  }

  // -----------------------------------------------------------------------
  // Read loop
  // -----------------------------------------------------------------------

  @Override
  public int next() {
    if (rowIndex >= rows.size()) return 0;

    int batchEnd = Math.min(rowIndex + TARGET_BATCH_SIZE, rows.size());
    int count    = batchEnd - rowIndex;
    List<Field> fields = tableDef.fields;

    for (int vi = 0; vi < fields.size(); vi++) {
      Field f = fields.get(vi);
      ValueVector vec = vectors[vi];

      if (vec instanceof VarCharVector) {
        VarCharVector v = (VarCharVector) vec;
        v.allocateNew(count);
        for (int ri = 0; ri < count; ri++) {
          Object val = rows.get(rowIndex + ri).get(f.getName());
          if (val == null) {
            v.setNull(ri);
          } else {
            byte[] b = val.toString().getBytes(StandardCharsets.UTF_8);
            v.setSafe(ri, b, 0, b.length);
          }
        }
        v.setValueCount(count);

      } else if (vec instanceof BigIntVector) {
        BigIntVector v = (BigIntVector) vec;
        v.allocateNew(count);
        for (int ri = 0; ri < count; ri++) {
          Object val = rows.get(rowIndex + ri).get(f.getName());
          if (val == null) v.setNull(ri);
          else v.setSafe(ri, ((Number) val).longValue());
        }
        v.setValueCount(count);

      } else if (vec instanceof Float8Vector) {
        Float8Vector v = (Float8Vector) vec;
        v.allocateNew(count);
        for (int ri = 0; ri < count; ri++) {
          Object val = rows.get(rowIndex + ri).get(f.getName());
          if (val == null) v.setNull(ri);
          else v.setSafe(ri, ((Number) val).doubleValue());
        }
        v.setValueCount(count);
      }
    }

    rowIndex = batchEnd;
    return count;
  }

  @Override
  public void close() {
    rows = null;
  }
}
