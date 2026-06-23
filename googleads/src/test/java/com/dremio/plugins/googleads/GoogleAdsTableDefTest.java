package com.dremio.plugins.googleads;

import com.dremio.plugins.googleads.scan.GoogleAdsSubScan.GoogleAdsScanSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GoogleAdsTableDefTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void allNineTablesPresent() {
    assertEquals(9, GoogleAdsTableDef.ALL.size());
    assertTrue(GoogleAdsTableDef.ALL.containsKey("campaigns"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("ad_groups"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("ads"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("keywords"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("campaign_performance"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("ad_group_performance"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("search_terms"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("geographic_performance"));
    assertTrue(GoogleAdsTableDef.ALL.containsKey("audience_performance"));
  }

  @Test
  void performanceTableFlags() {
    assertFalse(GoogleAdsTableDef.CAMPAIGNS.isPerformanceTable);
    assertFalse(GoogleAdsTableDef.AD_GROUPS.isPerformanceTable);
    assertFalse(GoogleAdsTableDef.ADS.isPerformanceTable);
    assertFalse(GoogleAdsTableDef.KEYWORDS.isPerformanceTable);
    assertTrue(GoogleAdsTableDef.CAMPAIGN_PERFORMANCE.isPerformanceTable);
    assertTrue(GoogleAdsTableDef.AD_GROUP_PERFORMANCE.isPerformanceTable);
    assertTrue(GoogleAdsTableDef.SEARCH_TERMS.isPerformanceTable);
    assertTrue(GoogleAdsTableDef.GEOGRAPHIC_PERFORMANCE.isPerformanceTable);
    assertTrue(GoogleAdsTableDef.AUDIENCE_PERFORMANCE.isPerformanceTable);
  }

  @Test
  void buildGaqlInterpolatesDateRange() {
    String gaql = GoogleAdsTableDef.CAMPAIGN_PERFORMANCE
        .buildGaql("2024-01-01", "2024-01-31");
    assertTrue(gaql.contains("2024-01-01"), "start_date not interpolated");
    assertTrue(gaql.contains("2024-01-31"), "end_date not interpolated");
    assertFalse(gaql.contains("{start_date}"), "placeholder not replaced");
    assertFalse(gaql.contains("{end_date}"),   "placeholder not replaced");
  }

  @Test
  void staticTableGaqlHasNoPlaceholders() {
    for (GoogleAdsTableDef def : new GoogleAdsTableDef[]{
        GoogleAdsTableDef.CAMPAIGNS, GoogleAdsTableDef.AD_GROUPS,
        GoogleAdsTableDef.ADS, GoogleAdsTableDef.KEYWORDS}) {
      assertFalse(def.gaqlTemplate.contains("{start_date}"),
          def.tableName + " should not have {start_date}");
    }
  }

  @Test
  void campaignMapperExtractsFields() {
    ObjectNode row = MAPPER.createObjectNode();
    ObjectNode campaign = row.putObject("campaign");
    campaign.put("id", "123456");
    campaign.put("name", "Test Campaign");
    campaign.put("status", "ENABLED");
    campaign.put("advertisingChannelType", "SEARCH");
    campaign.put("biddingStrategyType", "TARGET_CPA");
    campaign.put("optimizationScore", 0.87);
    row.putObject("campaignBudget").put("amountMicros", "5000000");

    Map<String, Object> mapped = GoogleAdsTableDef.CAMPAIGNS.mapper.apply(row);
    assertEquals(123456L, mapped.get("campaign_id"));
    assertEquals("Test Campaign", mapped.get("campaign_name"));
    assertEquals("ENABLED", mapped.get("status"));
    assertEquals("SEARCH", mapped.get("advertising_channel_type"));
    assertEquals("TARGET_CPA", mapped.get("bidding_strategy_type"));
    assertEquals(5000000L, mapped.get("budget_amount_micros"));
    assertEquals(0.87, (Double) mapped.get("optimization_score"), 0.001);
  }

  @Test
  void campaignPerformanceMapperExtractsMetrics() {
    ObjectNode row = MAPPER.createObjectNode();
    row.putObject("campaign").put("id", "789").put("name", "Perf Campaign").put("status", "ENABLED");
    row.putObject("segments").put("date", "2024-01-15");
    ObjectNode metrics = row.putObject("metrics");
    metrics.put("impressions", "10000");
    metrics.put("clicks", "500");
    metrics.put("costMicros", "25000000");
    metrics.put("conversions", 12.5);
    metrics.put("ctr", 0.05);
    metrics.put("averageCpc", 50000.0);
    metrics.put("viewThroughConversions", "20");

    Map<String, Object> mapped = GoogleAdsTableDef.CAMPAIGN_PERFORMANCE.mapper.apply(row);
    assertEquals(789L,         mapped.get("campaign_id"));
    assertEquals("2024-01-15", mapped.get("date"));
    assertEquals(10000L,       mapped.get("impressions"));
    assertEquals(500L,         mapped.get("clicks"));
    assertEquals(25000000L,    mapped.get("cost_micros"));
    assertEquals(12.5,  (Double) mapped.get("conversions"),  0.001);
    assertEquals(0.05,  (Double) mapped.get("ctr"),           0.001);
    assertEquals(20L,          mapped.get("view_through_conversions"));
  }

  @Test
  void keywordMapperExtractsNestedFields() {
    ObjectNode row = MAPPER.createObjectNode();
    ObjectNode criterion = row.putObject("adGroupCriterion");
    criterion.put("criterionId", "999");
    criterion.putObject("keyword").put("text", "running shoes").put("matchType", "BROAD");
    criterion.put("status", "ENABLED");
    criterion.put("cpcBidMicros", "300000");
    criterion.putObject("qualityInfo").put("qualityScore", 7);
    row.putObject("adGroup").put("id", "111").put("name", "Shoes Group");
    row.putObject("campaign").put("id", "222").put("name", "Shoes Campaign");

    Map<String, Object> mapped = GoogleAdsTableDef.KEYWORDS.mapper.apply(row);
    assertEquals(999L,            mapped.get("criterion_id"));
    assertEquals("running shoes", mapped.get("keyword_text"));
    assertEquals("BROAD",         mapped.get("match_type"));
    assertEquals(300000L,         mapped.get("cpc_bid_micros"));
    assertEquals(7L,              mapped.get("quality_score"));
    assertEquals(111L,            mapped.get("ad_group_id"));
  }

  @Test
  void nullSafeExtractors() {
    ObjectNode empty = MAPPER.createObjectNode();
    assertNull(GoogleAdsTableDef.lng(empty, "campaign", "id"));
    assertNull(GoogleAdsTableDef.dbl(empty, "metrics", "ctr"));
    assertNull(GoogleAdsTableDef.str(empty, "segments", "date"));
  }

  @Test
  void allTablesHaveNonEmptyFieldList() {
    for (GoogleAdsTableDef def : GoogleAdsTableDef.ALL.values()) {
      assertFalse(def.fields.isEmpty(),
          def.tableName + " should have at least one field");
    }
  }

  @Test
  void scanSpecRoundTrip() {
    GoogleAdsScanSpec spec = new GoogleAdsScanSpec("campaign_performance", 60);
    GoogleAdsScanSpec restored = GoogleAdsScanSpec.fromExtendedProperty(spec.toExtendedProperty());
    assertEquals("campaign_performance", restored.getTableName());
    assertEquals(60, restored.getDateRangeDays());
  }
}
