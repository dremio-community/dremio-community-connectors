package com.dremio.plugins.googleads;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Defines all 9 Google Ads tables: GAQL query, Arrow schema, and row mapper.
 *
 * The Google Ads REST API returns nested JSON objects per GAQL resource
 * (e.g. "campaign", "metrics", "segments"). Row mappers flatten these into
 * the flat column layout expected by Dremio.
 *
 * Numeric IDs and micros values come back as JSON strings from the REST API;
 * helpers handle both string and numeric JSON nodes.
 */
public class GoogleAdsTableDef {

  // -----------------------------------------------------------------------
  // Arrow type shortcuts
  // -----------------------------------------------------------------------
  private static final ArrowType VARCHAR = ArrowType.Utf8.INSTANCE;
  private static final ArrowType BIGINT  = new ArrowType.Int(64, true);
  private static final ArrowType DOUBLE  = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  private static Field f(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  public final String tableName;
  public final String gaqlTemplate;
  public final boolean isPerformanceTable;
  public final List<Field> fields;
  public final Function<JsonNode, Map<String, Object>> mapper;

  private GoogleAdsTableDef(String tableName, String gaqlTemplate,
                             boolean isPerformanceTable, List<Field> fields,
                             Function<JsonNode, Map<String, Object>> mapper) {
    this.tableName          = tableName;
    this.gaqlTemplate       = gaqlTemplate;
    this.isPerformanceTable = isPerformanceTable;
    this.fields             = Collections.unmodifiableList(fields);
    this.mapper             = mapper;
  }

  public String buildGaql(String startDate, String endDate) {
    return gaqlTemplate
        .replace("{start_date}", startDate)
        .replace("{end_date}", endDate);
  }

  // -----------------------------------------------------------------------
  // Helper extractors (handle string-encoded numbers from REST API)
  // -----------------------------------------------------------------------

  static Long lng(JsonNode node, String... path) {
    JsonNode n = dig(node, path);
    if (n == null || n.isNull() || n.isMissingNode()) return null;
    if (n.isNumber()) return n.longValue();
    String s = n.asText("").trim();
    if (s.isEmpty() || s.equals("null")) return null;
    try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
  }

  static Double dbl(JsonNode node, String... path) {
    JsonNode n = dig(node, path);
    if (n == null || n.isNull() || n.isMissingNode()) return null;
    if (n.isNumber()) return n.doubleValue();
    String s = n.asText("").trim();
    if (s.isEmpty() || s.equals("null")) return null;
    try { return Double.parseDouble(s); } catch (NumberFormatException e) { return null; }
  }

  static String str(JsonNode node, String... path) {
    JsonNode n = dig(node, path);
    if (n == null || n.isNull() || n.isMissingNode()) return null;
    if (n.isArray()) {
      StringBuilder sb = new StringBuilder();
      n.forEach(e -> { if (sb.length() > 0) sb.append(','); sb.append(e.asText()); });
      return sb.toString();
    }
    String s = n.asText("").trim();
    return s.isEmpty() ? null : s;
  }

  private static JsonNode dig(JsonNode node, String... path) {
    JsonNode cur = node;
    for (String p : path) {
      if (cur == null || cur.isNull() || cur.isMissingNode()) return null;
      cur = cur.get(p);
    }
    return cur;
  }

  private static Map<String, Object> row(Object... kvs) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      m.put((String) kvs[i], kvs[i + 1]);
    }
    return m;
  }

  // -----------------------------------------------------------------------
  // Table definitions
  // -----------------------------------------------------------------------

  public static final GoogleAdsTableDef CAMPAIGNS = new GoogleAdsTableDef(
      "campaigns",
      "SELECT campaign.id, campaign.name, campaign.status,"
      + " campaign.advertising_channel_type, campaign.bidding_strategy_type,"
      + " campaign_budget.amount_micros, campaign.optimization_score"
      + " FROM campaign ORDER BY campaign.id",
      false,
      Arrays.asList(
          f("campaign_id",              BIGINT),
          f("campaign_name",            VARCHAR),
          f("status",                   VARCHAR),
          f("advertising_channel_type", VARCHAR),
          f("bidding_strategy_type",    VARCHAR),
          f("budget_amount_micros",     BIGINT),
          f("optimization_score",       DOUBLE)
      ),
      r -> row(
          "campaign_id",              lng(r, "campaign", "id"),
          "campaign_name",            str(r, "campaign", "name"),
          "status",                   str(r, "campaign", "status"),
          "advertising_channel_type", str(r, "campaign", "advertisingChannelType"),
          "bidding_strategy_type",    str(r, "campaign", "biddingStrategyType"),
          "budget_amount_micros",     lng(r, "campaignBudget", "amountMicros"),
          "optimization_score",       dbl(r, "campaign", "optimizationScore")
      )
  );

  public static final GoogleAdsTableDef AD_GROUPS = new GoogleAdsTableDef(
      "ad_groups",
      "SELECT ad_group.id, ad_group.name, ad_group.status,"
      + " ad_group.type, campaign.id, campaign.name,"
      + " ad_group.cpc_bid_micros, ad_group.target_cpa_micros"
      + " FROM ad_group ORDER BY ad_group.id",
      false,
      Arrays.asList(
          f("ad_group_id",       BIGINT),
          f("ad_group_name",     VARCHAR),
          f("status",            VARCHAR),
          f("type",              VARCHAR),
          f("campaign_id",       BIGINT),
          f("campaign_name",     VARCHAR),
          f("cpc_bid_micros",    BIGINT),
          f("target_cpa_micros", BIGINT)
      ),
      r -> row(
          "ad_group_id",       lng(r, "adGroup", "id"),
          "ad_group_name",     str(r, "adGroup", "name"),
          "status",            str(r, "adGroup", "status"),
          "type",              str(r, "adGroup", "type"),
          "campaign_id",       lng(r, "campaign", "id"),
          "campaign_name",     str(r, "campaign", "name"),
          "cpc_bid_micros",    lng(r, "adGroup", "cpcBidMicros"),
          "target_cpa_micros", lng(r, "adGroup", "targetCpaMicros")
      )
  );

  public static final GoogleAdsTableDef ADS = new GoogleAdsTableDef(
      "ads",
      "SELECT ad_group_ad.ad.id, ad_group_ad.ad.name,"
      + " ad_group_ad.ad.type, ad_group_ad.status,"
      + " ad_group.id, ad_group.name,"
      + " campaign.id, campaign.name,"
      + " ad_group_ad.ad.final_urls, ad_group_ad.ad.display_url"
      + " FROM ad_group_ad ORDER BY ad_group_ad.ad.id",
      false,
      Arrays.asList(
          f("ad_id",          BIGINT),
          f("ad_name",        VARCHAR),
          f("ad_type",        VARCHAR),
          f("status",         VARCHAR),
          f("ad_group_id",    BIGINT),
          f("ad_group_name",  VARCHAR),
          f("campaign_id",    BIGINT),
          f("campaign_name",  VARCHAR),
          f("final_urls",     VARCHAR),
          f("display_url",    VARCHAR)
      ),
      r -> row(
          "ad_id",         lng(r, "adGroupAd", "ad", "id"),
          "ad_name",       str(r, "adGroupAd", "ad", "name"),
          "ad_type",       str(r, "adGroupAd", "ad", "type"),
          "status",        str(r, "adGroupAd", "status"),
          "ad_group_id",   lng(r, "adGroup", "id"),
          "ad_group_name", str(r, "adGroup", "name"),
          "campaign_id",   lng(r, "campaign", "id"),
          "campaign_name", str(r, "campaign", "name"),
          "final_urls",    str(r, "adGroupAd", "ad", "finalUrls"),
          "display_url",   str(r, "adGroupAd", "ad", "displayUrl")
      )
  );

  public static final GoogleAdsTableDef KEYWORDS = new GoogleAdsTableDef(
      "keywords",
      "SELECT ad_group_criterion.criterion_id,"
      + " ad_group_criterion.keyword.text,"
      + " ad_group_criterion.keyword.match_type,"
      + " ad_group_criterion.status,"
      + " ad_group_criterion.cpc_bid_micros,"
      + " ad_group_criterion.quality_info.quality_score,"
      + " ad_group.id, ad_group.name, campaign.id, campaign.name"
      + " FROM ad_group_criterion"
      + " WHERE ad_group_criterion.type = 'KEYWORD'"
      + " ORDER BY ad_group_criterion.criterion_id",
      false,
      Arrays.asList(
          f("criterion_id",  BIGINT),
          f("keyword_text",  VARCHAR),
          f("match_type",    VARCHAR),
          f("status",        VARCHAR),
          f("cpc_bid_micros",BIGINT),
          f("quality_score", BIGINT),
          f("ad_group_id",   BIGINT),
          f("ad_group_name", VARCHAR),
          f("campaign_id",   BIGINT),
          f("campaign_name", VARCHAR)
      ),
      r -> row(
          "criterion_id",  lng(r, "adGroupCriterion", "criterionId"),
          "keyword_text",  str(r, "adGroupCriterion", "keyword", "text"),
          "match_type",    str(r, "adGroupCriterion", "keyword", "matchType"),
          "status",        str(r, "adGroupCriterion", "status"),
          "cpc_bid_micros",lng(r, "adGroupCriterion", "cpcBidMicros"),
          "quality_score", lng(r, "adGroupCriterion", "qualityInfo", "qualityScore"),
          "ad_group_id",   lng(r, "adGroup", "id"),
          "ad_group_name", str(r, "adGroup", "name"),
          "campaign_id",   lng(r, "campaign", "id"),
          "campaign_name", str(r, "campaign", "name")
      )
  );

  public static final GoogleAdsTableDef CAMPAIGN_PERFORMANCE = new GoogleAdsTableDef(
      "campaign_performance",
      "SELECT campaign.id, campaign.name, campaign.status,"
      + " segments.date,"
      + " metrics.impressions, metrics.clicks, metrics.cost_micros,"
      + " metrics.conversions, metrics.conversions_value,"
      + " metrics.ctr, metrics.average_cpc, metrics.view_through_conversions"
      + " FROM campaign"
      + " WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
      + " ORDER BY segments.date, campaign.id",
      true,
      Arrays.asList(
          f("campaign_id",              BIGINT),
          f("campaign_name",            VARCHAR),
          f("status",                   VARCHAR),
          f("date",                     VARCHAR),
          f("impressions",              BIGINT),
          f("clicks",                   BIGINT),
          f("cost_micros",              BIGINT),
          f("conversions",              DOUBLE),
          f("conversions_value",        DOUBLE),
          f("ctr",                      DOUBLE),
          f("average_cpc",              DOUBLE),
          f("view_through_conversions", BIGINT)
      ),
      r -> row(
          "campaign_id",              lng(r, "campaign", "id"),
          "campaign_name",            str(r, "campaign", "name"),
          "status",                   str(r, "campaign", "status"),
          "date",                     str(r, "segments", "date"),
          "impressions",              lng(r, "metrics", "impressions"),
          "clicks",                   lng(r, "metrics", "clicks"),
          "cost_micros",              lng(r, "metrics", "costMicros"),
          "conversions",              dbl(r, "metrics", "conversions"),
          "conversions_value",        dbl(r, "metrics", "conversionsValue"),
          "ctr",                      dbl(r, "metrics", "ctr"),
          "average_cpc",              dbl(r, "metrics", "averageCpc"),
          "view_through_conversions", lng(r, "metrics", "viewThroughConversions")
      )
  );

  public static final GoogleAdsTableDef AD_GROUP_PERFORMANCE = new GoogleAdsTableDef(
      "ad_group_performance",
      "SELECT ad_group.id, ad_group.name,"
      + " campaign.id, campaign.name,"
      + " segments.date,"
      + " metrics.impressions, metrics.clicks, metrics.cost_micros,"
      + " metrics.conversions, metrics.conversions_value,"
      + " metrics.ctr, metrics.average_cpc"
      + " FROM ad_group"
      + " WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
      + " ORDER BY segments.date, ad_group.id",
      true,
      Arrays.asList(
          f("ad_group_id",      BIGINT),
          f("ad_group_name",    VARCHAR),
          f("campaign_id",      BIGINT),
          f("campaign_name",    VARCHAR),
          f("date",             VARCHAR),
          f("impressions",      BIGINT),
          f("clicks",           BIGINT),
          f("cost_micros",      BIGINT),
          f("conversions",      DOUBLE),
          f("conversions_value",DOUBLE),
          f("ctr",              DOUBLE),
          f("average_cpc",      DOUBLE)
      ),
      r -> row(
          "ad_group_id",       lng(r, "adGroup", "id"),
          "ad_group_name",     str(r, "adGroup", "name"),
          "campaign_id",       lng(r, "campaign", "id"),
          "campaign_name",     str(r, "campaign", "name"),
          "date",              str(r, "segments", "date"),
          "impressions",       lng(r, "metrics", "impressions"),
          "clicks",            lng(r, "metrics", "clicks"),
          "cost_micros",       lng(r, "metrics", "costMicros"),
          "conversions",       dbl(r, "metrics", "conversions"),
          "conversions_value", dbl(r, "metrics", "conversionsValue"),
          "ctr",               dbl(r, "metrics", "ctr"),
          "average_cpc",       dbl(r, "metrics", "averageCpc")
      )
  );

  public static final GoogleAdsTableDef SEARCH_TERMS = new GoogleAdsTableDef(
      "search_terms",
      "SELECT search_term_view.search_term, search_term_view.status,"
      + " campaign.id, campaign.name,"
      + " ad_group.id, ad_group.name,"
      + " segments.date,"
      + " metrics.impressions, metrics.clicks, metrics.cost_micros,"
      + " metrics.conversions, metrics.ctr, metrics.average_cpc"
      + " FROM search_term_view"
      + " WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
      + " ORDER BY segments.date",
      true,
      Arrays.asList(
          f("search_term",  VARCHAR),
          f("status",       VARCHAR),
          f("campaign_id",  BIGINT),
          f("campaign_name",VARCHAR),
          f("ad_group_id",  BIGINT),
          f("ad_group_name",VARCHAR),
          f("date",         VARCHAR),
          f("impressions",  BIGINT),
          f("clicks",       BIGINT),
          f("cost_micros",  BIGINT),
          f("conversions",  DOUBLE),
          f("ctr",          DOUBLE),
          f("average_cpc",  DOUBLE)
      ),
      r -> row(
          "search_term",  str(r, "searchTermView", "searchTerm"),
          "status",       str(r, "searchTermView", "status"),
          "campaign_id",  lng(r, "campaign", "id"),
          "campaign_name",str(r, "campaign", "name"),
          "ad_group_id",  lng(r, "adGroup", "id"),
          "ad_group_name",str(r, "adGroup", "name"),
          "date",         str(r, "segments", "date"),
          "impressions",  lng(r, "metrics", "impressions"),
          "clicks",       lng(r, "metrics", "clicks"),
          "cost_micros",  lng(r, "metrics", "costMicros"),
          "conversions",  dbl(r, "metrics", "conversions"),
          "ctr",          dbl(r, "metrics", "ctr"),
          "average_cpc",  dbl(r, "metrics", "averageCpc")
      )
  );

  public static final GoogleAdsTableDef GEOGRAPHIC_PERFORMANCE = new GoogleAdsTableDef(
      "geographic_performance",
      "SELECT geographic_view.country_criterion_id, geographic_view.resource_name,"
      + " campaign.id, campaign.name,"
      + " segments.date,"
      + " metrics.impressions, metrics.clicks, metrics.cost_micros,"
      + " metrics.conversions, metrics.ctr"
      + " FROM geographic_view"
      + " WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
      + " ORDER BY segments.date",
      true,
      Arrays.asList(
          f("country_criterion_id", BIGINT),
          f("resource_name",        VARCHAR),
          f("campaign_id",          BIGINT),
          f("campaign_name",        VARCHAR),
          f("date",                 VARCHAR),
          f("impressions",          BIGINT),
          f("clicks",               BIGINT),
          f("cost_micros",          BIGINT),
          f("conversions",          DOUBLE),
          f("ctr",                  DOUBLE)
      ),
      r -> row(
          "country_criterion_id", lng(r, "geographicView", "countryCriterionId"),
          "resource_name",        str(r, "geographicView", "resourceName"),
          "campaign_id",          lng(r, "campaign", "id"),
          "campaign_name",        str(r, "campaign", "name"),
          "date",                 str(r, "segments", "date"),
          "impressions",          lng(r, "metrics", "impressions"),
          "clicks",               lng(r, "metrics", "clicks"),
          "cost_micros",          lng(r, "metrics", "costMicros"),
          "conversions",          dbl(r, "metrics", "conversions"),
          "ctr",                  dbl(r, "metrics", "ctr")
      )
  );

  public static final GoogleAdsTableDef AUDIENCE_PERFORMANCE = new GoogleAdsTableDef(
      "audience_performance",
      "SELECT ad_group_audience_view.resource_name,"
      + " ad_group.id, ad_group.name,"
      + " campaign.id, campaign.name,"
      + " segments.date,"
      + " metrics.impressions, metrics.clicks, metrics.cost_micros,"
      + " metrics.conversions"
      + " FROM ad_group_audience_view"
      + " WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
      + " ORDER BY segments.date",
      true,
      Arrays.asList(
          f("resource_name", VARCHAR),
          f("ad_group_id",   BIGINT),
          f("ad_group_name", VARCHAR),
          f("campaign_id",   BIGINT),
          f("campaign_name", VARCHAR),
          f("date",          VARCHAR),
          f("impressions",   BIGINT),
          f("clicks",        BIGINT),
          f("cost_micros",   BIGINT),
          f("conversions",   DOUBLE)
      ),
      r -> row(
          "resource_name", str(r, "adGroupAudienceView", "resourceName"),
          "ad_group_id",   lng(r, "adGroup", "id"),
          "ad_group_name", str(r, "adGroup", "name"),
          "campaign_id",   lng(r, "campaign", "id"),
          "campaign_name", str(r, "campaign", "name"),
          "date",          str(r, "segments", "date"),
          "impressions",   lng(r, "metrics", "impressions"),
          "clicks",        lng(r, "metrics", "clicks"),
          "cost_micros",   lng(r, "metrics", "costMicros"),
          "conversions",   dbl(r, "metrics", "conversions")
      )
  );

  // -----------------------------------------------------------------------
  // Registry
  // -----------------------------------------------------------------------

  public static final Map<String, GoogleAdsTableDef> ALL;
  static {
    Map<String, GoogleAdsTableDef> m = new LinkedHashMap<>();
    for (GoogleAdsTableDef t : new GoogleAdsTableDef[]{
        CAMPAIGNS, AD_GROUPS, ADS, KEYWORDS,
        CAMPAIGN_PERFORMANCE, AD_GROUP_PERFORMANCE, SEARCH_TERMS,
        GEOGRAPHIC_PERFORMANCE, AUDIENCE_PERFORMANCE}) {
      m.put(t.tableName, t);
    }
    ALL = Collections.unmodifiableMap(m);
  }
}
