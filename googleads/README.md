# Dremio Google Ads Connector

A Dremio storage plugin that exposes Google Ads account data as queryable SQL tables via the Google Ads REST API.

## Tables

| Table | Type | Description |
|-------|------|-------------|
| `campaigns` | Snapshot | Campaign metadata — name, status, channel type, bidding strategy, budget |
| `ad_groups` | Snapshot | Ad group metadata — name, status, type, bids |
| `ads` | Snapshot | Ad/creative metadata — type, status, final URLs |
| `keywords` | Snapshot | Keyword targets — text, match type, bids, quality score |
| `campaign_performance` | Date range | Daily metrics by campaign — impressions, clicks, cost, conversions |
| `ad_group_performance` | Date range | Daily metrics by ad group |
| `search_terms` | Date range | Search term performance — what users actually searched |
| `geographic_performance` | Date range | Performance by country |
| `audience_performance` | Date range | Performance by audience segment |

Performance tables default to the last 30 days (configurable via **Date Range Days**).

## Authentication

Google Ads requires three credentials:

1. **Developer Token** — obtained from your Google Ads Manager Account → API Center
2. **OAuth2 Client ID + Secret** — from a Google Cloud Console project with the Google Ads API enabled
3. **OAuth2 Refresh Token** — obtained via the OAuth2 consent flow

### Getting a refresh token

```bash
pip install google-auth-oauthlib
python3 - <<'EOF'
from google_auth_oauthlib.flow import InstalledAppFlow
flow = InstalledAppFlow.from_client_config(
    {"installed": {
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }},
    scopes=["https://www.googleapis.com/auth/adwords"]
)
creds = flow.run_console()
print("Refresh token:", creds.refresh_token)
EOF
```

## Setup

### 1. Build

```bash
cd dremio-googleads-connector
./rebuild.sh --force
```

### 2. Install

```bash
./install.sh try-dremio
```

### 3. Register the source

```bash
./add-googleads-source.sh \
  --name google_ads \
  --developer-token YOUR_DEV_TOKEN \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET \
  --refresh-token YOUR_REFRESH_TOKEN \
  --customer-id 1234567890
```

Or via the Dremio UI: **Sources → + → Google Ads**

## Example queries

```sql
-- All active campaigns with budget
SELECT campaign_id, campaign_name, status, budget_amount_micros / 1000000.0 AS budget_usd
FROM google_ads.campaigns
WHERE status = 'ENABLED'
ORDER BY budget_usd DESC;

-- Last 30 days spend and conversions by campaign
SELECT campaign_name,
       SUM(impressions)  AS total_impressions,
       SUM(clicks)       AS total_clicks,
       SUM(cost_micros) / 1000000.0 AS spend_usd,
       SUM(conversions)  AS total_conversions,
       SUM(cost_micros) / NULLIF(SUM(conversions), 0) / 1000000.0 AS cpa_usd
FROM google_ads.campaign_performance
GROUP BY campaign_name
ORDER BY spend_usd DESC;

-- Top 20 search terms by clicks
SELECT search_term, SUM(clicks) AS clicks, SUM(impressions) AS impressions,
       SUM(cost_micros) / 1000000.0 AS spend_usd
FROM google_ads.search_terms
GROUP BY search_term
ORDER BY clicks DESC
LIMIT 20;

-- Keywords with low quality score (optimization candidates)
SELECT keyword_text, match_type, quality_score, campaign_name, ad_group_name
FROM google_ads.keywords
WHERE quality_score < 5 AND status = 'ENABLED'
ORDER BY quality_score ASC;

-- Daily spend trend
SELECT date, SUM(cost_micros) / 1000000.0 AS daily_spend_usd, SUM(clicks) AS clicks
FROM google_ads.campaign_performance
GROUP BY date
ORDER BY date DESC;

-- Geographic performance (top countries by conversions)
SELECT country_criterion_id, campaign_name,
       SUM(conversions) AS conversions,
       SUM(cost_micros) / 1000000.0 AS spend_usd
FROM google_ads.geographic_performance
GROUP BY country_criterion_id, campaign_name
ORDER BY conversions DESC
LIMIT 20;

-- Cross-source: compare ad spend with Salesforce opportunities
SELECT g.campaign_name, SUM(g.cost_micros)/1000000.0 AS spend_usd,
       COUNT(s.id) AS opps_created
FROM google_ads.campaign_performance g
LEFT JOIN salesforce.opportunity s ON s.campaign_source LIKE CONCAT('%', g.campaign_name, '%')
GROUP BY g.campaign_name
ORDER BY spend_usd DESC;
```

## Configuration reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| Developer Token | ✅ | — | From Google Ads Manager Account → API Center |
| OAuth2 Client ID | ✅ | — | From Google Cloud Console |
| OAuth2 Client Secret | ✅ | — | From Google Cloud Console |
| OAuth2 Refresh Token | ✅ | — | From OAuth2 consent flow |
| Customer ID | ✅ | — | 10-digit Google Ads account ID (no dashes) |
| Manager Account ID | ❌ | — | MCC account ID if accessing sub-accounts |
| Date Range Days | ❌ | 30 | Days back for performance report tables |

## Notes

- **Cost values** are in micros (millionths of a currency unit). Divide by 1,000,000 for the actual amount.
- **Manager accounts**: If your customer ID is a manager/MCC account, set Manager Account ID to the same value. Performance queries will automatically route to accessible client accounts.
- **API rate limits**: Google Ads API has daily query limits. For high-frequency dashboards, set a reasonable Date Range Days to limit result size.
- **Test accounts**: Use a Google Ads test account during development to avoid affecting live campaigns.
