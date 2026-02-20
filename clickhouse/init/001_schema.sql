-- property_intel database is auto-created via CLICKHOUSE_DB env var
-- This file runs on first boot to set up the schema and useful views.

-- ============================================================
-- CORE TABLES
-- ============================================================

-- Main crimes table
-- ReplacingMergeTree deduplicates rows with identical ORDER BY key
-- Partitioned by month for efficient range scans
CREATE TABLE IF NOT EXISTS property_intel.crimes
(
    persistent_id       Nullable(String),
    api_id              Nullable(Int64),
    category            LowCardinality(String),
    category_name       LowCardinality(String),
    crime_month         String,
    crime_date          Date,
    postcode_district   LowCardinality(String),
    street_name         Nullable(String),
    street_id           Nullable(Int64),
    latitude            Nullable(Float64),
    longitude           Nullable(Float64),
    location_type       LowCardinality(Nullable(String)),
    outcome_category    LowCardinality(Nullable(String)),
    outcome_date        Nullable(String),
    scraped_at          DateTime,
    source_endpoint     Nullable(String)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(crime_date)
ORDER BY (postcode_district, crime_date, category)
SETTINGS index_granularity = 8192;

-- Scrape run audit log
CREATE TABLE IF NOT EXISTS property_intel.scrape_runs
(
    run_id              String,
    target_month        String,
    postcode_district   LowCardinality(String),
    started_at          DateTime,
    completed_at        Nullable(DateTime),
    status              LowCardinality(String),
    records_found       Int32,
    records_written     Int32,
    error_message       Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (started_at, postcode_district, target_month);


-- ============================================================
-- ANALYTICAL VIEWS
-- These power your property insights API layer
-- ============================================================

-- Crime totals by postcode district per month
-- Core query for "how safe is BT5 vs BT1?"
CREATE OR REPLACE VIEW property_intel.v_crime_by_postcode_month AS
SELECT
    postcode_district,
    crime_month,
    crime_date,
    category,
    category_name,
    count()                             AS crime_count
FROM property_intel.crimes
GROUP BY postcode_district, crime_month, crime_date, category, category_name
ORDER BY postcode_district, crime_date DESC, crime_count DESC;


-- Rolling 12-month crime score per postcode district
-- Useful for a "headline safety score" on property listings
CREATE OR REPLACE VIEW property_intel.v_crime_score_12m AS
SELECT
    postcode_district,
    count()                                                  AS total_crimes_12m,
    countIf(category = 'anti-social-behaviour')              AS anti_social_behaviour,
    countIf(category = 'burglary')                           AS burglary,
    countIf(category = 'violent-crime')                      AS violent_crime,
    countIf(category = 'robbery')                            AS robbery,
    countIf(category = 'vehicle-crime')                      AS vehicle_crime,
    countIf(category = 'drugs')                              AS drugs,
    countIf(category = 'criminal-damage-arson')              AS criminal_damage_arson,
    countIf(category = 'shoplifting')                        AS shoplifting,
    countIf(category = 'theft-from-the-person')              AS theft_from_person,
    countIf(category = 'other-theft')                        AS other_theft,
    countIf(category = 'public-order')                       AS public_order,
    countIf(category = 'weapons')                            AS weapons,
    countIf(category = 'bicycle-theft')                      AS bicycle_theft,
    countIf(category = 'other-crime')                        AS other_crime,
    round(total_crimes_12m / 12.0, 1)                        AS avg_crimes_per_month
FROM property_intel.crimes
WHERE crime_date >= toDate(now() - INTERVAL 12 MONTH)
GROUP BY postcode_district
ORDER BY total_crimes_12m DESC;


-- Year-on-year trend — is crime going up or down in each area?
CREATE OR REPLACE VIEW property_intel.v_crime_yoy_trend AS
WITH
    current_year AS (
        SELECT postcode_district, count() AS crimes_current
        FROM property_intel.crimes
        WHERE crime_date >= toDate(now() - INTERVAL 12 MONTH)
        GROUP BY postcode_district
    ),
    prior_year AS (
        SELECT postcode_district, count() AS crimes_prior
        FROM property_intel.crimes
        WHERE crime_date >= toDate(now() - INTERVAL 24 MONTH)
          AND crime_date <  toDate(now() - INTERVAL 12 MONTH)
        GROUP BY postcode_district
    )
SELECT
    c.postcode_district,
    c.crimes_current,
    p.crimes_prior,
    round((c.crimes_current - p.crimes_prior) / nullIf(p.crimes_prior, 0) * 100, 1) AS yoy_change_pct,
    CASE
        WHEN yoy_change_pct > 10  THEN 'INCREASING'
        WHEN yoy_change_pct < -10 THEN 'DECREASING'
        ELSE 'STABLE'
    END AS trend
FROM current_year c
LEFT JOIN prior_year p ON c.postcode_district = p.postcode_district
ORDER BY yoy_change_pct DESC;


-- Street-level hotspot ranking (for geo visualisation)
CREATE OR REPLACE VIEW property_intel.v_street_hotspots AS
SELECT
    postcode_district,
    street_name,
    latitude,
    longitude,
    count()     AS total_crimes,
    max(crime_date) AS last_seen
FROM property_intel.crimes
WHERE street_name IS NOT NULL
  AND latitude IS NOT NULL
  AND crime_date >= toDate(now() - INTERVAL 12 MONTH)
GROUP BY postcode_district, street_name, latitude, longitude
ORDER BY total_crimes DESC;
