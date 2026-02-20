package com.propertyintel.crime.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Normalised crime record ready for database ingestion.
 *
 * Schema design notes:
 *  - postcode_district is our primary query dimension (BT1, BT2, etc.)
 *  - lat/lng stored for future geo queries (H3 index in ClickHouse)
 *  - outcome_category is nullable — PSNI outcomes are not available via API
 *  - scraped_at lets us track freshness and support idempotent re-runs
 */
@Data
@Builder
public class CrimeRecord {

    // ── Source identifiers ──────────────────────────────────────────────────
    /** Stable 64-char hash from police API — use for deduplication */
    private String persistentId;

    /** Numeric ID from the API — may change between releases, don't rely on it */
    private Long apiId;

    // ── Classification ──────────────────────────────────────────────────────
    /**
     * Crime category slug as returned by the API.
     * e.g. anti-social-behaviour, burglary, violent-crime, drugs, etc.
     */
    private String category;

    /** Human-readable category label (resolved from /crime-categories) */
    private String categoryName;

    // ── Time ────────────────────────────────────────────────────────────────
    /** Year-month the crime occurred (API returns YYYY-MM) */
    private String crimeMonth;

    /** First day of crimeMonth as a proper date — useful for range queries */
    private LocalDate crimeDate;

    // ── Location ────────────────────────────────────────────────────────────
    /** BT postcode district this record was scraped for, e.g. "BT1" */
    private String postcodeDistrict;

    /** Street name from the API (anonymised to nearest street) */
    private String streetName;

    /** Street ID from the API */
    private Long streetId;

    /** Anonymised latitude (snapped to nearest street node) */
    private Double latitude;

    /** Anonymised longitude (snapped to nearest street node) */
    private Double longitude;

    /** Location type: Force or BTP */
    private String locationType;

    // ── Outcome ─────────────────────────────────────────────────────────────
    /**
     * Latest outcome category.
     * NULL for PSNI data — the Police API does not expose PSNI outcomes.
     */
    private String outcomeCategory;

    /** Date of latest outcome if available */
    private String outcomeDate;

    // ── Metadata ────────────────────────────────────────────────────────────
    /** Timestamp this record was fetched and written */
    private LocalDateTime scrapedAt;

    /** Which API endpoint was called to retrieve this record */
    private String sourceEndpoint;
}
