package com.propertyintel.crime.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Queries crime data from ClickHouse by postcode.
 *
 * Uses postcodes.io to resolve a full postcode to lat/lng, then queries
 * ClickHouse using greatCircleDistance() to find crimes within a radius.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CrimeQueryService {

    private final JdbcTemplate jdbcTemplate;
    private final PostcodeLookupService postcodeLookup;

    /**
     * Get crime summary for a postcode within a given radius.
     *
     * @param postcode  Full postcode e.g. "BT23 4WJ"
     * @param radiusM   Search radius in metres (default 500m ≈ 5 min walk)
     * @param months    How many months of history to include
     */
    public Map<String, Object> getCrimeSummary(String postcode, int radiusM, int months) {
        PostcodeLookupService.PostcodeResult coords = postcodeLookup.lookup(postcode);

        log.info("Crime summary for {} ({}, {}), radius {}m, {} months",
                postcode, coords.latitude(), coords.longitude(), radiusM, months);

        // Total crimes by category within radius
        String categoryQuery = """
            SELECT
                category_name,
                count() AS total,
                countIf(crime_date >= toDate(now() - INTERVAL ? MONTH)) AS recent
            FROM property_intel.crimes
            WHERE
                latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND greatCircleDistance(longitude, latitude, ?, ?) <= ?
                AND crime_date >= toDate(now() - INTERVAL ? MONTH)
            GROUP BY category_name
            ORDER BY total DESC
            """;

        List<Map<String, Object>> byCategory = jdbcTemplate.queryForList(
                categoryQuery, months, coords.longitude(), coords.latitude(), radiusM, months);

        // Monthly trend
        String trendQuery = """
            SELECT
                crime_month,
                count() AS total
            FROM property_intel.crimes
            WHERE
                latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND greatCircleDistance(longitude, latitude, ?, ?) <= ?
                AND crime_date >= toDate(now() - INTERVAL ? MONTH)
            GROUP BY crime_month
            ORDER BY crime_month ASC
            """;

        List<Map<String, Object>> trend = jdbcTemplate.queryForList(
                trendQuery, coords.longitude(), coords.latitude(), radiusM, months);

        // Total count
        long totalCrimes = byCategory.stream()
                .mapToLong(r -> ((Number) r.get("total")).longValue())
                .sum();

        return Map.of(
                "postcode", coords.postcode(),
                "latitude", coords.latitude(),
                "longitude", coords.longitude(),
                "radiusMetres", radiusM,
                "monthsAnalysed", months,
                "totalCrimes", totalCrimes,
                "byCategory", byCategory,
                "monthlyTrend", trend
        );
    }

    /**
     * Get nearby crime hotspots — useful for a map view.
     */
    public List<Map<String, Object>> getNearbyHotspots(String postcode, int radiusM) {
        PostcodeLookupService.PostcodeResult coords = postcodeLookup.lookup(postcode);

        String sql = """
            SELECT
                street_name,
                latitude,
                longitude,
                count() AS total_crimes,
                groupArray(category_name) AS crime_types
            FROM property_intel.crimes
            WHERE
                latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND street_name IS NOT NULL
                AND greatCircleDistance(longitude, latitude, ?, ?) <= ?
                AND crime_date >= toDate(now() - INTERVAL 12 MONTH)
            GROUP BY street_name, latitude, longitude
            ORDER BY total_crimes DESC
            LIMIT 20
            """;

        return jdbcTemplate.queryForList(sql,
                coords.longitude(), coords.latitude(), radiusM);
    }
}
