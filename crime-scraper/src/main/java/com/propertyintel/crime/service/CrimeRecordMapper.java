package com.propertyintel.crime.service;

import com.propertyintel.crime.model.CrimeRecord;
import com.propertyintel.crime.model.PoliceApiCrime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;

/**
 * Maps raw Police API responses to the normalised CrimeRecord domain model.
 */
@Component
@Slf4j
public class CrimeRecordMapper {

    /**
     * Convert a raw API crime DTO to a database-ready CrimeRecord.
     *
     * @param raw              Raw DTO from the Police API
     * @param postcodeDistrict The BT postcode district this was scraped for
     * @param sourceEndpoint   The API URL used (for lineage tracking)
     */
    public CrimeRecord map(PoliceApiCrime raw, String postcodeDistrict, String sourceEndpoint) {
        LocalDate crimeDate = parseCrimeDate(raw.getMonth());

        Double lat = parseDouble(raw.getLocation() != null ? raw.getLocation().getLatitude() : null);
        Double lng = parseDouble(raw.getLocation() != null ? raw.getLocation().getLongitude() : null);

        String streetName = null;
        Long streetId = null;
        if (raw.getLocation() != null && raw.getLocation().getStreet() != null) {
            streetName = raw.getLocation().getStreet().getName();
            streetId = raw.getLocation().getStreet().getId();
        }

        String outcomeCategory = null;
        String outcomeDate = null;
        if (raw.getOutcomeStatus() != null) {
            outcomeCategory = raw.getOutcomeStatus().getCategory();
            outcomeDate = raw.getOutcomeStatus().getDate();
        }

        return CrimeRecord.builder()
                .persistentId(emptyToNull(raw.getPersistentId()))
                .apiId(raw.getId())
                .category(raw.getCategory())
                .categoryName(humanise(raw.getCategory()))
                .crimeMonth(raw.getMonth())
                .crimeDate(crimeDate)
                .postcodeDistrict(postcodeDistrict)
                .streetName(streetName)
                .streetId(streetId)
                .latitude(lat)
                .longitude(lng)
                .locationType(raw.getLocationType())
                .outcomeCategory(outcomeCategory)
                .outcomeDate(outcomeDate)
                .scrapedAt(LocalDateTime.now())
                .sourceEndpoint(sourceEndpoint)
                .build();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private LocalDate parseCrimeDate(String yearMonth) {
        if (yearMonth == null) return null;
        try {
            return YearMonth.parse(yearMonth).atDay(1);
        } catch (DateTimeParseException e) {
            log.warn("Could not parse crime month: {}", yearMonth);
            return null;
        }
    }

    private Double parseDouble(String val) {
        if (val == null || val.isBlank()) return null;
        try {
            return Double.parseDouble(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String emptyToNull(String val) {
        return (val == null || val.isBlank()) ? null : val;
    }

    /**
     * Convert API slug to a human-readable label.
     * e.g. "anti-social-behaviour" → "Anti-social Behaviour"
     */
    private String humanise(String slug) {
        if (slug == null) return null;
        return java.util.Arrays.stream(slug.split("-"))
                .map(word -> Character.toUpperCase(word.charAt(0)) + word.substring(1))
                .collect(java.util.stream.Collectors.joining(" "));
    }
}
