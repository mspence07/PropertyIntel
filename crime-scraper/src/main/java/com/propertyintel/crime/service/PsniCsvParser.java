package com.propertyintel.crime.service;

import com.propertyintel.crime.model.CrimeRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses raw PSNI bulk CSV lines into CrimeRecord domain objects.
 *
 * Stores ALL NI crimes with raw lat/lng — no geographic filtering at ingest time.
 * Postcode-based filtering happens at query time using greatCircleDistance() in ClickHouse.
 *
 * PSNI CSV columns (no Crime ID column):
 *   Month, Reported by, Falls within, Longitude, Latitude,
 *   Location, LSOA code, LSOA name, Crime type, Last outcome category, Context
 */
@Component
@Slf4j
public class PsniCsvParser {

    private static final int COL_MONTH      = 1;
    private static final int COL_LONGITUDE  = 4;
    private static final int COL_LATITUDE   = 5;
    private static final int COL_LOCATION   = 6;
    private static final int COL_CRIME_TYPE = 9;
    private static final int COL_OUTCOME    = 10;

    public List<CrimeRecord> parse(List<String> lines, String yearMonth) {
        if (lines.isEmpty()) return List.of();

        List<CrimeRecord> records = new ArrayList<>();
        int malformed = 0;

        // Skip header (index 0)
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) continue;

            String[] cols = parseCsvLine(line);
            if (cols.length < 9) {
                malformed++;
                continue;
            }

            Double lat = parseDouble(safeGet(cols, COL_LATITUDE));
            Double lng = parseDouble(safeGet(cols, COL_LONGITUDE));

            // Skip records with no coordinates
            if (lat == null || lng == null) {
                malformed++;
                continue;
            }

            String crimeMonth = safeGet(cols, COL_MONTH);
            if (crimeMonth.isBlank()) crimeMonth = yearMonth;

            String crimeType = safeGet(cols, COL_CRIME_TYPE);

            records.add(CrimeRecord.builder()
                    .persistentId(null)
                    .apiId(null)
                    .category(slugify(crimeType))
                    .categoryName(crimeType)
                    .crimeMonth(crimeMonth)
                    .crimeDate(parseCrimeDate(crimeMonth))
                    .postcodeDistrict(deriveDistrict(lat, lng))  // e.g. "BT" — broad region only
                    .streetName(emptyToNull(safeGet(cols, COL_LOCATION)))
                    .streetId(null)
                    .latitude(lat)
                    .longitude(lng)
                    .locationType("Force")
                    .outcomeCategory(emptyToNull(safeGet(cols, COL_OUTCOME)))
                    .outcomeDate(null)
                    .scrapedAt(LocalDateTime.now())
                    .sourceEndpoint("bulk-csv-archive/" + yearMonth)
                    .build());
        }

        log.info("Parsed {}: {} records, {} malformed/no-coords skipped",
                yearMonth, records.size(), malformed);

        return records;
    }

    /**
     * Derive a broad postcode district label for partitioning/display.
     * All NI postcodes start with BT — we store "NI" as a catchall.
     * Exact postcode lookup happens at query time via postcodes.io.
     */
    private String deriveDistrict(Double lat, Double lng) {
        return "NI";
    }

    // ── CSV parsing ───────────────────────────────────────────────────────────

    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString().trim());
        return fields.toArray(new String[0]);
    }

    private String safeGet(String[] cols, int idx) {
        if (idx >= cols.length) return "";
        return cols[idx] == null ? "" : cols[idx].replace("\"", "").trim();
    }

    private Double parseDouble(String val) {
        if (val == null || val.isBlank()) return null;
        try { return Double.parseDouble(val); } catch (NumberFormatException e) { return null; }
    }

    private LocalDate parseCrimeDate(String yearMonth) {
        try { return YearMonth.parse(yearMonth).atDay(1); }
        catch (DateTimeParseException e) { return null; }
    }

    private String emptyToNull(String val) {
        return (val == null || val.isBlank()) ? null : val;
    }

    private String slugify(String name) {
        if (name == null) return "other-crime";
        return name.toLowerCase().replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
    }
}
