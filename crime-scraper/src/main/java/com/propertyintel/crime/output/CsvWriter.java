package com.propertyintel.crime.output;

import com.opencsv.CSVWriter;
import com.propertyintel.crime.config.CrimeScraperProperties;
import com.propertyintel.crime.model.CrimeRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Writes crime records to CSV files.
 *
 * Output path pattern: {outputDir}/crimes_{postcode}_{month}.csv
 * e.g. /data/output/crimes_BT1_2024-01.csv
 *
 * These CSVs can be loaded into ClickHouse via:
 *   INSERT INTO property_intel.crimes FROM INFILE '/data/output/*.csv' FORMAT CSVWithNames
 *
 * Or piped into any other data warehouse / pipeline.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CsvWriter {

    private final CrimeScraperProperties properties;

    private static final String[] HEADERS = {
            "persistent_id", "api_id",
            "category", "category_name",
            "crime_month", "crime_date",
            "postcode_district", "street_name", "street_id",
            "latitude", "longitude", "location_type",
            "outcome_category", "outcome_date",
            "scraped_at", "source_endpoint"
    };

    public void write(List<CrimeRecord> records, String yearMonth, String postcodeDistrict) {
        if (records.isEmpty()) return;

        Path outputDir = Paths.get(properties.getOutput().getCsv().getOutputDir());
        ensureDirectory(outputDir);

        String filename = String.format("crimes_%s_%s.csv", postcodeDistrict, yearMonth);
        Path outputPath = outputDir.resolve(filename);

        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputPath.toFile()),
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {

            if (properties.getOutput().getCsv().isIncludeHeader()) {
                writer.writeNext(HEADERS);
            }

            for (CrimeRecord r : records) {
                writer.writeNext(toRow(r));
            }

            log.info("Written {} records to CSV: {}", records.size(), outputPath);

        } catch (IOException e) {
            log.error("Failed to write CSV file {}: {}", outputPath, e.getMessage(), e);
            throw new RuntimeException("CSV write failed", e);
        }
    }

    private String[] toRow(CrimeRecord r) {
        return new String[]{
                str(r.getPersistentId()),
                str(r.getApiId()),
                str(r.getCategory()),
                str(r.getCategoryName()),
                str(r.getCrimeMonth()),
                str(r.getCrimeDate()),
                str(r.getPostcodeDistrict()),
                str(r.getStreetName()),
                str(r.getStreetId()),
                str(r.getLatitude()),
                str(r.getLongitude()),
                str(r.getLocationType()),
                str(r.getOutcomeCategory()),
                str(r.getOutcomeDate()),
                str(r.getScrapedAt()),
                str(r.getSourceEndpoint())
        };
    }

    private String str(Object val) {
        return val == null ? "" : val.toString();
    }

    private void ensureDirectory(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create output directory: " + dir, e);
        }
    }
}
