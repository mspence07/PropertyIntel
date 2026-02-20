package com.propertyintel.crime.output;

import com.propertyintel.crime.config.CrimeScraperProperties;
import com.propertyintel.crime.model.CrimeRecord;
import com.propertyintel.crime.model.ScrapeRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Routes output to the appropriate sink(s) based on configuration.
 * Supports CLICKHOUSE, CSV, or BOTH modes.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OutputRouter {

    private final ClickHouseWriter clickHouseWriter;
    private final CsvWriter csvWriter;
    private final CrimeScraperProperties properties;

    public void write(List<CrimeRecord> records, String yearMonth, String postcodeDistrict) {
        CrimeScraperProperties.Output.OutputMode mode = properties.getOutput().getMode();

        switch (mode) {
            case CLICKHOUSE -> clickHouseWriter.write(records);
            case CSV -> csvWriter.write(records, yearMonth, postcodeDistrict);
            case BOTH -> {
                clickHouseWriter.write(records);
                csvWriter.write(records, yearMonth, postcodeDistrict);
            }
        }
    }

    public void writeScrapeRun(ScrapeRun run) {
        try {
            if (properties.getOutput().getMode() != CrimeScraperProperties.Output.OutputMode.CSV) {
                clickHouseWriter.writeScrapeRun(run);
            }
        } catch (Exception e) {
            log.warn("Failed to write scrape run metadata: {}", e.getMessage());
        }
    }
}
