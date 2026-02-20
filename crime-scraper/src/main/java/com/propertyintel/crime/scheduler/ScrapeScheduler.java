package com.propertyintel.crime.scheduler;

import com.propertyintel.crime.config.CrimeScraperProperties;
import com.propertyintel.crime.output.ClickHouseWriter;
import com.propertyintel.crime.service.CrimeScrapeService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Manages scheduled and on-startup scraping.
 *
 * Default schedule: 15th of each month at 02:00 UTC.
 * The Police API typically lags ~2 months behind, so running monthly is appropriate.
 *
 * Override with CRON env var or crime-scraper.scheduling.cron property.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ScrapeScheduler {

    private final CrimeScrapeService scrapeService;
    private final ClickHouseWriter clickHouseWriter;
    private final CrimeScraperProperties properties;

    /**
     * On application startup:
     *  1. Always ensure the database schema exists
     *  2. Optionally run a backfill if RUN_ON_STARTUP=true
     */
    @PostConstruct
    public void onStartup() {
        try {
            clickHouseWriter.ensureSchema();
        } catch (Exception e) {
            log.warn("Could not initialise ClickHouse schema (running in CSV-only mode?): {}", e.getMessage());
        }

        if (properties.getScheduling().isRunOnStartup()) {
            int months = properties.getScheduling().getBackfillMonths();
            log.info("RUN_ON_STARTUP=true — running backfill for {} months", months);
            try {
                scrapeService.backfill(months);
            } catch (Exception e) {
                log.error("Startup backfill failed: {}", e.getMessage(), e);
            }
        } else {
            log.info("Scraper ready. Next scheduled run: {}", properties.getScheduling().getCron());
        }
    }

    /**
     * Scheduled incremental scrape.
     * Default: 15th of each month at 02:00 UTC.
     */
    @Scheduled(cron = "${crime-scraper.scheduling.cron:0 0 2 15 * ?}", zone = "UTC")
    public void scheduledScrape() {
        log.info("Scheduled scrape triggered");
        try {
            scrapeService.scrapeLatest();
        } catch (Exception e) {
            log.error("Scheduled scrape failed: {}", e.getMessage(), e);
        }
    }
}
