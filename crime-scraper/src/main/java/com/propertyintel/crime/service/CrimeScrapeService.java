package com.propertyintel.crime.service;

import com.propertyintel.crime.model.CrimeRecord;
import com.propertyintel.crime.model.ScrapeRun;
import com.propertyintel.crime.output.OutputRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Orchestrates the scrape cycle using the single latest.zip archive.
 *
 * One download contains all historical months — no need to loop per month.
 * The backfill() and scrapeLatest() methods both trigger a fresh download
 * and process whichever months are requested.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CrimeScrapeService {

    private final BulkCsvDownloader downloader;
    private final PsniCsvParser parser;
    private final OutputRouter outputRouter;

    /**
     * Download the full archive and process all available months.
     * This is the main backfill entry point.
     */
    public void backfill(int months) {
        log.info("Starting full backfill (up to {} months)...", months);
        try {
            List<BulkCsvDownloader.MonthData> allMonths = downloader.downloadAllNiMonths();

            // Take the last N months
            List<BulkCsvDownloader.MonthData> toProcess = allMonths.size() <= months
                    ? allMonths
                    : allMonths.subList(allMonths.size() - months, allMonths.size());

            log.info("Processing {} months of NI data", toProcess.size());

            for (BulkCsvDownloader.MonthData monthData : toProcess) {
                processMonth(monthData);
            }

            log.info("Backfill complete.");

        } catch (Exception e) {
            log.error("Backfill failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Download the full archive and process only the latest month.
     * Used for the monthly scheduled run.
     */
    public void scrapeLatest() {
        log.info("Scraping latest month...");
        try {
            List<BulkCsvDownloader.MonthData> allMonths = downloader.downloadAllNiMonths();
            if (allMonths.isEmpty()) {
                log.warn("No months found in archive");
                return;
            }
            processMonth(allMonths.get(allMonths.size() - 1));
        } catch (Exception e) {
            log.error("scrapeLatest failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Scrape a specific month — downloads full archive and processes just that month.
     */
    public void scrapeMonth(String yearMonth) {
        log.info("Scraping specific month: {}", yearMonth);
        try {
            List<BulkCsvDownloader.MonthData> allMonths = downloader.downloadAllNiMonths();
            allMonths.stream()
                    .filter(m -> m.yearMonth().equals(yearMonth))
                    .findFirst()
                    .ifPresentOrElse(
                            this::processMonth,
                            () -> log.warn("Month {} not found in archive", yearMonth)
                    );
        } catch (Exception e) {
            log.error("scrapeMonth {} failed: {}", yearMonth, e.getMessage(), e);
        }
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private void processMonth(BulkCsvDownloader.MonthData monthData) {
        String yearMonth = monthData.yearMonth();

        ScrapeRun run = ScrapeRun.builder()
                .runId(UUID.randomUUID().toString())
                .targetMonth(yearMonth)
                .postcodeDistrict("ALL_BT")
                .startedAt(LocalDateTime.now())
                .status("RUNNING")
                .build();

        try {
            List<CrimeRecord> records = parser.parse(monthData.lines(), yearMonth);
            log.info("Month {}: {} Belfast records", yearMonth, records.size());

            if (!records.isEmpty()) {
                outputRouter.write(records, yearMonth, "ALL_BT");
            }

            run.setStatus("SUCCESS");
            run.setRecordsFound(records.size());
            run.setRecordsWritten(records.size());

        } catch (Exception e) {
            log.error("Failed processing month {}: {}", yearMonth, e.getMessage(), e);
            run.setStatus("FAILED");
            run.setErrorMessage(e.getMessage());
        } finally {
            run.setCompletedAt(LocalDateTime.now());
            outputRouter.writeScrapeRun(run);
        }
    }
}
