package com.propertyintel.crime.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Tracks each scrape run for observability and idempotency.
 * Stored in the scrape_runs table in ClickHouse.
 */
@Data
@Builder
public class ScrapeRun {

    private String runId;           // UUID
    private String targetMonth;     // YYYY-MM
    private String postcodeDistrict;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private String status;          // RUNNING | SUCCESS | FAILED | SKIPPED
    private int recordsFound;
    private int recordsWritten;
    private String errorMessage;    // null on success
}
