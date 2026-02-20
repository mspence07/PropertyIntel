package com.propertyintel.crime.output;

import com.propertyintel.crime.model.CrimeRecord;
import com.propertyintel.crime.model.ScrapeRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class ClickHouseWriter {

    private static final int BATCH_SIZE = 1000;
    private final JdbcTemplate jdbcTemplate;

    public void ensureSchema() {
        log.info("Ensuring ClickHouse schema exists...");

        jdbcTemplate.execute("""
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
            SETTINGS index_granularity = 8192
        """);

        jdbcTemplate.execute("""
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
            ORDER BY (started_at, postcode_district, target_month)
        """);

        log.info("ClickHouse schema ready.");
    }

    public void write(List<CrimeRecord> records) {
        if (records.isEmpty()) return;

        int total = records.size();
        log.info("Writing {} records to ClickHouse in batches of {}", total, BATCH_SIZE);

        for (int i = 0; i < total; i += BATCH_SIZE) {
            List<CrimeRecord> batch = records.subList(i, Math.min(i + BATCH_SIZE, total));
            try {
                writeBatchAsValues(batch);
                log.debug("Wrote batch {}/{}", Math.min(i + BATCH_SIZE, total), total);
            } catch (Exception e) {
                log.error("Batch write failed at offset {}: {}", i, e.getMessage(), e);
                throw e;
            }
        }

        log.info("Successfully wrote {} records", total);
    }

    /**
     * Build a single INSERT ... VALUES statement with all rows in the batch.
     * This is the most reliable approach with the ClickHouse JDBC driver —
     * avoids issues with PreparedStatement batch handling.
     */
    private void writeBatchAsValues(List<CrimeRecord> batch) {
        StringBuilder sql = new StringBuilder("""
            INSERT INTO property_intel.crimes
            (persistent_id, api_id, category, category_name, crime_month, crime_date,
             postcode_district, street_name, street_id, latitude, longitude, location_type,
             outcome_category, outcome_date, scraped_at, source_endpoint)
            VALUES
            """);

        String rows = batch.stream()
                .map(this::toValueRow)
                .collect(Collectors.joining(",\n"));

        sql.append(rows);
        jdbcTemplate.execute(sql.toString());
    }

    private String toValueRow(CrimeRecord r) {
        return String.format("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                sqlStr(r.getPersistentId()),
                sqlLong(r.getApiId()),
                sqlStr(r.getCategory()),
                sqlStr(r.getCategoryName()),
                sqlStr(r.getCrimeMonth()),
                r.getCrimeDate() != null ? sqlStr(r.getCrimeDate().toString()) : "toDate('1970-01-01')",
                sqlStr(r.getPostcodeDistrict()),
                sqlStr(r.getStreetName()),
                sqlLong(r.getStreetId()),
                r.getLatitude() != null ? r.getLatitude() : "NULL",
                r.getLongitude() != null ? r.getLongitude() : "NULL",
                sqlStr(r.getLocationType()),
                sqlStr(r.getOutcomeCategory()),
                sqlStr(r.getOutcomeDate()),
                sqlStr(r.getScrapedAt() != null ? r.getScrapedAt().toString() : null),
                sqlStr(r.getSourceEndpoint())
        );
    }

    private String sqlStr(Object val) {
        if (val == null) return "NULL";
        return "'" + val.toString().replace("'", "\\'") + "'";
    }

    private String sqlLong(Long val) {
        return val == null ? "NULL" : val.toString();
    }

    public void writeScrapeRun(ScrapeRun run) {
        try {
            String sql = String.format("""
                INSERT INTO property_intel.scrape_runs
                (run_id, target_month, postcode_district, started_at, completed_at,
                 status, records_found, records_written, error_message)
                VALUES ('%s','%s','%s','%s',%s,'%s',%d,%d,%s)
                """,
                    run.getRunId(),
                    run.getTargetMonth(),
                    run.getPostcodeDistrict(),
                    run.getStartedAt(),
                    run.getCompletedAt() != null ? "'" + run.getCompletedAt() + "'" : "NULL",
                    run.getStatus(),
                    run.getRecordsFound(),
                    run.getRecordsWritten(),
                    run.getErrorMessage() != null ? "'" + run.getErrorMessage().replace("'", "\\'") + "'" : "NULL"
            );
            jdbcTemplate.execute(sql);
        } catch (Exception e) {
            log.warn("Failed to write scrape run: {}", e.getMessage());
        }
    }
}
