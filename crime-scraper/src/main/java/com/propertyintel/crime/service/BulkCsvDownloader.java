package com.propertyintel.crime.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Downloads the single latest archive from data.police.uk which contains
 * all historical months in subfolders.
 *
 * Archive URL: https://data.police.uk/data/archive/latest.zip
 *
 * Internal structure:
 *   2023-01/2023-01-northern-ireland-street.csv
 *   2023-02/2023-02-northern-ireland-street.csv
 *   ...
 *   2025-12/2025-12-northern-ireland-street.csv
 *
 * We stream the zip and extract only the northern-ireland-street.csv files,
 * keyed by their month folder, so the rest of the pipeline processes them
 * month by month exactly as before.
 */
@Service
@Slf4j
public class BulkCsvDownloader {

    private static final String LATEST_ARCHIVE_URL = "https://data.police.uk/data/archive/latest.zip";
    private static final String NI_FILE_SUFFIX = "-northern-ireland-street.csv";

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .build();

    /**
     * Download the latest archive and return all NI crime lines grouped by month.
     * Returns a list of MonthData, each containing the month string and its CSV lines
     * (header included once per month).
     */
    public List<MonthData> downloadAllNiMonths() throws IOException, InterruptedException {
        log.info("Downloading latest archive from: {}", LATEST_ARCHIVE_URL);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(LATEST_ARCHIVE_URL))
                .timeout(Duration.ofMinutes(30))  // large file — be generous
                .GET()
                .build();

        HttpResponse<InputStream> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to download archive — HTTP " + response.statusCode());
        }

        log.info("Archive download started, streaming and extracting NI files...");
        return extractNiMonths(response.body());
    }

    /**
     * Stream the zip and pull out every northern-ireland-street.csv, one per month folder.
     */
    private List<MonthData> extractNiMonths(InputStream zipStream) throws IOException {
        List<MonthData> months = new ArrayList<>();

        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(zipStream, 65536))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();

                // Match entries like: 2023-01/2023-01-northern-ireland-street.csv
                if (!entry.isDirectory() && name.endsWith(NI_FILE_SUFFIX)) {

                    // Extract the month from the filename e.g. "2023-01"
                    String filename = name.contains("/")
                            ? name.substring(name.lastIndexOf('/') + 1)
                            : name;
                    String yearMonth = filename.replace(NI_FILE_SUFFIX, "");

                    log.info("Extracting: {} (month: {})", name, yearMonth);

                    List<String> lines = new ArrayList<>();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zis), 65536);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.isBlank()) lines.add(line);
                    }

                    log.info("  → {} lines extracted", lines.size());
                    months.add(new MonthData(yearMonth, lines));
                }

                zis.closeEntry();
            }
        }

        log.info("Extraction complete. {} months of NI data found.", months.size());
        return months;
    }

    /**
     * Fetch the list of available months from the API (used for incremental scrapes).
     */
    public List<String> fetchAvailableMonths() throws IOException, InterruptedException {
        String url = "https://data.police.uk/api/crimes-street-dates";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to fetch available months — HTTP " + response.statusCode());
        }

        List<String> months = new ArrayList<>();
        String body = response.body();
        int idx = 0;
        while ((idx = body.indexOf("\"date\":\"", idx)) != -1) {
            idx += 8;
            int end = body.indexOf("\"", idx);
            if (end != -1) {
                months.add(body.substring(idx, end));
                idx = end;
            }
        }

        log.info("Available months: {} to {}",
                months.isEmpty() ? "none" : months.get(0),
                months.isEmpty() ? "none" : months.get(months.size() - 1));

        return months;
    }

    // ── Inner record ──────────────────────────────────────────────────────────

    public record MonthData(String yearMonth, List<String> lines) {}
}
