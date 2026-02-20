package com.propertyintel.crime.config;

import com.propertyintel.crime.service.CrimeQueryService;
import com.propertyintel.crime.service.CrimeScrapeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequiredArgsConstructor
public class ScrapeController {

    private final CrimeScrapeService scrapeService;
    private final CrimeQueryService crimeQueryService;

    // ── Scrape triggers ───────────────────────────────────────────────────────

    @PostMapping("/scrape/trigger/latest")
    public ResponseEntity<Map<String, String>> triggerLatest() {
        new Thread(scrapeService::scrapeLatest, "manual-scrape-latest").start();
        return ResponseEntity.accepted().body(Map.of("status", "accepted", "target", "latest"));
    }

    @PostMapping("/scrape/trigger/{yearMonth}")
    public ResponseEntity<Map<String, String>> triggerMonth(@PathVariable String yearMonth) {
        if (!yearMonth.matches("\\d{4}-\\d{2}")) {
            return ResponseEntity.badRequest().body(Map.of("error", "yearMonth must be YYYY-MM"));
        }
        new Thread(() -> scrapeService.scrapeMonth(yearMonth), "manual-scrape-" + yearMonth).start();
        return ResponseEntity.accepted().body(Map.of("status", "accepted", "target", yearMonth));
    }

    @PostMapping("/scrape/backfill")
    public ResponseEntity<Map<String, String>> backfill(@RequestParam(defaultValue = "24") int months) {
        new Thread(() -> scrapeService.backfill(months), "manual-backfill").start();
        return ResponseEntity.accepted().body(Map.of("status", "accepted", "months", String.valueOf(months)));
    }

    @GetMapping("/scrape/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "service", "property-intel-crime-scraper",
                "version", "1.0.0",
                "region", "Northern Ireland (all NI)",
                "dataSource", "data.police.uk / PSNI bulk CSV"
        ));
    }

    // ── Crime query API ───────────────────────────────────────────────────────

    /**
     * Get crime summary for a postcode.
     *
     * GET /crimes?postcode=BT1+4NX&radius=500&months=12
     *
     * Returns total crimes by category + monthly trend within radius of postcode.
     */
    @GetMapping("/crimes")
    public ResponseEntity<?> getCrimes(
            @RequestParam String postcode,
            @RequestParam(defaultValue = "500") int radius,
            @RequestParam(defaultValue = "12") int months) {
        try {
            Map<String, Object> result = crimeQueryService.getCrimeSummary(postcode, radius, months);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Crime query failed for postcode {}: {}", postcode, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get nearby street-level hotspots for a postcode.
     *
     * GET /crimes/hotspots?postcode=BT1+4NX&radius=500
     */
    @GetMapping("/crimes/hotspots")
    public ResponseEntity<?> getHotspots(
            @RequestParam String postcode,
            @RequestParam(defaultValue = "500") int radius) {
        try {
            List<Map<String, Object>> result = crimeQueryService.getNearbyHotspots(postcode, radius);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Hotspot query failed for postcode {}: {}", postcode, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
