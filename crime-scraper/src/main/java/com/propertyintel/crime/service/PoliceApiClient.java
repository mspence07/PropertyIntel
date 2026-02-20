package com.propertyintel.crime.service;

import com.propertyintel.crime.config.CrimeScraperProperties;
import com.propertyintel.crime.model.PoliceApiCrime;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Thin client over the data.police.uk REST API.
 *
 * Rate limiting: The API has no official rate limit documented, but as a public
 * service we apply a configurable delay between calls (default 1 second).
 * A 429 or 503 will trigger the Resilience4j retry with exponential backoff.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PoliceApiClient {

    private final RestTemplate restTemplate;
    private final CrimeScraperProperties properties;

    /**
     * Fetch all crimes within 1 mile of a lat/lng for a given month.
     *
     * @param lat       Latitude of the postcode centre
     * @param lng       Longitude of the postcode centre
     * @param yearMonth e.g. "2024-01"
     * @return List of crimes (may be empty, never null)
     */
    @Retry(name = "policeApi")
    public List<PoliceApiCrime> fetchCrimesNearPoint(double lat, double lng, String yearMonth) {
        String url = UriComponentsBuilder
                .fromHttpUrl(properties.getApi().getBaseUrl() + "/crimes-street/all-crime")
                .queryParam("lat", lat)
                .queryParam("lng", lng)
                .queryParam("date", yearMonth)
                .toUriString();

        return callApi(url);
    }

    /**
     * Fetch crimes within a polygon (for tighter postcode boundaries).
     * The poly param format is lat,lng:lat,lng:lat,lng (max ~10,000 crimes per call).
     *
     * @param polygon   Colon-separated lat,lng pairs e.g. "54.597,-5.930:54.590,-5.920:..."
     * @param yearMonth e.g. "2024-01"
     */
    @Retry(name = "policeApi")
    public List<PoliceApiCrime> fetchCrimesInPolygon(String polygon, String yearMonth) {
        String url = UriComponentsBuilder
                .fromHttpUrl(properties.getApi().getBaseUrl() + "/crimes-street/all-crime")
                .queryParam("poly", polygon)
                .queryParam("date", yearMonth)
                .toUriString();

        return callApi(url);
    }

    /**
     * Get the latest available month from the API.
     * Useful to avoid querying future months.
     */
    public String fetchLastAvailableMonth() {
        String url = properties.getApi().getBaseUrl() + "/crimes-street-dates";
        try {
            // Returns array of {"date":"YYYY-MM","stop-and-search":["force",...]}
            Object[] dates = restTemplate.getForObject(url, Object[].class);
            if (dates != null && dates.length > 0) {
                // Dates are in ascending order, last is most recent
                @SuppressWarnings("unchecked")
                var lastEntry = (java.util.Map<String, Object>) dates[dates.length - 1];
                return (String) lastEntry.get("date");
            }
        } catch (Exception e) {
            log.warn("Could not determine last available month, defaulting to 2 months ago: {}", e.getMessage());
        }
        // Safe fallback: 2 months ago
        return java.time.YearMonth.now().minusMonths(2).toString();
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private List<PoliceApiCrime> callApi(String url) {
        log.debug("Calling Police API: {}", url);
        try {
            applyRateLimit();
            PoliceApiCrime[] response = restTemplate.getForObject(url, PoliceApiCrime[].class);
            if (response == null) {
                return Collections.emptyList();
            }
            log.debug("API returned {} crimes for URL: {}", response.length, url);
            return Arrays.asList(response);

        } catch (HttpClientErrorException.NotFound e) {
            // 404 can mean no data for that month/location — treat as empty
            log.debug("No data found (404) for URL: {}", url);
            return Collections.emptyList();

        } catch (HttpClientErrorException.TooManyRequests e) {
            log.warn("Rate limited (429) by Police API — increasing delay");
            sleepMs(5000); // extra back-off
            throw e; // let Resilience4j retry

        } catch (Exception e) {
            log.error("API call failed for URL {}: {}", url, e.getMessage());
            throw e;
        }
    }

    private void applyRateLimit() {
        sleepMs(properties.getApi().getRateLimitDelayMs());
    }

    private void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
