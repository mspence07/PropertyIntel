package com.propertyintel.crime.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Resolves a full UK postcode to lat/lng using the free postcodes.io API.
 * No API key required. Supports NI (BT) postcodes fully.
 *
 * Example: "BT23 4WJ" → {lat: 54.5123, lng: -5.7890}
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PostcodeLookupService {

    private static final String POSTCODES_IO_URL = "https://api.postcodes.io/postcodes/";

    private final ObjectMapper objectMapper;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public record PostcodeResult(String postcode, double latitude, double longitude) {}

    public PostcodeResult lookup(String postcode) {
        String normalised = postcode.trim().toUpperCase().replace(" ", "%20");
        String url = POSTCODES_IO_URL + normalised;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 404) {
                throw new IllegalArgumentException("Postcode not found: " + postcode);
            }
            if (response.statusCode() != 200) {
                throw new RuntimeException("postcodes.io returned HTTP " + response.statusCode());
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode result = root.get("result");

            double lat = result.get("latitude").asDouble();
            double lng = result.get("longitude").asDouble();

            log.debug("Postcode {} → lat={}, lng={}", postcode, lat, lng);
            return new PostcodeResult(postcode.toUpperCase().trim(), lat, lng);

        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Postcode lookup failed for " + postcode + ": " + e.getMessage(), e);
        }
    }
}
