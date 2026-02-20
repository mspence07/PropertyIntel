package com.propertyintel.crime.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Raw DTO matching the data.police.uk JSON structure.
 * Kept separate from the domain model to isolate API coupling.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PoliceApiCrime {

    private String category;

    @JsonProperty("persistent_id")
    private String persistentId;

    private Long id;

    private String month;

    private String context;

    @JsonProperty("location_type")
    private String locationType;

    @JsonProperty("location_subtype")
    private String locationSubtype;

    private Location location;

    @JsonProperty("outcome_status")
    private OutcomeStatus outcomeStatus;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Location {
        private String latitude;
        private String longitude;
        private Street street;

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Street {
            private Long id;
            private String name;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutcomeStatus {
        private String category;
        private String date;
    }
}
