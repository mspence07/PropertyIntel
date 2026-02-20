package com.propertyintel.crime.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "crime-scraper")
@Data
public class CrimeScraperProperties {

    private Output output = new Output();
    private Scheduling scheduling = new Scheduling();

    @Data
    public static class Output {
        private OutputMode mode = OutputMode.CLICKHOUSE;
        private Csv csv = new Csv();

        @Data
        public static class Csv {
            private String outputDir = "/data/output";
            private boolean includeHeader = true;
        }

        public enum OutputMode {
            CLICKHOUSE, CSV, BOTH
        }
    }

    @Data
    public static class Scheduling {
        private String cron = "0 0 2 15 * ?";
        private boolean runOnStartup = false;
        private int backfillMonths = 24;
    }
}
