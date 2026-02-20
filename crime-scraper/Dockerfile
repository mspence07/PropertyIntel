# ── Build stage ────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# Cache dependency layer separately from source
COPY pom.xml .
COPY src ./src

# Install Maven (slim Alpine image doesn't include it)
RUN apk add --no-cache maven && \
    mvn dependency:go-offline -q && \
    mvn package -DskipTests -q

# ── Runtime stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy jar from build stage
COPY --from=builder /build/target/crime-scraper-1.0.0.jar app.jar

# Output directory for CSV mode
RUN mkdir -p /data/output && chown appuser:appgroup /data/output

USER appuser

# Expose actuator + API port
EXPOSE 8080

# JVM tuning for containers:
#  - UseContainerSupport: respect Docker memory limits
#  - MaxRAMPercentage: use up to 75% of container RAM for heap
#  - ExitOnOutOfMemoryError: crash fast so Docker can restart cleanly
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", "app.jar"]
