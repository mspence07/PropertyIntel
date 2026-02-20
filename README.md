# Property Intel — Crime Scraper

A microservice that pulls PSNI crime data from data.police.uk and stores it in ClickHouse, queryable by any NI postcode. This is the first module of a broader property intelligence platform — think HPI checks but for houses.

## How it works

Downloads the monthly bulk archive from data.police.uk, extracts the Northern Ireland street-level crime CSV, and writes all records into ClickHouse with lat/lng coordinates. At query time, any full postcode (e.g. `BT1 4NX`) is resolved to coordinates via postcodes.io, and crimes are returned within a configurable radius using ClickHouse's `greatCircleDistance()` function.

```
data.police.uk/archive  →  Crime Scraper (Spring Boot)  →  ClickHouse
                                                               ↑
                                                        postcodes.io
                                                        (postcode → lat/lng)
```

## Stack

- Java 21 / Spring Boot 3.2
- ClickHouse (columnar store, fast geo queries)
- postcodes.io (free postcode geocoding, no API key needed)
- Docker Compose

## Getting started

```bash
docker-compose up -d

# Load the last 24 months of NI crime data (~200k records)
curl -X POST "http://localhost:8080/scrape/backfill?months=24"

# Query crimes near a postcode
curl "http://localhost:8080/crimes?postcode=BT1+1AA&radius=500&months=12"

# Street-level hotspots
curl "http://localhost:8080/crimes/hotspots?postcode=BT1+1AA&radius=500"
```

The backfill downloads one zip from data.police.uk containing all historical months — takes a few minutes depending on your connection. Subsequent monthly runs are scheduled automatically on the 15th of each month.

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/crimes?postcode=BT1+1AA&radius=500&months=12` | Crime summary by category near a postcode |
| `GET` | `/crimes/hotspots?postcode=BT1+1AA&radius=500` | Top 20 street hotspots within radius |
| `POST` | `/scrape/backfill?months=N` | Load N months of history |
| `POST` | `/scrape/trigger/latest` | Pull the latest available month |
| `POST` | `/scrape/trigger/2024-01` | Pull a specific month |
| `GET` | `/scrape/status` | Service info |
| `GET` | `/actuator/health` | Health check |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_URL` | `jdbc:clickhouse://localhost:8123/property_intel` | ClickHouse connection |
| `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | _(empty)_ | ClickHouse password |
| `OUTPUT_MODE` | `CLICKHOUSE` | `CLICKHOUSE`, `CSV`, or `BOTH` |
| `RUN_ON_STARTUP` | `false` | Run backfill on container start |
| `BACKFILL_MONTHS` | `24` | Months to load when `RUN_ON_STARTUP=true` |

## ClickHouse schema

The main `property_intel.crimes` table stores every NI crime record with coordinates. Key columns:

| Column | Notes |
|--------|-------|
| `crime_date` | First day of the month the crime occurred |
| `category` | Slugified crime type e.g. `anti-social-behaviour` |
| `category_name` | Human readable e.g. `Anti-social Behaviour` |
| `latitude` / `longitude` | Anonymised to nearest street node (Home Office policy) |
| `street_name` | e.g. `On or near High Street` |
| `outcome_category` | Outcome where available (often null for PSNI) |

## Data notes

**Source:** data.police.uk publishes PSNI data as part of its monthly bulk archive. The street-level CSV contains all recorded crimes across Northern Ireland with anonymised coordinates.

**Coordinate anonymisation:** Locations are snapped to the nearest pre-defined street node rather than the exact crime location — this is a Home Office anonymisation requirement, not a data quality issue.

**Data lag:** The archive typically runs 2 months behind. The scraper uses the `/crimes-street-dates` endpoint to determine what's actually available before downloading.

**Deduplication:** The table uses `ReplacingMergeTree` ordered on `(postcode_district, crime_date, category, street_name, latitude, longitude)` — re-running a backfill is safe.

## Planned modules

The idea is to build a postcode-level property intelligence platform — a kind of HPI check for houses rather than cars. Crime is the first data layer. Planned additions:

| Module | Source |
|--------|--------|
| Planning applications | Belfast City Council / NI Planning Portal |
| Flood risk | DAERA flood maps |
| HMO / rental licences | NIHE, Belfast City Council |
| Sold prices | Land Registry NI |
| School proximity | Department of Education NI |
| Broadband speeds | Ofcom Connected Nations |

Each will follow the same pattern — Docker microservice writing into ClickHouse, queryable by postcode through a shared API layer.
