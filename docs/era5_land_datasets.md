# ERA5-Land datasets

ERA5-Land is a global reanalysis of land surface variables produced by ECMWF. It provides a consistent record from 1950 to the near-present at ~9 km resolution. The Open Geo Service ships six ERA5-Land data sources covering temperature and precipitation at daily and monthly resolution.

## Setup

ERA5-Land data is fetched from two sources depending on the dataset. Both are free but require registration.

### Earth Data Hub (monthly datasets and daily normals)

1. Create a free account at [earthdatahub.destine.eu](https://earthdatahub.destine.eu).
2. Go to **Account settings → My API keys → Standard API keys** and generate a key.
3. Set it in `.env`:

```
EDH_API_KEY=<your-standard-api-key>
```

### Copernicus Climate Data Store (daily datasets)

1. Register at [cds.climate.copernicus.eu](https://cds.climate.copernicus.eu) and get your API key from your profile.
2. Set it in `.env`:

```
CDS_API_KEY=<your-cds-api-key>
```

**Set up before this release?** Open Geo Service no longer reads `~/.netrc` or `~/.ecmwfdatastoresrc`; move both keys into `.env`. The daily datasets read from CDS, so they need the CDS key even where they once needed only Earth Data Hub.

### Ingest your first ERA5-Land dataset

Once authenticated, ingest ERA5-Land daily temperature:

```bash
curl -s -X POST http://127.0.0.1:9000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "era5land_temperature_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "publish": true
  }' | jq
```

---

## Recent days are preliminary

Both sources serve the most recent days as ERA5-Land-T, computed from ECMWF's preliminary ERA5T, and replace them with final values about two months after the month in question. A month built from such days is preliminary too. Every dataset on this page therefore declares `sync.refresh_after: P4M`: a sync fetches a period again once, four months after it ends, which leaves margin over the source's usual delay. See [Preliminary periods](architecture.md#preliminary-periods). A managed dataset lists the periods still preliminary in `provisional_periods`.

The monthly anomalies (`era5land_*_monthly_anomaly_1991_2020`) follow their monthly dataset, so a sync that replaces preliminary months recomputes them. The daily anomalies are computed once and keep the values they were computed from; run them again to pick up final values.

A store ingested before this existed records no fetch times, so its preliminary days are taken as final. Ingest it again to have them replaced.

---

## Temperature datasets

### Daily (from hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_temperature_daily` |
| **Variable** | `t2m` |
| **Units** | °C |
| **Period** | Daily |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [CDS ERA5-Land analysis-ready data](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=analysis_ready_data) |
| **Lag** | ~5 days |

Daily mean temperature, averaged from the 24 hourly values of the **UTC calendar day** (00:00–23:59 UTC).

`era5land_temperature_daily_min` and `era5land_temperature_daily_max` are the same, with the daily minimum and maximum of the hourly values instead of the mean.

---

### Monthly

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_temperature_monthly` |
| **Variable** | `t2m` |
| **Units** | °C |
| **Period** | Monthly |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [Earth Data Hub ERA5-Land daily](https://earthdatahub.destine.eu/collections/era5/datasets/era5-land-daily) |
| **Lag** | Available after each month is complete in the source |

Monthly mean 2 m temperature, averaged from the Earth Data Hub daily means of the calendar month. Only complete months are ingested.

---

## Precipitation datasets

Precipitation values represent **total precipitation reaching the surface**, including rain and snow as liquid-equivalent water. Units are **mm**, a total: the daily value is the day's total, and the monthly value the month's. xclim's drought indices take a rate instead: see [Precipitation totals and rates](climate_indices.md#precipitation-totals-and-rates).

### Daily (from hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_precipitation_daily` |
| **Variable** | `tp` |
| **Units** | mm |
| **Period** | Daily |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [CDS ERA5-Land analysis-ready data](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=analysis_ready_data) |
| **Lag** | ~6 days |

Total precipitation for the **UTC calendar day**, summed from the 24 hourly values of that day.

Each hourly precipitation value is the total for the hour *ending* at its timestamp, so the value stamped 00:00 belongs to the previous day. A day therefore sums the values stamped 01:00 through 00:00 the next day. The CDS publishes whole days of timestamps (00:00–23:00), so the hour that closes the latest day only arrives with the next day's update: precipitation always ends one day earlier than temperature, and that day is appended on the next sync.

---

### Monthly

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_precipitation_monthly` |
| **Variable** | `tp` |
| **Units** | mm |
| **Period** | Monthly |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [Earth Data Hub ERA5-Land daily](https://earthdatahub.destine.eu/collections/era5/datasets/era5-land-daily) |
| **Lag** | Available after each month is complete in the source |

Total precipitation for the calendar month, summed from the Earth Data Hub daily totals. Only complete months are ingested.

---

## Humidity datasets

### Daily dewpoint (from hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_dewpoint_daily` |
| **Variable** | `d2m` |
| **Units** | °C |
| **Period** | Daily |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [CDS ERA5-Land analysis-ready data](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=analysis_ready_data) |
| **Lag** | ~5 days |

Daily mean 2 m dewpoint temperature, averaged from the 24 hourly values of the **UTC calendar day**. Dewpoint is the temperature at which water vapor will start condensing as dew.

Together with `era5land_temperature_daily`, it can be used as input to the [`relative_humidity_from_daily_dewpoint`](workflows.md#relative-humidity-from-daily-dewpoint) workflow, which publishes an estimated daily relative humidity where only daily inputs exist.

---

### Daily relative humidity (from hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_relative_humidity_daily` |
| **Variable** | `rh` |
| **Units** | % |
| **Period** | Daily |
| **Coverage** | Global, 1950–present |
| **Resolution** | ~9 km |
| **Source** | [CDS ERA5-Land analysis-ready data](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=analysis_ready_data) |
| **Lag** | ~5 days |

Daily mean 2 m relative humidity for the **UTC calendar day**. It is computed for each hour from the hourly temperature and dewpoint, then averaged. Computing it from daily means instead underestimates it, most where days are hot and nights cool, because saturation vapour pressure rises exponentially with temperature. Ingesting it reads both hourly variables.

`era5land_relative_humidity_daily_min` and `era5land_relative_humidity_daily_max` are the same, with the daily minimum and maximum of the hourly values instead of the mean. The `relative_humidity_from_daily_dewpoint` workflow cannot produce these, as it estimates relative humidity from daily means only.

---

## Choosing the right dataset

| Use case | Recommended dataset |
| --- | --- |
| Daily health modelling | `era5land_temperature_daily` / `era5land_precipitation_daily` |
| Monthly climate–health modelling | `era5land_temperature_monthly` / `era5land_precipitation_monthly` |
| CHAP integration (South Sudan, East Africa) | `era5land_precipitation_monthly` |
