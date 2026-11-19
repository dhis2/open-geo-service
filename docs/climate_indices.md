# Climate Indices

The Open Geo Service exposes the [xclim](https://xclim.readthedocs.io) climate indicators — 183 peer-reviewed, CF-compliant indices — as native openEO processes. Each appears automatically in `GET /processes` and is callable by name from any openEO process graph or client, with no additional configuration.

Indicators that return more than one output (such as `cffwis` and `rain_season`) are left out, since a process returns one datacube. So are xclim's generic indicators, the anuclim, cf and icclim collections, which repeat the core indicators under other names, and most of its `convert` realm, whose humidity and vapour-pressure converters repeat the [earthkit-meteo processes](#derived-meteorological-variables-earthkit-meteo).

---

## What is available

The indicators come from three xclim realms, plus the water-budget converters SPEI needs:

| Module | Domain | Examples |
|---|---|---|
| `atmos` | Atmospheric | SPI, SPEI, CDD, CWD, TX days above, heat wave frequency, frost days, growing degree days, … |
| `land` | Land surface | Snow water equivalent, soil freeze/thaw, … |
| `seaIce` | Sea ice | Ice extent, concentration trends |
| `convert` | Conversions | `potential_evapotranspiration`, `water_budget`, `water_budget_from_tas` |

Browse the full list from any connected client:

```python
import openeo

conn = openeo.connect("http://your-instance:9000")
processes = conn.list_processes()
print([p.id for p in processes])
```

Or call `GET /processes` directly and search by keyword. In the [openEO Web Editor](https://editor.openeo.org), the search box filters by id and summary.

---

## Precipitation totals and rates

Open Geo Service stores precipitation as **totals**: `mm` per day for daily datasets and `mm` per month for monthly ones. Two things need a **rate** instead:

- **xclim's precipitation indicators** (SPI, SPEI, CDD, CWD and the rest), which refuse a total with *"Data units mm are not compatible with requested [precipitation]"*.
- **Comparisons across periods of different length.** January's total covers 31 days and February's 28 or 29, so a February total looks drier than the same daily rainfall in January. As a rate in mm/d, months compare directly with each other and with daily data.

Convert in the process graph with `amount2rate`, between `load_collection` and whatever needs the rate. In the openEO Python client:

```python
precip = conn.load_collection("chirps3_precipitation_monthly", temporal_extent=["2020-01-01", "2023-12-31"])
rate = precip.process("amount2rate", arguments={"data": precip})  # mm/d
```

As a process graph:

```json
{
  "process_graph": {
    "load": {
      "process_id": "load_collection",
      "arguments": {
        "id": "chirps3_precipitation_monthly",
        "temporal_extent": ["2020-01-01", "2023-12-31"]
      }
    },
    "rate": {
      "process_id": "amount2rate",
      "arguments": { "data": { "from_node": "load" } },
      "result": true
    }
  }
}
```

`amount2rate` divides each step by its own length, so a 29-day February is handled correctly, and `out_units` selects another unit (`mm/d` by default). `rate2amount` goes the other way, from a rate to the total over each step (`mm` by default), for example to turn a computed rate back into monthly totals. For daily data the numbers do not change: a day's total in mm is its rate in mm/d.

### Why totals

Totals are what users expect, what reporting products and the Climate App use, and how [WMO-No. 1203](https://library.wmo.int/idurl/4/55797) treats precipitation: a sum parameter, where the monthly value is the month's total, the monthly normal is the mean of the monthly totals, and the annual normal is the sum of the monthly normals. One stored copy serves both needs, since rates come from `amount2rate`.

### Limitations

- **At least 3 regular time steps.** The length of a step is inferred from the time axis, which needs at least 3 steps with none missing. A shorter or gappy cube is refused; widen `temporal_extent`.
- **Climatologies are not converted.** A normal has a `dayofyear` or `month` axis rather than dates, so it is refused. A rate normal is about the monthly normal total divided by the days in the month, with February counted as 28.25 days as in WMO-No. 1203.
- **SPI over several months** (`window` > 1) averages the monthly rates rather than summing the totals, so each month counts equally whatever its length. The result differs slightly from an SPI built on accumulated totals.

---

## Standardized Precipitation Index (SPI)

SPI is the primary drought monitoring index. The `window` parameter selects the accumulation timescale:

| Process call | Timescale |
|---|---|
| `spi(pr, window=1)` | SPI-1 — short-term moisture anomaly |
| `spi(pr, window=3)` | SPI-3 — seasonal drought |
| `spi(pr, window=6)` | SPI-6 — medium-term drought |
| `spi(pr, window=12)` | SPI-12 — long-term / groundwater recharge |

### Python client

```python
import openeo

conn = openeo.connect("http://your-instance:9000")

precip = conn.load_collection(
    "chirps3_precipitation_daily",
    temporal_extent=["2000-01-01", "2023-12-31"],
)
rate = precip.process("amount2rate", arguments={"data": precip})

spi3 = rate.process(
    "spi",
    arguments={
        "pr": rate,
        "window": 3,
        "freq": "MS",
        "cal_start": "2000-01-01",
        "cal_end": "2020-12-31",
    },
)

job = spi3.save_result("Zarr").create_job()
job.start_and_wait()
```

### Process graph (JSON)

```json
{
  "process_graph": {
    "load": {
      "process_id": "load_collection",
      "arguments": {
        "id": "chirps3_precipitation_daily",
        "temporal_extent": ["2000-01-01", "2023-12-31"]
      }
    },
    "rate": {
      "process_id": "amount2rate",
      "arguments": { "data": { "from_node": "load" } }
    },
    "spi": {
      "process_id": "spi",
      "arguments": {
        "pr": { "from_node": "rate" },
        "window": 3,
        "freq": "MS",
        "cal_start": "2000-01-01",
        "cal_end": "2020-12-31"
      },
      "result": true
    }
  }
}
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pr` | datacube | — | Precipitation rate, such as mm/d; convert stored totals with `amount2rate` |
| `window` | integer | `1` | Accumulation window in months (1=SPI-1, 3=SPI-3, 6=SPI-6) |
| `cal_start` | string | start of record | Calibration period start (YYYY-MM-DD) |
| `cal_end` | string | end of record | Calibration period end (YYYY-MM-DD) |
| `freq` | string | `"MS"` | Output frequency — `"MS"` = monthly, `None` = pre-aggregated input |

SPI values are standardised: `0` = climatological median, `±1` = moderate anomaly, `±2` = extreme anomaly.

---

## Standardized Precipitation Evapotranspiration Index (SPEI)

SPEI is SPI applied to the water budget, precipitation minus potential evapotranspiration, so it also captures drought driven by heat. Build the budget from a precipitation rate and an evapotranspiration estimate, here Hargreaves (1985) from daily minimum and maximum temperature:

```python
extent = ["2000-01-01", "2023-12-31"]
precip = conn.load_collection("era5land_precipitation_daily", temporal_extent=extent)
tasmin = conn.load_collection("era5land_temperature_daily_min", temporal_extent=extent)
tasmax = conn.load_collection("era5land_temperature_daily_max", temporal_extent=extent)

rate = precip.process("amount2rate", arguments={"data": precip})
pet = tasmin.process(
    "potential_evapotranspiration",
    arguments={"tasmin": tasmin, "tasmax": tasmax, "method": "hargreaves85"},
)
wb = rate.process("water_budget", arguments={"pr": rate, "evspsblpot": pet})
spei3 = wb.process("spei", arguments={"wb": wb, "freq": "MS", "window": 3})
```

As a process graph:

```json
{
  "process_graph": {
    "precip": {
      "process_id": "load_collection",
      "arguments": { "id": "era5land_precipitation_daily", "temporal_extent": ["2000-01-01", "2023-12-31"] }
    },
    "tasmin": {
      "process_id": "load_collection",
      "arguments": { "id": "era5land_temperature_daily_min", "temporal_extent": ["2000-01-01", "2023-12-31"] }
    },
    "tasmax": {
      "process_id": "load_collection",
      "arguments": { "id": "era5land_temperature_daily_max", "temporal_extent": ["2000-01-01", "2023-12-31"] }
    },
    "rate": {
      "process_id": "amount2rate",
      "arguments": { "data": { "from_node": "precip" } }
    },
    "pet": {
      "process_id": "potential_evapotranspiration",
      "arguments": {
        "tasmin": { "from_node": "tasmin" },
        "tasmax": { "from_node": "tasmax" },
        "method": "hargreaves85"
      }
    },
    "wb": {
      "process_id": "water_budget",
      "arguments": { "pr": { "from_node": "rate" }, "evspsblpot": { "from_node": "pet" } }
    },
    "spei": {
      "process_id": "spei",
      "arguments": { "wb": { "from_node": "wb" }, "freq": "MS", "window": 3 },
      "result": true
    }
  }
}
```

The cubes must share a grid, which the three ERA5-Land datasets do; to combine sources such as CHIRPS3 precipitation with ERA5-Land temperature, resample one onto the other first (`resample_cube_spatial`). The evapotranspiration methods need the latitude of each cell, which is read from the cube's `y` coordinate when its CRS is geographic, as it is for every built-in dataset.

`water_budget_from_tas` computes the budget in one step from precipitation and temperature, choosing the evapotranspiration method with its `method` argument.

---

## Other common indices

### Consecutive dry / wet days

These take a precipitation rate too:

```python
rate = precip.process("amount2rate", arguments={"data": precip})

# Annual maximum consecutive dry days (CDD)
rate.process("cdd", arguments={"pr": rate, "thresh": "1 mm/day", "freq": "YS"})

# Monthly maximum consecutive wet days (CWD)
rate.process("cwd", arguments={"pr": rate, "thresh": "1 mm/day", "freq": "MS"})
```

### Days above temperature threshold

```python
# TX days above 35°C per year
tmax.process("tx_days_above", arguments={"tasmax": tmax, "thresh": "35 degC", "freq": "YS"})
```

### Heat wave frequency

```python
# Annual count of heat waves (≥3 consecutive days with Tmax>35°C and Tmin>20°C)
tmax.process(
    "heat_wave_frequency",
    arguments={
        "tasmin": tmin,
        "tasmax": tmax,
        "thresh_tasmin": "20 degC",
        "thresh_tasmax": "35 degC",
        "freq": "YS",
    },
)
```

---

## Derived meteorological variables (earthkit-meteo)

Alongside the xclim indicators, the thermodynamic functions of ECMWF's [earthkit-meteo](https://earthkit.ecmwf.int) are auto-registered as processes. These are not indices but *derivations* — quantities computed from other quantities, useful as inputs to an index or a health model:

| Process | Computes |
|---|---|
| `relative_humidity_from_dewpoint` | Relative humidity (%) from temperature and dewpoint |
| `dewpoint_from_relative_humidity` | Dewpoint from temperature and relative humidity |
| `specific_humidity_from_dewpoint` | Specific humidity from dewpoint and pressure |
| `wet_bulb_temperature_from_dewpoint` | Wet-bulb temperature — a heat-stress input |
| `saturation_vapour_pressure` | Saturation vapour pressure over water/ice/mixed phase |
| `potential_temperature`, `virtual_temperature` | Thermodynamic temperatures |

Relative humidity is the common case, since the ERA5-Land source carries temperature and dewpoint but not humidity directly:

```python
t2m = conn.load_collection("era5land_temperature_daily")
d2m = conn.load_collection("era5land_dewpoint_daily")

rh = t2m.process("relative_humidity_from_dewpoint", arguments={"t": t2m, "td": d2m})
```

Two things worth noting. Both cubes are passed **explicitly** — the collection you call
`.process()` on is only the entry point, not an implicit first argument. And
`era5land_dewpoint_daily` is **not a built-in**: the shipped ERA5-Land templates cover
temperature (`t2m`) and precipitation (`tp`) only. Dewpoint needs a template of its own,
which is a copy of the temperature one with `variable: d2m` — the same
one-plugin-many-templates pattern described in
[Architecture](architecture.md#streaming-plugin).

Browse the full set in `GET /processes`; every function earthkit-meteo documents with a single cube output is registered.

### Units are converted for you

These functions expect ECMWF's native units — temperature in **K**, pressure in **Pa** — and do no checking of their own. Our stores are usually not in those units: ERA5-Land temperature is converted to `degC` at ingest. Passing `degC` where `K` is expected produces no error, just a wrong number.

The registered processes therefore enforce units on the way in. A cube in a compatible unit is converted (`degC` → `K`, `hPa` → `Pa`); a cube whose `units` attribute is missing or incompatible is rejected with an explanatory error.

So the call above needs no change on a `degC` store: `era5land_temperature_daily` and `era5land_dewpoint_daily` are both converted to `K` on the way in, and the answer is correct without a manual conversion step.

If a cube is rejected for missing units, the fix is to declare `units` on the variable in its dataset template — that value is what gets CF-stamped at ingest.

---

## Customising an index

The auto-registered xclim processes can be overridden per-instance by placing a `@process`-decorated function with the same id in `plugins_dir/processes/`. This is useful for adjusting default parameters or adding domain-specific documentation without modifying shared code.

```python
# plugins/processes/climate_indices.py
import xarray as xr
import xclim.compute
from open_geo_service.openeo.xclim_processes import call_with_ogs_time_dim
from open_geo_service.process import process


@process(summary="Consecutive dry days (Rwanda threshold)")
def cdd(pr: xr.DataArray, thresh: str = "0.5 mm/day", freq: str = "MS") -> xr.DataArray:
    """CDD with a lower threshold suited to Rwanda's dry season definition."""
    return call_with_ogs_time_dim(xclim.compute.maximum_consecutive_dry_days, pr, thresh=thresh, freq=freq)
```

Datacubes in Open Geo Service name their time dimension `t`, while xclim requires `time`. `call_with_ogs_time_dim` renames it on the way in and back on the result; calling xclim directly on a cube fails with `KeyError: 'time'`.

See [Extensibility — Processes](extensibility.md#processes) for the full `@process` decorator reference.

---

## References

- [xclim documentation](https://xclim.readthedocs.io)
- [xclim indicator catalogue](https://xclim.readthedocs.io/en/stable/indicators.html)
- [earthkit-meteo documentation](https://earthkit-meteo.readthedocs.io)
- [openEO process graph specification](https://processes.openeo.org)
