"""Tests for the SPI curated wrapper and auto-registered xclim index processes."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from open_geo_service.openeo.plugin_processes import load_plugin_processes
from open_geo_service.plugins.processes.climate_indices import spi
from open_geo_service.plugins.processes.precipitation_conversion import amount2rate, rate2amount
from open_geo_service.process import get_process_metadata


@pytest.fixture()
def daily_precip() -> xr.DataArray:
    """Five years of synthetic daily precipitation (kg m-2 s-1)."""
    rng = np.random.default_rng(42)
    time = pd.date_range("2000-01-01", periods=365 * 5, freq="D")
    data = rng.exponential(scale=3.0, size=len(time)).astype(np.float32)
    da = xr.DataArray(data, dims=["time"], coords={"time": time})
    da.attrs["units"] = "kg m-2 s-1"
    return da


@pytest.fixture()
def daily_tasmax() -> xr.DataArray:
    """Five years of synthetic daily maximum temperature (degC)."""
    rng = np.random.default_rng(7)
    time = pd.date_range("2000-01-01", periods=365 * 5, freq="D")
    data = rng.normal(loc=20.0, scale=8.0, size=len(time)).astype(np.float32)
    da = xr.DataArray(data, dims=["time"], coords={"time": time})
    da.attrs["units"] = "degC"
    return da


# ---------------------------------------------------------------------------
# SPI — curated wrapper
# ---------------------------------------------------------------------------


def test_spi_is_registered_as_process() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    assert meta["id"] == "spi"
    assert "SPI" in meta["summary"]


def test_spi_parameters_include_window_and_calibration() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    param_names = {p["name"] for p in meta["parameters"]}
    assert {"pr", "window", "cal_start", "cal_end", "freq"} <= param_names


def test_spi_window_defaults() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    window_param = next(p for p in meta["parameters"] if p["name"] == "window")
    assert window_param["optional"] is True
    assert window_param["default"] == 1


def test_spi1_returns_dataarray_with_time_dimension(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1)
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


def test_spi1_output_length_matches_monthly_resampling(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1)
    assert result.sizes["time"] == 60


def test_spi_drops_xclim_fit_diagnostic_coords(daily_precip: xr.DataArray) -> None:
    """xclim attaches gamma-fit diagnostics as non-dim coords; we drop them so the
    published store / STAC cube:variables expose only the SPI field, while keeping
    georeferencing coords like spatial_ref."""
    cube = daily_precip.assign_coords(spatial_ref=0)
    result = spi(cube, window=3)
    assert not {"prob_of_zero", "number_of_zeros", "number_of_notnull"} & set(result.coords)
    assert "spatial_ref" in result.coords
    assert result.dtype == "float32"


def test_spi_accepts_ogs_t_dimension_and_returns_t(daily_precip: xr.DataArray) -> None:
    """OGS stores name the temporal dim 't'; xclim needs 'time'. The wrapper renames
    on-the-fly and restores 't' on the output, so graphs need no rename_dimension."""
    cube_t = daily_precip.rename({"time": "t"})
    result = spi(cube_t, window=1)
    assert isinstance(result, xr.DataArray)
    assert "t" in result.dims and "time" not in result.dims
    assert result.sizes["t"] == 60


def test_spi3_produces_different_values_than_spi1(daily_precip: xr.DataArray) -> None:
    r1 = spi(daily_precip, window=1)
    r3 = spi(daily_precip, window=3)
    assert not np.allclose(r1.values, r3.values, equal_nan=True)


def test_spi_with_calibration_period(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1, cal_start="2000-01-01", cal_end="2002-12-31")
    assert isinstance(result, xr.DataArray)
    assert result.sizes["time"] == 60


def test_spi_appears_in_get_processes_endpoint(client: TestClient) -> None:
    response = client.get("/processes")
    assert response.status_code == 200
    ids = {p["id"] for p in response.json()["processes"]}
    assert {"spi", "amount2rate", "rate2amount"} <= ids
    # Of xclim's `convert` realm, only the water-budget chain SPEI needs; its humidity converters
    # would repeat the earthkit-meteo processes under other names.
    assert {"potential_evapotranspiration", "water_budget", "water_budget_from_tas"} <= ids
    assert not {"hurs_fromdewpoint", "huss_fromdewpoint", "e_sat"} & ids
    assert "relative_humidity_from_dewpoint" in ids


# ---------------------------------------------------------------------------
# Precipitation totals and rates (CLIM-1638)
# ---------------------------------------------------------------------------


def _totals(times: pd.DatetimeIndex, values: np.ndarray) -> xr.DataArray:
    """Precipitation totals in mm as a stored OGS cube: a `t` dimension and a geographic CRS."""
    import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]  # activates .rio

    data = np.broadcast_to(values.astype("float32")[:, None, None], (len(times), 2, 2)).copy()
    cube = xr.DataArray(
        data,
        dims=("t", "y", "x"),
        coords={"t": times, "y": [9.0, 8.0], "x": [-12.0, -11.0]},
        name="precip",
        attrs={"units": "mm", "standard_name": "lwe_thickness_of_precipitation_amount", "cell_methods": "time: sum"},
    )
    return cube.rio.write_crs(4326)


def test_amount2rate_divides_each_month_by_its_length() -> None:
    times = pd.date_range("2023-12-01", periods=4, freq="MS")  # February 2024 has 29 days
    totals = _totals(times, np.array([31.0, 62.0, 29.0, 93.0]))

    rate = amount2rate(totals)

    assert rate.dims == ("t", "y", "x")
    assert rate.isel(y=0, x=0).values.tolist() == pytest.approx([1.0, 2.0, 1.0, 3.0])
    assert rate.dtype == "float32"
    assert rate.attrs["units"] == "mm d-1"
    assert rate.attrs["standard_name"] == "lwe_precipitation_rate"
    assert rate.attrs["cell_methods"] == "time: mean"
    back = rate2amount(rate)
    assert back.isel(y=0, x=0).values.tolist() == pytest.approx([31.0, 62.0, 29.0, 93.0])
    assert back.attrs["units"] == "mm"
    assert back.attrs["standard_name"] == "lwe_thickness_of_precipitation_amount"
    assert back.attrs["cell_methods"] == "time: sum"


@pytest.mark.parametrize("steps", [[0], [0, 1], [0, 1, 3]], ids=["one", "two", "gap"])
def test_amount2rate_refuses_steps_it_cannot_measure(steps: list[int]) -> None:
    """Without 3 regular steps xclim cannot infer a step's length; with a missing step it would
    silently divide by the gap instead."""
    totals = _totals(pd.date_range("2024-01-01", periods=4, freq="MS"), np.full(4, 30.0)).isel(t=steps)
    with pytest.raises(ValueError, match="at least 3 regular time steps"):
        amount2rate(totals)


def test_amount2rate_refuses_a_climatology() -> None:
    normal = _totals(pd.date_range("2024-01-01", periods=12, freq="MS"), np.full(12, 30.0))
    normal = normal.rename({"t": "month"}).assign_coords(month=np.arange(1, 13))
    with pytest.raises(ValueError, match="climatology"):
        amount2rate(normal)


def test_spi_of_totals_is_the_spi_of_accumulated_totals(daily_precip: xr.DataArray) -> None:
    """SPI-3 is SPI-1 of 3-month totals (WMO-No. 1090), computed here without the wrapper's
    own monthly sum or xclim's window. In float64, as float32 sums differ in the last digits."""
    totals = _totals(pd.DatetimeIndex(daily_precip.time.values), daily_precip.values).astype("float64")
    three_months = totals.resample(t="MS").sum().rolling(t=3).sum()
    reference = spi(three_months.assign_attrs(units="mm month-1"), window=1)

    xr.testing.assert_allclose(spi(totals, window=3), reference)
    # A rate, from amount2rate or stored, still goes to xclim as it is.
    assert spi(amount2rate(totals), window=3).sizes["t"] == 60
    with pytest.raises(ValueError, match="freq must be 'MS'"):
        spi(totals, freq="YS")


def test_spei_from_precipitation_totals_and_temperature() -> None:
    """The chain in docs/climate_indices.md, through the registered processes."""
    procs = dict(load_plugin_processes())
    times = pd.date_range("2000-01-01", periods=365 * 4, freq="D")
    rng = np.random.default_rng(3)
    season = 5 * np.sin(2 * np.pi * np.arange(len(times)) / 365)
    tasmin = _totals(times, 20 + season).assign_attrs(units="degC", standard_name="air_temperature")
    tasmax = (tasmin + 10).assign_attrs(tasmin.attrs)
    totals = _totals(times, rng.gamma(0.8, 4.0, len(times)) * (1 + season / 5))

    pet = procs["potential_evapotranspiration"](tasmin=tasmin, tasmax=tasmax, method="HG85")
    wb = procs["water_budget"](pr=procs["amount2rate"](data=totals), evspsblpot=pet)
    spei = procs["spei"](wb=wb, freq="MS", window=3)

    assert spei.dims == ("t", "y", "x")
    assert spei.sizes["t"] == 48
    assert np.isfinite(spei.values).any()


def test_spi_process_summary_in_catalog(client: TestClient) -> None:
    response = client.get("/processes")
    procs = {p["id"]: p for p in response.json()["processes"]}
    assert "SPI" in procs["spi"]["summary"]


# ---------------------------------------------------------------------------
# CDD, CWD, TX days above — auto-registered from xclim
# ---------------------------------------------------------------------------


def test_cdd_cwd_tx_appear_in_get_processes(client: TestClient) -> None:
    response = client.get("/processes")
    ids = {p["id"] for p in response.json()["processes"]}
    assert {"cdd", "cwd", "tx_days_above"} <= ids


def test_cdd_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "consecutive dry" in procs["cdd"]["summary"].lower()
    thresh = next(p for p in procs["cdd"]["parameters"] if p["name"] == "thresh")
    assert thresh["default"] == "1 mm/day"
    assert thresh["schema"].get("type") == "string"


def test_cwd_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "consecutive wet" in procs["cwd"]["summary"].lower()


def test_tx_days_above_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "maximum temperature" in procs["tx_days_above"]["summary"].lower()
    thresh = next(p for p in procs["tx_days_above"]["parameters"] if p["name"] == "thresh")
    assert "25" in thresh["default"]


def test_cdd_registered_callable_computes(daily_precip: xr.DataArray) -> None:
    procs = dict(load_plugin_processes())
    assert "cdd" in procs
    result = xr.DataArray(procs["cdd"](pr=daily_precip))
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


def test_cdd_cwd_registered_callables_are_complementary(daily_precip: xr.DataArray) -> None:
    procs = dict(load_plugin_processes())
    r_cdd = xr.DataArray(procs["cdd"](pr=daily_precip))
    r_cwd = xr.DataArray(procs["cwd"](pr=daily_precip))
    assert not np.allclose(r_cdd.values, r_cwd.values, equal_nan=True)


def test_tx_days_above_registered_callable_higher_threshold_gives_fewer_days(daily_tasmax: xr.DataArray) -> None:
    procs = dict(load_plugin_processes())
    assert "tx_days_above" in procs
    r25 = xr.DataArray(procs["tx_days_above"](tasmax=daily_tasmax, thresh="25 degC"))
    r35 = xr.DataArray(procs["tx_days_above"](tasmax=daily_tasmax, thresh="35 degC"))
    assert float(r35.mean().item()) <= float(r25.mean().item())


def test_xclim_returns_schema_is_datacube() -> None:
    """Auto-registered xclim processes should declare a datacube return type."""
    procs = dict(load_plugin_processes())
    cdd_fn = procs["cdd"]
    meta = get_process_metadata(cdd_fn)
    assert meta is not None
    schema = meta.get("returns", {}).get("schema", {})
    assert schema.get("subtype") == "datacube", f"expected datacube subtype in returns schema, got {schema}"


def test_spi_nullable_string_params_have_no_null_default() -> None:
    """str | None parameters should not emit default=None with a string schema.

    Emitting default=null alongside schema={'type':'string'} is a mismatch
    that confuses clients which trust the metadata.
    """
    meta = get_process_metadata(spi)
    assert meta is not None
    for param in meta["parameters"]:
        schema = param.get("schema", {})
        if schema.get("type") == "string":
            assert param.get("default") is not None or "default" not in param, (
                f"Parameter '{param['name']}' has schema type=string but default=null"
            )


def test_cdd_via_process_graph(client: TestClient, daily_precip: xr.DataArray) -> None:
    """CDD should be callable through the openEO synchronous result endpoint."""
    from unittest.mock import patch

    def mock_run(process: dict, request: object = None, **kwargs: object) -> object:
        procs = dict(load_plugin_processes())
        cdd_fn = procs["cdd"]
        # Return a plain dict so the route serialises it as JSON
        result = cdd_fn(pr=daily_precip, thresh="1 mm/day")
        return {"cdd": float(result.mean().item())}  # type: ignore[union-attr]

    with patch("open_geo_service.openeo.execution.run_process_graph", mock_run):
        resp = client.post(
            "/result",
            json={
                "process_graph": {
                    "load": {
                        "process_id": "load_collection",
                        "arguments": {"id": "chirps3_precipitation_daily"},
                    },
                    "cdd": {
                        "process_id": "cdd",
                        "arguments": {"pr": {"from_node": "load"}, "thresh": "1 mm/day"},
                        "result": True,
                    },
                }
            },
        )
    assert resp.status_code == 200
