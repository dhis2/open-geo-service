"""Tests for the auto-registered xclim index processes, SPI among them."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient
from xclim.core import ValidationError
from xclim.core.indicator import registry

from open_geo_service.openeo.plugin_processes import load_plugin_processes
from open_geo_service.openeo.xclim_processes import _REALMS, REPLACED
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
# SPI — xclim's own indicator, registered like the others (CLIM-2021)
# ---------------------------------------------------------------------------


def test_spi_appears_in_get_processes_endpoint(client: TestClient) -> None:
    response = client.get("/processes")
    assert response.status_code == 200
    procs = {p["id"]: p for p in response.json()["processes"]}
    ids = set(procs)
    assert {"spi", "amount2rate", "rate2amount"} <= ids
    assert "SPI" in procs["spi"]["summary"]
    spi_params = {p["name"]: p for p in procs["spi"]["parameters"]}
    # The fit is the caller's choice, as xclim offers it.
    assert {"pr", "window", "freq", "cal_start", "cal_end", "dist", "method", "fitkwargs"} <= set(spi_params)
    assert spi_params["window"]["default"] == 1
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


def test_spi_of_precipitation_totals_takes_their_rate(daily_precip: xr.DataArray) -> None:
    """The chain in docs/climate_indices.md, through the registered processes."""
    procs = dict(load_plugin_processes())
    totals = _totals(pd.DatetimeIndex(daily_precip.time.values), daily_precip.values)

    spi3 = procs["spi"](pr=procs["amount2rate"](data=totals), window=3, freq="MS")

    assert spi3.dims == ("t", "y", "x")
    assert spi3.sizes["t"] == 60
    assert np.isfinite(spi3.values).any()
    with pytest.raises(ValidationError, match="units mm are not compatible"):
        procs["spi"](pr=totals, window=3, freq="MS")


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


# ---------------------------------------------------------------------------
# Dry and wet spells, TX days above — auto-registered from xclim
# ---------------------------------------------------------------------------


def test_spells_and_tx_appear_in_get_processes(client: TestClient) -> None:
    response = client.get("/processes")
    ids = {p["id"] for p in response.json()["processes"]}
    assert {"dry_spell_max_length", "wet_spell_max_length", "tx_days_above"} <= ids
    assert set(REPLACED.values()) <= ids
    assert not set(REPLACED) & ids


def test_every_deprecated_indicator_is_replaced() -> None:
    """An xclim upgrade that deprecates another indicator fails here until it is reviewed."""
    deprecated = {
        key: obj.version_deprecated[1]
        for key, obj in registry.items()
        if "." not in key and obj.realm in _REALMS and obj.version_deprecated
    }
    assert deprecated == REPLACED


def test_dry_spell_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "dry spell" in procs["dry_spell_max_length"]["summary"].lower()
    thresh = next(p for p in procs["dry_spell_max_length"]["parameters"] if p["name"] == "thresh")
    assert thresh["default"] == "1 mm"
    assert thresh["schema"].get("type") == "string"


def test_wet_spell_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "wet spell" in procs["wet_spell_max_length"]["summary"].lower()


def test_tx_days_above_catalog_metadata(client: TestClient) -> None:
    procs = {p["id"]: p for p in client.get("/processes").json()["processes"]}
    assert "maximum temperature" in procs["tx_days_above"]["summary"].lower()
    thresh = next(p for p in procs["tx_days_above"]["parameters"] if p["name"] == "thresh")
    assert "25" in thresh["default"]


def test_dry_spell_registered_callable_computes(daily_precip: xr.DataArray) -> None:
    procs = dict(load_plugin_processes())
    assert "dry_spell_max_length" in procs
    result = xr.DataArray(procs["dry_spell_max_length"](pr=daily_precip))
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


@pytest.mark.parametrize(
    ("old", "variable", "old_args", "new_args"),
    [
        ("cdd", "pr", {}, {"thresh": "1 mm", "window": 1}),
        ("cwd", "pr", {}, {"thresh": "1 mm", "window": 1}),
        ("heat_wave_index", "tasmax", {}, {"thresh": "25 degC", "window": 5}),
        ("cold_spell_days", "tas", {"thresh": "15 degC"}, {"thresh": "15 degC", "window": 5}),
        ("maximum_consecutive_warm_days", "tasmax", {}, {"thresh": "25 degC"}),
    ],
)
def test_a_replacement_gives_the_deprecated_indicators_values_with_the_documented_arguments(
    old: str,
    variable: str,
    old_args: dict[str, str],
    new_args: dict[str, str],
    daily_precip: xr.DataArray,
    daily_tasmax: xr.DataArray,
) -> None:
    from xclim.core.options import set_options

    # The fixture's flux would make every day wet; as a daily amount it has spells of both.
    data = daily_precip.assign_attrs(units="mm/day") if variable == "pr" else daily_tasmax
    with set_options(as_dataset=False):
        expected = registry[old](**{variable: data}, **old_args)
    new = dict(load_plugin_processes())[REPLACED[old]]
    assert float(expected.sum()) > 0
    np.testing.assert_array_equal(new(**{variable: data}, **new_args).values, expected.values)
    if old in ("cdd", "heat_wave_index"):
        # The replacement's own defaults count other spells.
        assert not np.array_equal(new(**{variable: data}, **old_args).values, expected.values)


def test_tx_days_above_registered_callable_higher_threshold_gives_fewer_days(daily_tasmax: xr.DataArray) -> None:
    procs = dict(load_plugin_processes())
    assert "tx_days_above" in procs
    r25 = xr.DataArray(procs["tx_days_above"](tasmax=daily_tasmax, thresh="25 degC"))
    r35 = xr.DataArray(procs["tx_days_above"](tasmax=daily_tasmax, thresh="35 degC"))
    assert float(r35.mean().item()) <= float(r25.mean().item())


def test_xclim_returns_schema_is_datacube() -> None:
    """Auto-registered xclim processes should declare a datacube return type."""
    procs = dict(load_plugin_processes())
    cdd_fn = procs["dry_spell_max_length"]
    meta = get_process_metadata(cdd_fn)
    assert meta is not None
    schema = meta.get("returns", {}).get("schema", {})
    assert schema.get("subtype") == "datacube", f"expected datacube subtype in returns schema, got {schema}"


def test_dry_spell_via_process_graph(client: TestClient, daily_precip: xr.DataArray) -> None:
    """A dry spell process should be callable through the openEO synchronous result endpoint."""
    from unittest.mock import patch

    def mock_run(process: dict, request: object = None, **kwargs: object) -> object:
        procs = dict(load_plugin_processes())
        cdd_fn = procs["dry_spell_max_length"]
        # Return a plain dict so the route serialises it as JSON
        result = cdd_fn(pr=daily_precip, thresh="1 mm")
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
                        "process_id": "dry_spell_max_length",
                        "arguments": {"pr": {"from_node": "load"}, "thresh": "1 mm"},
                        "result": True,
                    },
                }
            },
        )
    assert resp.status_code == 200
