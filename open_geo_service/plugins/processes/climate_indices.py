"""Curated overrides for xclim climate index processes.

The base layer of xclim processes is auto-registered by xclim_processes.scan().
This file only needs to override indicators where the auto-generated metadata
is insufficient.
"""

from typing import Any, cast

import xarray as xr
import xclim.compute
import xclim.indicators.atmos as xclim_atmos
from xclim.core import InputKind
from xclim.core.units import units2pint

from open_geo_service.openeo.xclim_processes import call_with_ogs_time_dim
from open_geo_service.process import process

_VARIABLE_SCHEMA: dict[str, Any] = {"type": "object", "subtype": "datacube"}


def _xclim_params(indicator: Any, *names: str) -> dict[str, dict[str, Any]]:
    """Read parameter descriptions and schemas from an xclim indicator."""
    result: dict[str, dict[str, Any]] = {}
    for name in names:
        if name not in indicator.parameters:
            continue
        p = indicator.parameters[name]
        entry: dict[str, Any] = {}
        if p.description:
            entry["description"] = p.description
        if p.kind in (InputKind.VARIABLE, InputKind.OPTIONAL_VARIABLE):
            entry["schema"] = _VARIABLE_SCHEMA
        result[name] = entry
    return result


_spi_ind = xclim_atmos.standardized_precipitation_index


@process(
    summary="Standardized Precipitation Index (SPI)",
    parameters={
        **_xclim_params(_spi_ind, "pr", "cal_start", "cal_end", "freq"),
        "pr": {
            "description": (
                "Precipitation as stored, daily or monthly totals in mm, or a rate such as mm/d. "
                "Totals are summed to monthly totals, so a window accumulates them."
            ),
            "schema": _VARIABLE_SCHEMA,
        },
        "window": {"description": "Accumulation window in months (1 = SPI-1, 3 = SPI-3, 6 = SPI-6)."},
    },
)
def spi(
    pr: xr.DataArray,
    window: int = 1,
    cal_start: str | None = None,
    cal_end: str | None = None,
    freq: str | None = "MS",
) -> xr.DataArray:
    """Standardized Precipitation Index.

    Computes SPI at a given accumulation timescale using a gamma distribution
    fitted to the calibration period. Values below -1 indicate drought conditions;
    values above +1 indicate wet conditions.

    Precipitation totals are summed to monthly totals first, so SPI-3 is fitted to 3-month
    totals as WMO-No. 1090 defines it; load whole months, since a partial month sums to too
    little. A rate is passed to xclim as it is.
    """
    if _is_amount(pr):
        pr = _monthly_totals(pr, freq)
    # Delegate to xclim — the custom process exists to give SPI a richer description,
    # and to pin a robust fitting configuration. call_with_ogs_time_dim maps our ``t``
    # dimension to the ``time`` dimension xclim requires (and back).
    #
    # We fit a two-parameter gamma (location fixed at 0 via fitkwargs) with the APP
    # (L-moments) method. This is the standard SPI setup and, unlike xclim's default
    # maximum-likelihood fit, does not raise FitError on real precipitation that has a
    # pronounced dry season (e.g. near-zero winter months), where ML optimization
    # diverges.
    result = call_with_ogs_time_dim(
        xclim.compute.standardized_precipitation_index,
        pr,
        freq=freq,
        window=window,
        dist="gamma",
        method="APP",
        fitkwargs={"floc": 0},
        cal_start=cal_start,
        cal_end=cal_end,
    )
    # xclim attaches gamma-fit diagnostics (number_of_zeros, number_of_notnull) as
    # non-dimension coordinates. They are fit internals, not publishable layers — drop
    # them so the store and STAC cube:variables expose only the SPI field
    # (georeferencing coords like spatial_ref are dimension/known coords and are left
    # untouched).
    diagnostics = [c for c in ("number_of_zeros", "number_of_notnull") if c in result.coords]
    if diagnostics:
        result = result.drop_vars(diagnostics)
    # xclim returns float64; downcast to float32. SPI is a small standardized index
    # (~±3) so float32 is ample precision, and it keeps the published store renderable
    # by WebGL map clients (carbonplan ZarrLayer uploads to float32 textures).
    return cast(xr.DataArray, result.astype("float32"))


def _is_amount(pr: xr.DataArray) -> bool:
    """Whether ``pr`` is a precipitation amount (a length such as mm) rather than a rate."""
    units = pr.attrs.get("units")
    if not isinstance(units, str) or not units:
        return False
    return bool(units2pint(units).dimensionality == units2pint("mm").dimensionality)


def _monthly_totals(pr: xr.DataArray, freq: str | None) -> xr.DataArray:
    """Sum precipitation totals to calendar months, labelled as a rate per month.

    xclim takes a rate, and converts ``mm month-1`` to its own units with a constant factor (a
    month being a twelfth of a year), which a gamma fit per calendar month does not see. Its
    mean over a ``window`` of months is then proportional to their total, the accumulation SPI.
    """
    if freq != "MS":
        raise ValueError(
            f"SPI of precipitation totals is computed on monthly totals, so freq must be 'MS', got {freq!r}; "
            "for another frequency, convert the totals to a rate with amount2rate first"
        )
    t_dim = "t" if "t" in pr.dims else "time"
    totals = pr.resample({t_dim: "MS"}).sum(min_count=1)
    return totals.assign_attrs(
        {**pr.attrs, "units": f"{pr.attrs['units']} month-1", "standard_name": "lwe_precipitation_rate"}
    )
