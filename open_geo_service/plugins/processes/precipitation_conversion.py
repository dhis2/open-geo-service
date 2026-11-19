"""Conversion between precipitation totals and rates (CLIM-1638).

Precipitation is stored as totals (``mm`` per day or month), while xclim's precipitation
indicators take a rate. These processes convert between the two with xclim's own converters,
which divide each step by its actual length (a 29-day February included).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import numpy as np
import xarray as xr
from xclim.core.units import amount2rate as xclim_amount2rate
from xclim.core.units import rate2amount as xclim_rate2amount

from open_geo_service.openeo.xclim_processes import call_with_ogs_time_dim
from open_geo_service.process import process

_DATA = {"description": "Precipitation cube with a datetime temporal dimension and a `units` attribute."}


@process(
    summary="Precipitation totals to rates",
    parameters={"data": _DATA, "out_units": {"description": "Units of the rate, `mm/d` by default."}},
)
def amount2rate(data: xr.DataArray, out_units: str = "mm/d") -> xr.DataArray:
    """Convert precipitation totals (e.g. mm per month) to a rate, dividing each step by its length.

    The rate is what xclim's precipitation indicators, such as `spi` and `water_budget`, take.
    Needs at least 3 regular time steps with none missing.
    """
    return _convert(xclim_amount2rate, data, out_units, "time: mean")


@process(
    summary="Precipitation rates to totals",
    parameters={"data": _DATA, "out_units": {"description": "Units of the total, `mm` by default."}},
)
def rate2amount(data: xr.DataArray, out_units: str = "mm") -> xr.DataArray:
    """Convert a precipitation rate (e.g. mm/d) to the total over each step, multiplying by its length.

    Needs at least 3 regular time steps with none missing.
    """
    return _convert(xclim_rate2amount, data, out_units, "time: sum")


def _convert(func: Callable[..., Any], data: xr.DataArray, out_units: str, cell_methods: str) -> xr.DataArray:
    t_dim = next((d for d in ("t", "time") if d in data.dims), None)
    if t_dim is None or not np.issubdtype(data[t_dim].dtype, np.datetime64):
        raise ValueError(
            "converting between precipitation totals and rates needs a datetime temporal dimension; "
            "a climatology (a dayofyear or month axis) cannot be converted, see "
            "docs/climate_indices.md#limitations"
        )
    # xclim needs the frequency to know each step's length. Where it cannot infer one because a
    # step is missing, it falls back to the gaps between steps and divides by the wrong length.
    try:
        freq = xr.infer_freq(data[t_dim])
    except ValueError:  # fewer than 3 steps
        freq = None
    if freq is None:
        raise ValueError(
            "converting between precipitation totals and rates needs at least 3 regular time steps "
            "with none missing; widen temporal_extent"
        )
    result = cast(xr.DataArray, call_with_ogs_time_dim(func, data, out_units=out_units))
    result.attrs["cell_methods"] = cell_methods
    return result.astype(data.dtype)
