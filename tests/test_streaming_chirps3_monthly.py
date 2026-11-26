"""Tests for the shared monthly period helper and the CHIRPS3 monthly template (CLIM-867).

The plugin itself is tested with the daily one, in test_streaming_chirps3.py.
"""

from __future__ import annotations

from datetime import date

import pytest

from open_geo_service.data_registry.services import datasets as registry
from open_geo_service.shared.time import monthly_period_ids

# --- monthly_period_ids ---------------------------------------------------------------


def test_enumerates_months_across_a_year_boundary() -> None:
    assert monthly_period_ids("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_single_month() -> None:
    assert monthly_period_ids("2025-01", "2025-01") == ["2025-01"]


def test_reversed_range_is_empty() -> None:
    assert monthly_period_ids("2025-03", "2025-01") == []


def test_cutoff_caps_the_end() -> None:
    assert monthly_period_ids("2024-11", "2025-06", cutoff="2024-12") == ["2024-11", "2024-12"]


def test_cutoff_beyond_the_end_is_ignored() -> None:
    assert monthly_period_ids("2025-01", "2025-02", cutoff="2030-01") == ["2025-01", "2025-02"]


@pytest.mark.parametrize("value", ["2024-11-17", "2024-11-01"])
def test_any_day_within_a_month_selects_that_month(value: str) -> None:
    assert monthly_period_ids(value, "2024-12") == ["2024-11", "2024-12"]


def test_accepts_date_objects() -> None:
    assert monthly_period_ids(date(2024, 11, 17), date(2025, 1, 3)) == ["2024-11", "2024-12", "2025-01"]


def test_an_out_of_range_month_does_not_loop_forever() -> None:
    """Regression: month 13 used to hang, not merely return a wrong answer.

    The increment only wraps at exactly 12, so a month of 13 climbed indefinitely while the
    ``(year, month) <= (last_year, last_month)`` guard stayed true — an unbounded loop
    appending to a list. Validation is what makes it unreachable.
    """
    import signal

    def _bail(*_: object) -> None:
        raise TimeoutError("monthly_period_ids did not terminate")

    original = signal.signal(signal.SIGALRM, _bail)
    signal.alarm(5)
    try:
        with pytest.raises(ValueError, match="Invalid monthly period"):
            monthly_period_ids("2025-13", "2026-01")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original)


@pytest.mark.parametrize("value", ["2025-13", "2025-00", "2025-1", "2025", "garbage", ""])
def test_rejects_malformed_months(value: str) -> None:
    """Fails fast like ``daily_period_ids`` does via ``date.fromisoformat``."""
    with pytest.raises(ValueError, match="Invalid monthly period"):
        monthly_period_ids(value, "2026-01")


def test_rejects_a_malformed_end_too() -> None:
    with pytest.raises(ValueError, match="Invalid monthly period"):
        monthly_period_ids("2025-01", "2025-13")


# --- the template ---------------------------------------------------------------------


def test_template_is_registered_with_the_monthly_plugin() -> None:
    template = {d["id"]: d for d in registry.list_datasets()}["chirps3_precipitation_monthly"]
    assert template["period_type"] == "monthly"
    assert template["ingestion"]["plugin"].endswith("CHIRPS3MonthlyPlugin")


def test_precipitation_templates_declare_totals() -> None:
    """Every precipitation dataset is a total in mm (CLIM-1638), so an observed month, its
    normal and their absolute anomaly agree, and only a relative anomaly is in percent."""
    precipitation = [d for d in registry.list_datasets() if d.get("variable") in {"precip", "tp"}]
    assert len(precipitation) >= 15
    for template in precipitation:
        expected = "%" if "relative_anomaly" in template["id"] else "mm"
        assert template["units"] == expected, template["id"]
        if template.get("ingestion") and template["period_type"] != "climatology":
            assert template["standard_name"] == "lwe_thickness_of_precipitation_amount", template["id"]
            assert template["cell_methods"] == "time: sum", template["id"]
