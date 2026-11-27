"""Derived dataset recipes: what a template may declare, and refreshing after the input updates."""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from open_geo_service.data_registry import recipes
from open_geo_service.data_registry.recipes import Recipe
from open_geo_service.derivations import service as derivations
from open_geo_service.jobs import dispatch
from open_geo_service.jobs.models import DATASET_UPDATED_EVENT_TYPE, JobEvent
from open_geo_service.openeo import jobs as openeo_jobs
from open_geo_service.openeo import workflows
from open_geo_service.openeo.schemas import OpenEOJobRecord, OpenEOJobStatus
from tests.test_managed_publish_units import managed_instance  # noqa: F401  # pyright: ignore[reportUnusedImport]
from tests.test_openeo_publication_events import (
    _DATASET,
    _job,
    service,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)

_ANOMALY = Recipe(
    output="rain_anomaly",
    workflow="climate_anomaly",
    arguments={"observed_dataset_id": "rain", "normal_dataset_id": "rain_normal", "variable": "tp"},
    refresh_on="observed_dataset_id",
)


def test_both_forms_name_their_workflow_and_only_a_recipe_runs() -> None:
    named = {"id": "rain_normal", "produced_by": "climate_normal"}
    recipe = {"id": "rain_anomaly", "produced_by": {"workflow": "climate_anomaly", **_recipe_body()}}

    assert [recipes.producing_workflow(t) for t in (named, recipe)] == ["climate_normal", "climate_anomaly"]
    assert recipes.recipe(named) is None
    assert recipes.recipe(recipe) == _ANOMALY
    assert _ANOMALY.input == "rain"
    changed = replace(_ANOMALY, arguments={**_ANOMALY.arguments, "variable": "t2m"})
    assert changed.digest != _ANOMALY.digest


def _recipe_body() -> dict[str, Any]:
    return {"arguments": dict(_ANOMALY.arguments), "refresh_on": "observed_dataset_id"}


def test_built_in_monthly_anomalies_refresh_after_their_source() -> None:
    drivers = {recipe.output: recipe.input for recipe in derivations.list_recipes()}

    assert {
        output: drivers.get(output)
        for output in (
            "chirps3_precipitation_monthly_anomaly_1991_2020",
            "chirps3_precipitation_monthly_relative_anomaly_1991_2020",
            "era5land_temperature_monthly_anomaly_1991_2020",
            "era5land_precipitation_monthly_anomaly_1991_2020",
            "era5land_precipitation_monthly_relative_anomaly_1991_2020",
        )
    } == {
        "chirps3_precipitation_monthly_anomaly_1991_2020": "chirps3_precipitation_monthly",
        "chirps3_precipitation_monthly_relative_anomaly_1991_2020": "chirps3_precipitation_monthly",
        "era5land_temperature_monthly_anomaly_1991_2020": "era5land_temperature_monthly",
        "era5land_precipitation_monthly_anomaly_1991_2020": "era5land_precipitation_monthly",
        "era5land_precipitation_monthly_relative_anomaly_1991_2020": "era5land_precipitation_monthly",
    }


def test_startup_refuses_recipes_that_cannot_run(monkeypatch: pytest.MonkeyPatch) -> None:
    derivations.validate_recipes()  # the built-in recipes run

    broken = [
        Recipe(output="a", workflow="no_such_workflow", arguments={}),
        Recipe(output="b", workflow="aggregate_by_features", arguments={}),
        Recipe(output="c", workflow="climate_anomaly", arguments={**_ANOMALY.arguments, "unknown": 1}),
        Recipe(output="d", workflow="climate_anomaly", arguments={"observed_dataset_id": "rain"}),
        Recipe(output="e", workflow="climate_anomaly", arguments=dict(_ANOMALY.arguments)),
        Recipe(
            output="f",
            workflow="climate_anomaly",
            arguments={**_ANOMALY.arguments, "temporal_extent": ["2020-01", "2020-12"]},
            refresh_on="observed_dataset_id",
        ),
        Recipe(
            output="g",
            workflow="climate_anomaly",
            arguments={**_ANOMALY.arguments, "observed_dataset_id": "h"},
            refresh_on="observed_dataset_id",
        ),
        Recipe(
            output="h",
            workflow="climate_anomaly",
            arguments={**_ANOMALY.arguments, "observed_dataset_id": "g"},
            refresh_on="observed_dataset_id",
        ),
    ]
    monkeypatch.setattr(derivations, "list_recipes", lambda: broken)

    with pytest.raises(ValueError) as refused:
        derivations.validate_recipes()

    message = str(refused.value)
    for expected in (
        "'a': 'no_such_workflow' is not a registered workflow",
        "'b': workflow 'aggregate_by_features' does not publish a dataset named by output_dataset_id",
        "'c': 'unknown' is not a parameter of workflow 'climate_anomaly'",
        "'d': missing argument(s) normal_dataset_id, variable",
        "'e': set 'temporal_extent': a recipe without refresh_on runs over a fixed range",
        "'f': leave 'temporal_extent' out",
        *(f"'{output}': its updates lead back to itself" for output in ("g", "h")),
        "'f': refresh_on names 'rain', which is not a dataset template",
    ):
        assert expected in message


class _FakeOpenEO:
    """Records the jobs refreshes create; nothing runs until a test says how it ended."""

    def __init__(self) -> None:
        self.jobs: dict[str, OpenEOJobRecord] = {}

    def create_triggered_job(
        self,
        body: Any,
        *,
        source_event_id: str,
        trigger_id: str,
        max_attempts: int = 1,
        merge_publication: bool = False,
    ) -> tuple[OpenEOJobRecord, bool]:
        job_id = openeo_jobs.triggered_job_id(source_event_id, trigger_id)
        if job_id in self.jobs:
            return self.jobs[job_id], False
        self.jobs[job_id] = OpenEOJobRecord(
            id=job_id,
            title=body.title,
            description=body.description,
            process=body.process,
            status=OpenEOJobStatus.CREATED,
            created=datetime.now(UTC),
            trigger_id=trigger_id,
            source_event_id=source_event_id,
            max_attempts=max_attempts,
            merge_publication=merge_publication,
        )
        return self.jobs[job_id], True

    def start_triggered_job(self, job_id: str) -> bool:
        if self.jobs[job_id].status != OpenEOJobStatus.CREATED:
            return False
        self.set(job_id, OpenEOJobStatus.QUEUED)
        return True

    def set(self, job_id: str, status: OpenEOJobStatus) -> OpenEOJobRecord:
        self.jobs[job_id] = self.jobs[job_id].model_copy(update={"status": status})
        return self.jobs[job_id]


def _update(day: int, dataset_id: str = "rain", *changes: tuple[str, str]) -> JobEvent:
    """An update of ``dataset_id`` on day ``day`` of November 2026; by default it changed month ``day``."""
    ranges = changes or ((f"2026-{day:02d}", f"2026-{day:02d}"),)
    return JobEvent(
        event_id=f"{dataset_id}-{day}:0",
        time=datetime(2026, 11, day, tzinfo=UTC),
        type=DATASET_UPDATED_EVENT_TYPE,
        source=f"/datasets/{dataset_id}",
        data={
            "dataset_id": dataset_id,
            "changes": [{"start": start, "end": end} for start, end in ranges],
            "coverage_start": "1991-01",
            "coverage_end": max(end for _, end in ranges),
        },
    )


class _Instance:
    """Recipes, the persisted event log and the derived datasets that exist, as a test sets them."""

    def __init__(self) -> None:
        self.openeo = _FakeOpenEO()
        self.log: list[JobEvent] = []
        self.created: set[str] = set()
        self.recipes = [_ANOMALY]

    def arrive(self, event: JobEvent) -> None:
        """Persist an update and hand it to the live consumer."""
        self.log.append(event)
        derivations.consume([event])

    def job(self, index: int) -> OpenEOJobRecord:
        return list(self.openeo.jobs.values())[index]


@pytest.fixture
def instance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _Instance:
    from open_geo_service.jobs import events as job_events

    state = _Instance()
    monkeypatch.setattr(derivations.api_config, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(openeo_jobs, "get_openeo_job_service", lambda: state.openeo)
    monkeypatch.setattr(openeo_jobs, "store_list_jobs", lambda: list(state.openeo.jobs.values()))
    monkeypatch.setattr(job_events, "read_event_log", lambda: job_events.EventLog(list(state.log), None))
    monkeypatch.setattr(derivations, "list_recipes", lambda: state.recipes)
    monkeypatch.setattr(derivations, "_exists", lambda dataset_id: dataset_id in state.created)
    return state


def _graph(start: str, end: str, recipe: Recipe = _ANOMALY) -> dict[str, Any]:
    """The frozen graph a refresh of ``recipe`` over ``start`` to ``end`` submits."""
    arguments = {**recipe.arguments, "output_dataset_id": recipe.output, "temporal_extent": [start, end]}
    return workflows.freeze_process_graph(
        {"derive": {"process_id": recipe.workflow, "arguments": arguments, "result": True}}
    )


def test_a_derived_dataset_recomputes_each_range_its_input_changed_one_at_a_time(
    instance: _Instance, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = instance.openeo
    queued, created, finished = OpenEOJobStatus.QUEUED, OpenEOJobStatus.CREATED, OpenEOJobStatus.FINISHED

    instance.arrive(_update(1))
    assert fake.jobs == {}  # never created, so nothing to keep current
    instance.created.add("rain_anomaly")
    instance.log.append(_update(2, "rain_anomaly"))  # an operator ran the workflow by hand
    derivations.reconcile()
    assert fake.jobs == {}  # the creating run covers update 1

    # A sync that replaced a preliminary month and added one: each range alone, merged into the store.
    instance.arrive(_update(3, "rain", ("2026-01", "2026-01"), ("2026-03", "2026-03")))
    first, second = instance.job(0), instance.job(1)
    assert [first.status, second.status] == [queued, created]  # one refresh at a time
    assert first.process["process_graph"] == _graph("2026-01", "2026-01")
    assert second.process["process_graph"] == _graph("2026-03", "2026-03")
    assert first.merge_publication and second.merge_publication and first.trigger_id == "derive:rain_anomaly"
    instance.arrive(_update(4))
    derivations.consume([instance.log[-1]])  # the same update again
    assert len(fake.jobs) == 3
    fake.set(first.id, finished)
    derivations.on_job_finished(fake.jobs[first.id])
    assert fake.jobs[second.id].status == queued

    fake.set(second.id, OpenEOJobStatus.ERROR)
    derivations.start_waiting()  # the maintenance pass starts what waited behind it
    assert instance.job(2).status == queued
    fake.set(instance.job(2).id, finished)
    instance.arrive(_update(5))
    full = instance.job(3)
    # Month 3 was never written, so the next update recomputes the whole history.
    assert not full.merge_publication and full.process["process_graph"] == _graph("1991-01", "2026-05")
    fake.set(full.id, finished)
    instance.arrive(_update(6))
    assert instance.job(4).merge_publication  # back to the changed range
    fake.set(instance.job(4).id, finished)

    changed = replace(_ANOMALY, arguments={**_ANOMALY.arguments, "variable": "t2m"})
    instance.recipes[0] = changed
    derivations.reconcile()
    assert len(fake.jobs) == 6  # a changed recipe recomputes the whole history at startup
    assert instance.job(5).process["process_graph"] == _graph("1991-01", "2026-06", changed)
    fake.set(instance.job(5).id, finished)
    derivations.reconcile()
    assert len(fake.jobs) == 6
    registered = workflows.get_workflow
    edited = registered("climate_anomaly").model_copy(  # type: ignore[union-attr]
        update={"description": "edited", "process_graph": {**registered("climate_anomaly").process_graph, "note": {}}}  # type: ignore[union-attr]
    )
    monkeypatch.setattr(
        workflows, "get_workflow", lambda name: edited if name == "climate_anomaly" else registered(name)
    )
    instance.arrive(_update(7))
    assert not instance.job(6).merge_publication  # so does a changed workflow, at its next update
    monkeypatch.setattr(derivations.api_config, "is_read_only", lambda: True)
    instance.arrive(_update(8))
    assert len(fake.jobs) == 7


def test_a_refresh_loads_a_feature_collection_bound_by_id_at_its_current_version(
    instance: _Instance, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from open_geo_service.features import services as feature_services

    refreshed = datetime(2026, 9, 1, 6, tzinfo=UTC)
    monkeypatch.setattr(
        feature_services, "registered_collections", lambda: {"districts": SimpleNamespace(created_at=refreshed)}
    )
    # climate_anomaly takes no geometry, so the workflow drops the argument; the node it reads is what counts.
    bound = {**_ANOMALY.arguments, "geometries": {"from_features": "districts"}}
    instance.recipes = [replace(_ANOMALY, arguments=bound)]
    instance.created.add("rain_anomaly")
    instance.arrive(_update(1))

    [job] = instance.openeo.jobs.values()
    assert job.process["process_graph"]["features_districts"] == {
        "process_id": "load_features",
        "arguments": {"id": "districts", "version": refreshed.isoformat()},
    }


def test_updates_handled_at_once_start_one_refresh(instance: _Instance, monkeypatch: pytest.MonkeyPatch) -> None:
    instance.created.add("rain_anomaly")
    creating = threading.Event()
    create = instance.openeo.create_triggered_job

    def slow_create(*args: Any, **kwargs: Any) -> tuple[OpenEOJobRecord, bool]:
        creating.set()
        time.sleep(0.2)  # long enough for the second update to look while the first creates
        return create(*args, **kwargs)

    monkeypatch.setattr(instance.openeo, "create_triggered_job", slow_create)
    first = threading.Thread(target=instance.arrive, args=(_update(1),))
    first.start()
    assert creating.wait(5)
    instance.arrive(_update(2))
    first.join(5)

    assert not first.is_alive()
    assert [job.status for job in instance.openeo.jobs.values()] == [OpenEOJobStatus.QUEUED, OpenEOJobStatus.CREATED]


def test_a_refresh_left_unstarted_waits_while_another_runs(instance: _Instance) -> None:
    """A process that stopped between creating and starting a refresh leaves it CREATED; the
    maintenance pass starts it, but not beside a refresh another process queued since."""
    instance.created.add("rain_anomaly")
    start = instance.openeo.start_triggered_job
    instance.openeo.start_triggered_job = lambda job_id: False  # stopped before starting it
    instance.arrive(_update(1))
    [left] = instance.openeo.jobs.values()
    instance.openeo.start_triggered_job = start
    other = left.model_copy(update={"id": "queued-elsewhere", "status": OpenEOJobStatus.QUEUED})
    instance.openeo.jobs[other.id] = other

    derivations.start_waiting()
    assert instance.openeo.jobs[left.id].status == OpenEOJobStatus.CREATED
    instance.openeo.set(other.id, OpenEOJobStatus.FINISHED)
    derivations.start_waiting()
    assert instance.openeo.jobs[left.id].status == OpenEOJobStatus.QUEUED


def test_one_failing_consumer_does_not_keep_events_from_the_others(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[str] = []

    def fail(_: list[JobEvent]) -> None:
        raise RuntimeError("broken")

    monkeypatch.setattr(
        dispatch,
        "_consumers",
        lambda: [("first", fail), ("second", lambda events: received.extend(e.event_id for e in events))],
    )

    dispatch.consume([_update(1)])

    assert received == ["rain-1:0"]


def test_an_input_update_refreshes_the_derived_dataset_and_its_pipeline_hears_of_it_across_a_restart(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real job service and publication; only the graph's computed result is stubbed.

    Stops where the pipeline runner receives the derived dataset's update: that an update starts
    one run, and that a run delivers once, are `test_a_dataset_update_runs_the_same_path_once`
    and `test_run_once_stores_its_intent_first_and_delivers_once` in test_pipelines.py.
    """
    first_process = request.getfixturevalue("service")
    from open_geo_service.jobs import store as native_store
    from open_geo_service.jobs.models import JobRecord, JobStatus
    from open_geo_service.pipelines import runner
    from open_geo_service.shared.time import utc_now

    recipe = replace(_ANOMALY, output=_DATASET)
    monkeypatch.setattr(derivations, "list_recipes", lambda: [recipe])
    running = [first_process]
    monkeypatch.setattr(openeo_jobs, "get_openeo_job_service", lambda: running[0])
    heard: list[str] = []
    monkeypatch.setattr(
        runner,
        "consume",
        lambda events: heard.extend(e.data["dataset_id"] for e in events if e.type == "dataset.updated"),
    )

    def started(openeo: Any) -> None:
        openeo.set_event_consumer(dispatch.consume)
        openeo.set_finished_listener(derivations.on_job_finished)

    def synced(number: int) -> JobEvent:
        """The input's update, persisted with its sync job, after the run that created the dataset."""
        event = _update(number).model_copy(update={"time": utc_now()})
        native_store.create_job_record(
            JobRecord(
                job_id=f"sync-{number}",
                process_id="sync",
                status=JobStatus.SUCCESSFUL,
                created_at=utc_now(),
                events=[event],
            )
        )
        return event

    def refreshes(expected: int) -> list[OpenEOJobRecord]:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            done = [
                job
                for job in openeo_jobs.store_list_jobs()
                if job.trigger_id == f"derive:{_DATASET}" and job.status == OpenEOJobStatus.FINISHED
            ]
            if len(done) >= expected and heard.count(_DATASET) >= expected:
                return done
            time.sleep(0.05)
        pytest.fail(f"expected {expected} finished refreshes of {_DATASET}")

    started(first_process)
    _job("created-by-hand", 1.0, monkeypatch)  # the operator creates the derived dataset once
    first_process._execute("created-by-hand")
    heard.clear()

    dispatch.consume([synced(1)])
    [first] = refreshes(1)
    assert first.process["process_graph"] == workflows.freeze_process_graph(
        {
            "derive": {
                "process_id": "climate_anomaly",
                "arguments": {
                    **recipe.arguments,
                    "output_dataset_id": _DATASET,
                    "temporal_extent": ["2026-01", "2026-01"],
                },
                "result": True,
            }
        }
    )
    # The stubbed result's periods, written into the store the operator created.
    assert [event.data["action"] for event in first.events] == ["merge"]

    first_process.shutdown()
    synced(2)  # while no process runs
    restarted = openeo_jobs.OpenEOJobService()
    running[0] = restarted
    try:
        started(restarted)
        derivations.reconcile()  # at startup
        assert len(refreshes(2)) == 2
        # The pipelines hear the input's update, then each refresh's publication.
        assert heard == ["rain", _DATASET, _DATASET]
    finally:
        restarted.shutdown()
