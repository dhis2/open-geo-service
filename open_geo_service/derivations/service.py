"""Run a derived dataset's recipe again when the dataset it refreshes on updates.

Dataset maintenance owns derivation; pipelines own delivery. A recipe with ``refresh_on``
(`data_registry.recipes`) is computed again after each ``dataset.updated`` event of its driving
dataset, once for each range of periods the event lists in ``changes``. Each run is an openEO
batch job of the recipe's workflow over that range, which writes the periods it computed into
the derived dataset's store and keeps the others (``merge_publication``). That publication emits
the derived dataset's own update, listing those periods, so a pipeline or recipe on the derived
dataset reruns only them. A range computed on its own is right for a workflow whose output
periods depend only on input inside them, as every shipped recipe's do; CLIM-2052 is for
workflows that need more.

The whole history is computed again instead, replacing the store, when the recipe or the
workflow it runs changed since the last refresh, or for the update after a refresh that failed
or was cancelled, whose periods were then never written. An update that rewrote its input from
the start already lists the whole coverage.

Only a derived dataset that exists is kept current: one an operator created once. A built-in
recipe therefore costs nothing on an instance that never asked for its dataset. The run that
created it covers the updates before it.

Each refresh is one job whose id is derived from the driving update, its range (or ``full``),
the recipe's digest and the digest of the workflow definition it runs, frozen with its nested
workflows. The job carries that frozen graph, so it runs what was current when it was
submitted, and submitting it again finds the same job. An update is handled once a job names it.
Refreshes of one derived dataset run one at a time, since each writes the same store: the others
wait as created jobs and start in the order they were created, the next when one finishes.

`consume` handles the updates just persisted. `reconcile`, at startup, reads the event log once
for updates a stopped process missed: those after the newest refresh job, or after the dataset's
creation when it has none. `start_waiting`, on the maintenance interval, starts a refresh left
waiting, such as one queued behind a refresh that failed, without reading the log.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException

from open_geo_service import config as api_config
from open_geo_service.data_registry import recipes
from open_geo_service.data_registry.recipes import OUTPUT_PARAMETER, Recipe
from open_geo_service.features.bindings import feature_versions, resolve_feature_references
from open_geo_service.jobs.models import DATASET_UPDATED_EVENT_TYPE, JobEvent
from open_geo_service.openeo.schemas import OpenEOJobCreate, OpenEOJobRecord, OpenEOJobStatus, WorkflowRecord

logger = logging.getLogger(__name__)

TRIGGER_PREFIX = "derive:"
MAX_ATTEMPTS = 3
_ACTIVE = frozenset({OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING})


def trigger_id(output: str) -> str:
    """The trigger id every refresh job of ``output`` carries, which is how its jobs are found."""
    return f"{TRIGGER_PREFIX}{output}"


def _templates() -> list[dict[str, Any]]:
    from open_geo_service.data_registry.services import datasets

    return datasets.list_datasets()


def list_recipes(templates: list[dict[str, Any]] | None = None) -> list[Recipe]:
    """Every recipe the dataset templates declare; *templates* when the caller has read them already."""
    read = _templates() if templates is None else templates
    return [found for template in read if (found := recipes.recipe(template)) is not None]


# --- validation --------------------------------------------------------------------------------


def _publishes_output(record: WorkflowRecord) -> bool:
    """Whether the workflow's result saves a managed Zarr dataset named by ``output_dataset_id``."""
    for node in record.process_graph.values():
        if not isinstance(node, dict) or not node.get("result") or node.get("process_id") != "save_result":
            continue
        arguments = node.get("arguments") or {}
        options = arguments.get("options") or {}
        return str(arguments.get("format", "")).lower() == "zarr" and options.get("dataset_id") == {
            "from_parameter": OUTPUT_PARAMETER
        }
    return False


def recipe_problems(recipe: Recipe) -> list[str]:
    """Why the recipe cannot run against its registered workflow; empty when it can."""
    from open_geo_service.openeo import workflows as workflow_registry
    from open_geo_service.pipelines import workflow as binding

    record = workflow_registry.get_workflow(recipe.workflow)
    if record is None:
        return [f"'{recipe.workflow}' is not a registered workflow"]
    problems: list[str] = []
    declared = {str(p["name"]): p for p in binding.parameters(record)}
    if OUTPUT_PARAMETER not in declared or not _publishes_output(record):
        problems.append(f"workflow '{recipe.workflow}' does not publish a dataset named by {OUTPUT_PARAMETER}")
    for name, value in recipe.arguments.items():
        if name not in declared:
            problems.append(f"'{name}' is not a parameter of workflow '{recipe.workflow}'")
        elif (problem := binding.type_problem(name, binding.parameter_schema(declared[name]), value)) is not None:
            problems.append(problem)
    intervals = binding.interval_parameters(record)
    open_intervals = [name for name in intervals if name not in recipe.arguments]
    if len(intervals) != 1:
        problems.append(f"workflow '{recipe.workflow}' must declare exactly one temporal-interval parameter")
    elif recipe.refresh_on is not None and not open_intervals:
        problems.append(f"leave '{intervals[0]}' out: each refresh fills it with the periods the input changed")
    elif recipe.refresh_on is None and open_intervals:
        problems.append(f"set '{intervals[0]}': a recipe without refresh_on runs over a fixed range")
    supplied = {*recipe.arguments, OUTPUT_PARAMETER, *open_intervals}
    missing = sorted(name for name, p in declared.items() if binding.is_required(p) and name not in supplied)
    if missing:
        problems.append(f"missing argument(s) {', '.join(missing)}")
    return problems


def _cycles(found: list[Recipe]) -> list[str]:
    """The recipe outputs whose updates lead back to themselves through other recipes."""
    feeds: dict[str, set[str]] = {}
    for source, output in {(r.input, r.output) for r in found if r.input is not None}:
        feeds.setdefault(source, set()).add(output)

    def returns_to(start: str) -> bool:
        seen: set[str] = set()
        pending = list(feeds.get(start, ()))
        while pending:
            current = pending.pop()
            if current == start:
                return True
            if current not in seen:
                seen.add(current)
                pending.extend(feeds.get(current, ()))
        return False

    return sorted({recipe.output for recipe in found if recipe.input is not None and returns_to(recipe.output)})


def validate_recipes() -> None:
    """Refuse startup with every recipe that cannot run or loops."""
    found = list_recipes()
    problems = [f"'{recipe.output}': {problem}" for recipe in found for problem in recipe_problems(recipe)]
    known = {str(template["id"]) for template in _templates()}
    problems.extend(
        f"'{recipe.output}': refresh_on names '{recipe.input}', which is not a dataset template"
        for recipe in found
        if recipe.input is not None and recipe.input not in known
    )
    problems.extend(f"'{output}': its updates lead back to itself through recipes" for output in _cycles(found))
    if problems:
        raise ValueError("Derived dataset recipes cannot run:\n- " + "\n- ".join(problems))


# --- refreshing -------------------------------------------------------------------------------

_FULL = "full"
_FAILED = frozenset({OpenEOJobStatus.ERROR, OpenEOJobStatus.CANCELED})


@dataclass(frozen=True)
class _Source:
    """What a refresh job answers: an update, the range it computes or ``full``, and the definition it runs."""

    event_id: str
    span: str
    recipe_digest: str
    workflow_digest: str

    def __str__(self) -> str:
        return f"{self.event_id}:{self.span}:{self.recipe_digest}:{self.workflow_digest}"

    @classmethod
    def of(cls, job: OpenEOJobRecord) -> _Source | None:
        # From the right: an event id may hold colons, a period never does.
        parts = (job.source_event_id or "").rsplit(":", 3)
        return cls(*parts) if len(parts) == 4 else None


@dataclass(frozen=True)
class _Definition:
    """A recipe's workflow as a refresh submitted now runs it."""

    snapshot: WorkflowRecord
    digest: str
    interval: str


def _definition(recipe: Recipe) -> _Definition:
    from open_geo_service.openeo import workflows as workflow_registry
    from open_geo_service.pipelines import workflow as binding

    record = workflow_registry.get_workflow(recipe.workflow)
    if record is None:
        raise LookupError(f"workflow '{recipe.workflow}' is not registered")
    # One snapshot of the definition, nested workflows inlined, for the identity, the interval
    # and the submitted graph alike: the job runs what its id names, whatever is edited later.
    snapshot = binding.frozen(record)
    open_intervals = [name for name in binding.interval_parameters(snapshot) if name not in recipe.arguments]
    if len(open_intervals) != 1:
        # Checked at startup, but a workflow can be edited while OGS runs.
        raise ValueError(
            f"workflow '{recipe.workflow}' must leave exactly one temporal-interval parameter open, "
            f"not {open_intervals}"
        )
    return _Definition(snapshot, binding.definition_digest(snapshot), open_intervals[0])


def _exists(dataset_id: str) -> bool:
    from open_geo_service.ingestions.services import get_latest_artifact_for_dataset_or_404

    try:
        get_latest_artifact_for_dataset_or_404(dataset_id)
    except HTTPException as exc:
        if exc.status_code == 404:
            return False
        raise
    return True


def _is_update(event: JobEvent) -> bool:
    return event.type == DATASET_UPDATED_EVENT_TYPE and isinstance(event.data.get("dataset_id"), str)


def _jobs(output: str) -> list[OpenEOJobRecord]:
    """The refresh jobs of a derived dataset, oldest first."""
    from open_geo_service.openeo.jobs import store_list_jobs

    jobs = [job for job in store_list_jobs() if job.trigger_id == trigger_id(output)]
    return sorted(jobs, key=lambda job: (job.created, job.id))


def _full_due(jobs: list[OpenEOJobRecord], recipe: Recipe, definition: _Definition, *, after_failure: bool) -> bool:
    """Whether the next refresh must compute the whole history.

    Reads the jobs since the newest full refresh of the current definition that did not fail.
    One of another definition computed the stored periods differently; with ``after_failure``,
    one that failed or was cancelled left its range unwritten.
    """
    current = (recipe.digest, definition.digest)
    since = jobs
    for index in range(len(jobs) - 1, -1, -1):
        source = _Source.of(jobs[index])
        if (
            source is not None
            and source.span == _FULL
            and (source.recipe_digest, source.workflow_digest) == current
            and jobs[index].status not in _FAILED
        ):
            since = jobs[index + 1 :]
            break
    for job in since:
        source = _Source.of(job)
        if source is None or (source.recipe_digest, source.workflow_digest) != current:
            return True
        if after_failure and job.status in _FAILED:
            return True
    return False


def _create(
    recipe: Recipe, definition: _Definition, source: _Source, interval: list[str], *, merge: bool
) -> OpenEOJobRecord:
    """Create the refresh job ``source`` names, over ``interval``, without starting it."""
    from open_geo_service.openeo import workflows as workflow_registry
    from open_geo_service.openeo.jobs import get_openeo_job_service

    # A feature collection bound by id is loaded at the version current now, as a pipeline run does.
    feature_nodes: dict[str, Any] = {}
    arguments: dict[str, Any] = {
        **resolve_feature_references(recipe.arguments, feature_nodes, feature_versions(recipe.arguments)),
        OUTPUT_PARAMETER: recipe.output,
        definition.interval: interval,
    }
    call = {**feature_nodes, "derive": {"process_id": recipe.workflow, "arguments": arguments, "result": True}}
    scope = f"{interval[0]} to {interval[1]}" if merge else "its whole history"
    job, _ = get_openeo_job_service().create_triggered_job(
        OpenEOJobCreate(
            title=f"Refresh {recipe.output}: {scope}",
            description=(
                f"Refreshes derived dataset '{recipe.output}' ({scope}) with workflow '{recipe.workflow}' after "
                f"update {source.event_id} of '{recipe.input}' (recipe {recipe.digest[:12]}, "
                f"workflow {definition.digest[:12]})"
            ),
            process={
                "process_graph": workflow_registry.freeze_process_graph(
                    call, resolve=lambda name: definition.snapshot if name == recipe.workflow else None
                )
            },
        ),
        source_event_id=str(source),
        trigger_id=trigger_id(recipe.output),
        max_attempts=MAX_ATTEMPTS,
        merge_publication=merge,
    )
    return job


def _create_full(recipe: Recipe, definition: _Definition, update: JobEvent) -> list[OpenEOJobRecord]:
    coverage = [update.data.get("coverage_start"), update.data.get("coverage_end")]
    if not all(isinstance(value, str) for value in coverage):
        # An event recorded before CLIM-2029 renamed the coverage fields.
        logger.warning("Not refreshing %s: update %s does not name its coverage", recipe.output, update.event_id)
        return []
    source = _Source(update.event_id, _FULL, recipe.digest, definition.digest)
    return [_create(recipe, definition, source, [str(value) for value in coverage], merge=False)]


def _plan(
    recipe: Recipe, definition: _Definition, update: JobEvent, jobs: list[OpenEOJobRecord]
) -> list[OpenEOJobRecord]:
    """Create the refresh jobs one update of the input calls for: none once a job names it."""
    from open_geo_service.ingestions.processes import update_intervals

    if any((source := _Source.of(job)) is not None and source.event_id == update.event_id for job in jobs):
        return []
    if _full_due(jobs, recipe, definition, after_failure=True):
        return _create_full(recipe, definition, update)
    ranges = update_intervals(update.data)
    if not ranges:
        logger.warning("Not refreshing %s: update %s lists no changed periods", recipe.output, update.event_id)
    return [
        _create(
            recipe,
            definition,
            _Source(update.event_id, f"{start}/{end}", recipe.digest, definition.digest),
            [start, end],
            merge=True,
        )
        for start, end in ranges
    ]


def _start_next(jobs: list[OpenEOJobRecord]) -> None:
    """Start the oldest waiting refresh, unless one is queued or running."""
    from open_geo_service.openeo.jobs import get_openeo_job_service

    if any(job.status in _ACTIVE for job in jobs):
        return  # its completion starts the next
    waiting = next((job for job in jobs if job.status == OpenEOJobStatus.CREATED), None)
    if waiting is not None and get_openeo_job_service().start_triggered_job(waiting.id):
        logger.info("Refreshing derived dataset as job %s (%s)", waiting.id, waiting.title)


def _refresh(recipe: Recipe, updates: list[JobEvent], *, since: datetime | None = None) -> list[OpenEOJobRecord]:
    """Create the refreshes these updates of the input call for, start the next one, and return those created.

    ``since`` is for `reconcile`: updates before it count as handled, and a recipe or workflow
    changed with no update to answer still gets its full refresh.

    Reading the jobs, creating the next ones and starting one happen under one lock per derived
    dataset, shared by every process on the data directory, so two callers handling the same
    update create its jobs once and start one refresh between them.
    """
    from open_geo_service.shared.persistence import index_lock

    with index_lock(api_config.get_data_root() / "derivations" / recipe.output):
        if recipe.input is None or not _exists(recipe.output):
            return []
        definition = _definition(recipe)
        jobs = _jobs(recipe.output)
        created: list[OpenEOJobRecord] = []
        for update in updates:
            if since is not None and _utc(update.time) < since:
                continue
            new = _plan(recipe, definition, update, jobs)
            jobs.extend(new)
            created.extend(new)
        if since is not None and not created and updates and _full_due(jobs, recipe, definition, after_failure=False):
            # The recipe or the workflow changed while no update arrived.
            new = _create_full(recipe, definition, updates[-1])
            jobs.extend(new)
            created.extend(new)
        _start_next(jobs)
        return created


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def consume(events: list[JobEvent]) -> None:
    """Refresh the derived datasets whose driving dataset these events updated."""
    updates = [event for event in events if _is_update(event)]
    if not updates or api_config.is_read_only():
        return
    for recipe in list_recipes():
        mine = [event for event in updates if event.data["dataset_id"] == recipe.input]
        if not mine:
            continue
        try:
            _refresh(recipe, mine)
        except Exception:
            logger.exception("Derived dataset %s could not be refreshed", recipe.output)


def reconcile() -> None:
    """At startup, refresh each derived dataset for the updates of its input a stopped process missed.

    Reads the event log once. Updates before the newest refresh job were handled when they
    arrived, and those before the dataset was created are covered by the run that created it.
    An update stamped before the read but persisted after it can only come from a native job
    still unfinished, so the bound moves back to the oldest of those.
    """
    from open_geo_service.jobs.events import read_event_log

    if api_config.is_read_only():
        return
    found = [recipe for recipe in list_recipes() if recipe.input is not None]
    if not found:
        return
    log = read_event_log()
    for recipe in found:
        updates = [event for event in log.events if _is_update(event) and event.data["dataset_id"] == recipe.input]
        try:
            jobs = _jobs(recipe.output)
            created = [
                _utc(event.time)
                for event in log.events
                if _is_update(event) and event.data["dataset_id"] == recipe.output
            ]
            since = jobs[-1].created if jobs else (created[0] if created else None)
            if since is None:
                continue  # never published, so nothing to keep current
            since = _utc(since) if log.open_since is None else min(_utc(since), _utc(log.open_since))
            _refresh(recipe, updates, since=since)
        except Exception:
            logger.exception("Derived dataset %s could not be reconciled", recipe.output)


def start_waiting() -> None:
    """Start the next waiting refresh of each derived dataset that has none running. Reads no event log."""
    from open_geo_service.shared.persistence import index_lock

    if api_config.is_read_only():
        return
    for recipe in list_recipes():
        if recipe.input is None:
            continue
        try:
            with index_lock(api_config.get_data_root() / "derivations" / recipe.output):
                _start_next(_jobs(recipe.output))
        except Exception:
            logger.exception("Derived dataset %s could not start its next refresh", recipe.output)


def on_job_finished(job: OpenEOJobRecord) -> None:
    """Start the refresh waiting behind one that just finished. Never raises."""
    from open_geo_service.shared.persistence import index_lock

    trigger = job.trigger_id
    if not isinstance(trigger, str) or not trigger.startswith(TRIGGER_PREFIX):
        return
    output = trigger.removeprefix(TRIGGER_PREFIX)
    try:
        with index_lock(api_config.get_data_root() / "derivations" / output):
            _start_next(_jobs(output))
    except Exception:
        logger.exception("Derived dataset %s could not start its next refresh", output)
