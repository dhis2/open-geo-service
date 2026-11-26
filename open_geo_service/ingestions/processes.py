"""Process execution wrappers for ingestion and sync operations."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

from open_geo_service.extents import services as extent_services
from open_geo_service.ingestions import services
from open_geo_service.ingestions.schemas import ArtifactRecord, SyncAction, SyncResponse
from open_geo_service.jobs.models import DATASET_UPDATED_EVENT_TYPE, JobEventDraft, JobExecutionResult

logger = logging.getLogger(__name__)

INGEST_EVENT_ACTION = "ingest"
FULL_REWRITE_EVENT_ACTION = "full_rewrite"
"""`action` of an update that replaced the whole stored dataset, such as an openEO publication.

Its one change is the whole coverage: every period may have changed."""
"""`action` of the update event an ingestion emits; syncs report their planner action instead."""

_UPDATE_MARKER = "dataset_update_planned"
"""Cursor key recording that an attempt of this job planned a change to stored data.

Written before anything is fetched. A recovered job whose earlier attempt committed its data
and died before completing finds the dataset already current, so without this marker it would
succeed without an event and the configured workflows would never run for that ingestion.
"""


def dataset_update_event(
    *,
    dataset_id: str,
    artifact_id: str | None,
    action: str,
    changes: list[tuple[str, str]],
    coverage_start: str | None,
    coverage_end: str | None,
    producer_type: str | None = None,
    producer_job_id: str | None = None,
    output_snapshot_id: str | None = None,
) -> JobEventDraft:
    """Describe one change to a managed dataset, for the derived datasets and pipelines that follow it.

    `changes` lists the ranges of periods the change wrote, first and last period of each, in
    the order they were planned and never joined; a whole-store rewrite lists its whole
    coverage. `coverage_start` and `coverage_end` describe the coverage after the change.
    `action` says how the store changed, not who changed it.

    The producer fields are optional and only present when given, so an ingestion's or a
    sync's event keeps its shape: `producer_type` (for example ``openeo``), the producing
    job's id, and the Icechunk snapshot the change committed.
    """
    data: dict[str, Any] = {
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "action": action,
        "changes": [{"start": start, "end": end} for start, end in changes],
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
    }
    for key, value in (
        ("producer_type", producer_type),
        ("producer_job_id", producer_job_id),
        ("output_snapshot_id", output_snapshot_id),
    ):
        if value is not None:
            data[key] = value
    return JobEventDraft(type=DATASET_UPDATED_EVENT_TYPE, source=f"/datasets/{dataset_id}", data=data)


def ingest_dataset(
    *,
    dataset: dict[str, Any],
    start: str | None = None,
    end: str | None = None,
    overwrite: bool = False,
    publish: bool = True,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
) -> tuple[ArtifactRecord, list[JobEventDraft]]:
    """Ingest a dataset over the configured extent and describe any change it made.

    Returns the update event only when stored data changed, so re-ingesting a dataset that
    is already current triggers nothing. Inside a job, the planned change is also recorded
    in the job cursor, so a recovered attempt still emits the event its earlier attempt owed.
    """
    extent = extent_services.get_extent_or_404()
    cursor = load_cursor() if load_cursor is not None else None
    earlier = cursor.get(_UPDATE_MARKER) if isinstance(cursor, dict) else None
    # An earlier attempt's marker wins: it saw the coverage before any of this job's commits.
    marker = earlier if isinstance(earlier, dict) else None

    def planned(previous_end: str | None) -> None:
        nonlocal marker
        if marker is None:
            marker = {"previous_end": previous_end}
            if save_cursor is not None:
                save_cursor({**(cursor or {}), _UPDATE_MARKER: marker})

    def save(checkpoint: dict[str, Any]) -> None:
        # The streaming ingest replaces the whole cursor on each commit; keep the marker.
        if save_cursor is not None:
            save_cursor({**checkpoint, _UPDATE_MARKER: marker} if marker is not None else checkpoint)

    artifact = services.create_artifact(
        dataset=dataset,
        start=start,
        end=end,
        bbox=list(extent["bbox"]),
        country_code=extent.get("country_code"),
        overwrite=overwrite,
        publish=publish,
        on_progress=on_progress,
        is_cancel_requested=is_cancel_requested,
        save_cursor=save if save_cursor is not None else None,
        on_update_planned=planned,
    )
    if marker is None:
        return artifact, []
    temporal = artifact.coverage.temporal
    event = dataset_update_event(
        dataset_id=str(dataset["id"]),
        artifact_id=artifact.artifact_id,
        action=INGEST_EVENT_ACTION,
        changes=written_since(marker.get("previous_end"), temporal.start, temporal.end),
        coverage_start=temporal.start,
        coverage_end=temporal.end,
    )
    return artifact, [event]


def written_since(previous_end: str | None, start: str | None, end: str | None) -> list[tuple[str, str]]:
    """The range a write changed, from the coverage end before it: None means the whole coverage.

    A write after ``previous_end`` is listed from ``previous_end`` itself, re-sending the last
    period already stored, so an overwrite at a destination completes it.
    """
    first = previous_end if previous_end is not None else start
    return [(first, end)] if first is not None and end is not None else []


def sync_update_events(dataset_id: str, response: SyncResponse) -> list[JobEventDraft]:
    """Describe the change a completed sync made, if it wrote data."""
    detail = response.sync_detail
    if (
        response.status != "completed"
        or detail is None
        or detail.action
        not in {
            SyncAction.APPEND,
            SyncAction.REMATERIALIZE,
        }
    ):
        return []
    if detail.action == SyncAction.REMATERIALIZE:
        # A rematerialization may rewrite every historical value.
        changes = written_since(None, detail.current_start, detail.target_end)
    else:
        # An append lists the preliminary periods it replaced with final values (CLIM-931) and
        # the new periods it added, as two ranges: the stored periods between them did not change.
        changes = []
        if detail.refresh_start is not None and detail.refresh_end is not None:
            changes.append((detail.refresh_start, detail.refresh_end))
        if detail.delta_start is not None:
            changes += written_since(detail.current_end, detail.current_start, detail.target_end)
    return [
        dataset_update_event(
            dataset_id=dataset_id,
            artifact_id=response.sync_id,
            action=detail.action.value,
            changes=changes,
            coverage_start=detail.current_start,
            coverage_end=detail.target_end,
        )
    ]


def record_inline_update(*, label: str, request: dict[str, Any], result: Any, events: list[JobEventDraft]) -> None:
    """Make the events of work done inline durable, as a completed job, for automation.

    Logged rather than raised on failure: the data is already committed, so failing the
    request would misreport it. The update simply does not reach automation.
    """
    if not events:
        return
    from open_geo_service.ingestions.job_submission import INGESTION_JOB_HREF_BASE
    from open_geo_service.jobs.service import get_job_service

    try:
        get_job_service().record_completed_job(
            label=label,
            request=request,
            result=result,
            events=events,
            job_href_base=INGESTION_JOB_HREF_BASE,
        )
    except Exception:
        logger.exception("Could not record the dataset update from %s; automation will not see it", label)


def execute_ingestion(
    *,
    dataset_id: str,
    start: str | None = None,
    end: str | None = None,
    overwrite: bool = False,
    publish: bool = True,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
) -> JobExecutionResult:
    """Execute one managed-dataset ingestion as a job, emitting `dataset.updated` on change."""
    dataset = services.get_ingestable_template(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    artifact, events = ingest_dataset(
        dataset=dataset,
        start=start,
        end=end,
        overwrite=overwrite,
        publish=publish,
        on_progress=on_progress,
        is_cancel_requested=is_cancel_requested,
        save_cursor=save_cursor,
        load_cursor=load_cursor,
    )
    return JobExecutionResult(result=services.get_ingestion_or_404(artifact.artifact_id), events=events)


def execute_sync(
    *,
    dataset_id: str,
    end: str | None = None,
    publish: bool = True,
) -> JobExecutionResult:
    """Execute one sync and describe any resulting dataset update."""
    response = services.sync_dataset(
        dataset_id=dataset_id,
        end=end,
        publish=publish,
    )
    return JobExecutionResult(result=response, events=sync_update_events(dataset_id, response))
