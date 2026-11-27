"""Shared utility for loading and instantiating streaming plugin classes."""

from __future__ import annotations

import functools
import importlib
import inspect
import logging
from typing import Any, cast

from open_geo_service.streaming.protocol import IngestionPlugin, StaticIngestionPlugin

logger = logging.getLogger(__name__)


class PluginLoadError(Exception):
    """A plugin class cannot be loaded or cannot serve its template; the message is safe to show."""


def instantiate_plugin(plugin_path: str, default_params: dict[str, Any]) -> IngestionPlugin:
    """Load and instantiate a streaming plugin class from a dotted import path.

    Filters constructor kwargs to only those the class accepts, so templates
    can include extra keys in ``ingestion.default_params`` without breaking
    plugins that do not declare ``**kwargs``. Raises `PluginLoadError` when the class
    cannot be imported or does not implement ``periods`` and ``fetch_period``.
    """
    return cast(IngestionPlugin, _instantiate(plugin_path, default_params, static=False))


def instantiate_static_plugin(plugin_path: str, default_params: dict[str, Any]) -> StaticIngestionPlugin:
    """Load and instantiate the plugin of a static dataset, which needs only ``fetch``."""
    return cast(StaticIngestionPlugin, _instantiate(plugin_path, default_params, static=True))


def plugin_load_error(template: dict[str, Any]) -> str | None:
    """Why the ingestion plugin *template* names cannot be loaded, or None when it can or it names none.

    The class is imported, not instantiated, and checked against what ingestion will ask of it:
    `BaseTablePlugin` for a table template, ``fetch`` for a raster template without a time axis,
    and ``periods`` and ``fetch_period`` for one with. The reason names the kind of failure,
    not the exception, which can carry file paths; the exception is logged, so an operator finds
    a typo or a missing package from the listing rather than a failed ingest.

    Each plugin path is checked once per process for each kind of template. A module is not
    imported again after a failure, so a fixed plugin needs a restart, as a newly installed
    package does; a template pointed at another plugin is checked afresh.
    """
    from open_geo_service.data_registry.services.datasets import is_timeless
    from open_geo_service.tables.templates import is_table_template

    ingestion = template.get("ingestion")
    plugin_path = str(ingestion.get("plugin") or "").strip() if isinstance(ingestion, dict) else ""
    if not plugin_path:
        return None
    kind = "table" if is_table_template(template) else "static" if is_timeless(template) else "periodic"
    return _plugin_error(plugin_path, kind)


@functools.cache
def _plugin_error(plugin_path: str, kind: str) -> str | None:
    from open_geo_service.tables.plugin import BaseTablePlugin

    try:
        plugin_class = _load_class(plugin_path)
        if kind == "table":
            if not (isinstance(plugin_class, type) and issubclass(plugin_class, BaseTablePlugin)):
                raise PluginLoadError(f"its plugin '{plugin_path}' is not a BaseTablePlugin")
        else:
            _check_raster_class(plugin_class, plugin_path, static=kind == "static")
    except PluginLoadError as exc:
        logger.warning("Data sources naming '%s' cannot be ingested: %s", plugin_path, exc)
        return str(exc)
    return None


def _load_class(plugin_path: str) -> Any:
    """Import the object *plugin_path* names, raising `PluginLoadError` with a reason safe to show."""
    module_path, _, name = plugin_path.rpartition(".")
    if not module_path or not name:
        raise PluginLoadError(f"its plugin path '{plugin_path}' is not a dotted module path")
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise PluginLoadError(f"its plugin '{plugin_path}' cannot be imported: no module named '{exc.name}'") from exc
    except Exception as exc:
        # The exception can carry file paths, so it is logged rather than shown.
        logger.warning("Plugin '%s' raised while importing", plugin_path, exc_info=True)
        raise PluginLoadError(
            f"its plugin '{plugin_path}' cannot be imported: the module raised an error (see the server log)"
        ) from exc
    # A module may export its plugin lazily through a module-level __getattr__, which can raise
    # anything; only an AttributeError means the name is not there.
    try:
        return getattr(module, name)
    except AttributeError as exc:
        raise PluginLoadError(
            f"its plugin '{plugin_path}' cannot be imported: '{module_path}' has no '{name}'"
        ) from exc
    except Exception as exc:
        logger.warning("Plugin '%s' raised while being looked up", plugin_path, exc_info=True)
        raise PluginLoadError(
            f"its plugin '{plugin_path}' cannot be imported: "
            "the module raised an error looking it up (see the server log)"
        ) from exc


def _check_raster_class(plugin_class: Any, plugin_path: str, *, static: bool) -> None:
    """Raise `PluginLoadError` unless *plugin_class* implements what its raster template uses.

    ``fetch`` for a template without a time axis, ``periods`` and ``fetch_period`` for one with.
    A method still inherited from `BaseDatasetPlugin` counts as missing: there it only raises
    NotImplementedError, so a subclass that has it unchanged fails at the first call.
    """
    from open_geo_service.streaming.base import BaseDatasetPlugin

    if not isinstance(plugin_class, type):
        raise PluginLoadError(f"its plugin '{plugin_path}' is not a class")
    required = ("fetch",) if static else ("periods", "fetch_period")
    missing = [
        method
        for method in required
        if not callable(getattr(plugin_class, method, None))
        or getattr(plugin_class, method) is getattr(BaseDatasetPlugin, method)
    ]
    if missing:
        kind = "a data source without a time axis" if static else "a data source with a time axis"
        names = " and ".join(f"{method}()" for method in missing)
        raise PluginLoadError(f"its plugin '{plugin_path}' does not implement {names}, which {kind} needs")


def _instantiate(plugin_path: str, default_params: dict[str, Any], *, static: bool) -> Any:
    PluginClass = _load_class(plugin_path)
    _check_raster_class(PluginClass, plugin_path, static=static)
    sig = inspect.signature(PluginClass)
    accepts_var_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    kwargs = default_params if accepts_var_kwargs else {k: v for k, v in default_params.items() if k in sig.parameters}
    return PluginClass(**kwargs)
