"""Shared utility for loading and instantiating streaming plugin classes."""

from __future__ import annotations

import inspect
import logging
from typing import Any

from open_geo_service.shared.dynamic_import import get_dynamic_function
from open_geo_service.streaming.protocol import IngestionPlugin, StaticIngestionPlugin

logger = logging.getLogger(__name__)


def instantiate_plugin(plugin_path: str, default_params: dict[str, Any]) -> IngestionPlugin:
    """Load and instantiate a streaming plugin class from a dotted import path.

    Filters constructor kwargs to only those the class accepts, so templates
    can include extra keys in ``ingestion.default_params`` without breaking
    plugins that do not declare ``**kwargs``.
    """
    instance = _instantiate(plugin_path, default_params)
    if not isinstance(instance, IngestionPlugin):
        raise TypeError(f"'{plugin_path}' does not implement the IngestionPlugin protocol")
    return instance


def instantiate_static_plugin(plugin_path: str, default_params: dict[str, Any]) -> StaticIngestionPlugin:
    """Load and instantiate the plugin of a static dataset, which needs only ``fetch``."""
    instance = _instantiate(plugin_path, default_params)
    if not isinstance(instance, StaticIngestionPlugin):
        raise TypeError(f"'{plugin_path}' does not implement the StaticIngestionPlugin protocol")
    return instance


def plugin_load_error(template: dict[str, Any]) -> str | None:
    """Why the ingestion plugin *template* names cannot be loaded, or None when it can or it names none.

    The class is imported, not instantiated, and checked against what ingestion will ask of it:
    `BaseTablePlugin` for a table template, the `StaticIngestionPlugin` protocol for a raster
    template without a time axis and `IngestionPlugin` for one with. A failure is logged, so an
    operator finds a typo or a missing package from the listing rather than a failed ingest.
    """
    ingestion = template.get("ingestion")
    plugin_path = str(ingestion.get("plugin") or "").strip() if isinstance(ingestion, dict) else ""
    if not plugin_path:
        return None
    reason = _class_error(template, plugin_path)
    if reason is not None:
        logger.warning("Data source '%s' cannot be ingested: %s", template.get("id"), reason)
    return reason


def _class_error(template: dict[str, Any], plugin_path: str) -> str | None:
    from open_geo_service.data_registry.services.datasets import is_timeless
    from open_geo_service.tables.plugin import BaseTablePlugin
    from open_geo_service.tables.templates import is_table_template

    try:
        plugin_class = get_dynamic_function(plugin_path)
    except Exception as exc:
        return f"its plugin '{plugin_path}' cannot be imported ({type(exc).__name__}: {exc})"
    if is_table_template(template):
        expected: type = BaseTablePlugin
    else:
        expected = StaticIngestionPlugin if is_timeless(template) else IngestionPlugin
    if not (isinstance(plugin_class, type) and issubclass(plugin_class, expected)):
        return f"its plugin '{plugin_path}' is not a {expected.__name__}"
    return None


def _instantiate(plugin_path: str, default_params: dict[str, Any]) -> Any:
    PluginClass = get_dynamic_function(plugin_path)
    sig = inspect.signature(PluginClass)
    accepts_var_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    kwargs = default_params if accepts_var_kwargs else {k: v for k, v in default_params.items() if k in sig.parameters}
    return PluginClass(**kwargs)
