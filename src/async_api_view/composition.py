"""Composition root for the local SQLite, Databricks worker, and web UI."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from fastapi import FastAPI

from async_api_view.adapters.databricks import (
    CAPABILITIES,
    CliRunner,
    CommandRejected,
    DatabricksWorker,
    LifecyclePersistenceFailure,
    ResolvedTarget,
)
from async_api_view.application import DurableCoordinator, SystemBootstrapService
from async_api_view.config import (
    PLACEHOLDER_AUTHORITY_FINGERPRINT,
    ConfigError,
    ProjectSettings,
    canonical_config_id,
)
from async_api_view.contracts import (
    ActionState,
    AdapterAction,
    ConnectionBinding,
    IntentScopeState,
    KnowledgeState,
    PresenceState,
    RefreshCoverage,
    RefreshIntent,
    RefreshOrigin,
    RefreshScope,
    RemoteObject,
    TargetKind,
    TargetRef,
)
from async_api_view.ingestion import SQLiteObservationIngestor
from async_api_view.storage import (
    MIN_WRITE_RESERVE_BYTES,
    ActionActivityRecord,
    ActionAttemptRecord,
    FacetActionStatusRecord,
    FacetEvidenceRecord,
    OperationalEventRecord,
    SQLiteStore,
    SystemRecord,
)
from async_api_view.web import (
    ActionActivityView,
    ActionAttemptView,
    ActionDetailView,
    ActionHistoryQuery,
    ActionHistoryView,
    ActionSystemOption,
    ActivityView,
    AlertHistoryQuery,
    AlertHistoryView,
    DashboardQuery,
    DashboardView,
    FacetView,
    IntentScopeView,
    IntentView,
    LocalCallerAuthorizer,
    ObjectDetailQuery,
    ObjectDetailView,
    ObjectView,
    OperationalEventView,
    RefreshOption,
    RefreshRequest,
    RelatedObjectView,
    SystemView,
    create_app,
)

logger = logging.getLogger(__name__)

_TERMINAL_SCOPE_STATES = {
    IntentScopeState.SATISFIED,
    IntentScopeState.REJECTED,
    IntentScopeState.EXPIRED,
    IntentScopeState.CANCELLED,
}
_TERMINAL_ACTION_STATES = {
    ActionState.SATISFIED,
    ActionState.SUCCEEDED,
    ActionState.PARTIAL,
    ActionState.FAILED,
    ActionState.CANCELLED,
}


class SQLiteDatabricksTargetResolver:
    """Resolve only canonical/configured targets into adapter-local names."""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    @staticmethod
    def _payload_name(payload: Mapping[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def _facet_payload(self, object_id: str, facet: str) -> Mapping[str, Any]:
        state = self._store.get_facet_sync(object_id, facet)
        return state.payload if state is not None else {}

    async def resolve(
        self,
        *,
        action: AdapterAction,
        binding: ConnectionBinding,
    ) -> ResolvedTarget:
        if action.target.kind is TargetKind.CONFIGURED_SCOPE:
            scope = self._store.get_configured_scope(action.target.target_id)
            if scope is None or not scope.enabled or scope.system_id != action.system_id:
                raise CommandRejected("configured target is unavailable")
            if action.capability_key == "databricks.workspace.children.read":
                configured_object = (
                    self._store.get_object_sync(scope.object_id)
                    if scope.object_id is not None
                    else None
                )
                return ResolvedTarget(
                    workspace_path=scope.display_name,
                    workspace_root=str(binding.non_secret_settings.get("workspace_root", "")),
                    display_name=scope.display_name,
                    canonical_object_id=scope.object_id,
                    canonical_object_type=scope.object_type,
                    canonical_parent_external_key=(
                        configured_object.external_key if configured_object is not None else None
                    ),
                )
            if action.capability_key == "databricks.uc.catalogs.read":
                return ResolvedTarget(
                    display_name=scope.display_name,
                    canonical_object_id=scope.object_id,
                    canonical_object_type=scope.object_type,
                )
            raise CommandRejected("capability does not support this configured target")

        if action.target.kind is not TargetKind.OBJECT:
            raise CommandRejected("unsupported Databricks target kind")
        remote_object = self._store.get_object_sync(action.target.target_id)
        if remote_object is None or remote_object.system_id != action.system_id:
            raise CommandRejected("canonical target is unavailable")
        if remote_object.presence is PresenceState.ABSENT:
            raise CommandRejected("canonical target is absent")

        if action.capability_key.startswith("databricks.workspace."):
            payload = self._facet_payload(remote_object.object_id, "metadata")
            path = self._payload_name(payload, "path")
            if path is None and remote_object.external_key.startswith("workspace-path:"):
                path = remote_object.external_key.removeprefix("workspace-path:")
            if path is None and remote_object.external_key.startswith("workspace:/"):
                path = remote_object.external_key.removeprefix("workspace:")
            if path is None:
                raise CommandRejected("Workspace target has no observed path")
            return ResolvedTarget(
                workspace_path=path,
                workspace_root=str(binding.non_secret_settings.get("workspace_root", "")),
                display_name=remote_object.display_name,
                canonical_object_id=remote_object.object_id,
                canonical_object_type=remote_object.object_type,
                canonical_parent_external_key=remote_object.external_key,
            )

        if action.capability_key == "databricks.uc.schemas.read":
            if not remote_object.external_key.startswith("catalog:"):
                raise CommandRejected("catalog target has no canonical name")
            catalog_name = remote_object.external_key.removeprefix("catalog:")
            if not catalog_name:
                raise CommandRejected("catalog target has no canonical name")
            return ResolvedTarget(
                catalog_name=catalog_name,
                display_name=remote_object.display_name,
                canonical_object_id=remote_object.object_id,
                canonical_object_type=remote_object.object_type,
            )
        if action.capability_key in {
            "databricks.uc.relations.read",
            "databricks.uc.volumes.read",
        }:
            if (
                remote_object.source_kind != "databricks.uc.schema"
                or not remote_object.external_key.startswith("schema:")
            ):
                raise CommandRejected("schema target has no canonical full name")
            if remote_object.external_key.startswith("schema:schema_id:"):
                parent = self._store.get_present_parent_sync(remote_object.object_id)
                if (
                    parent is None
                    or parent.source_kind != "databricks.uc.catalog"
                    or not parent.external_key.startswith("catalog:")
                ):
                    raise CommandRejected("schema target has no unique canonical catalog")
                catalog_name = parent.external_key.removeprefix("catalog:")
                schema_name = remote_object.display_name
            else:
                full_name = remote_object.external_key.removeprefix("schema:")
                parts = full_name.split(".")
                if len(parts) != 2 or not all(parts):
                    raise CommandRejected("schema full name is not catalog.schema")
                catalog_name, schema_name = parts
            if not catalog_name or not schema_name:
                raise CommandRejected("schema full name is not catalog.schema")
            return ResolvedTarget(
                catalog_name=catalog_name,
                schema_name=schema_name,
                display_name=remote_object.display_name,
                canonical_object_id=remote_object.object_id,
                canonical_object_type=remote_object.object_type,
            )
        raise CommandRejected("capability does not support this object target")


class SQLiteWebBackend:
    """Translate concrete local state into presentation-only view models."""

    def __init__(
        self,
        store: SQLiteStore,
        *,
        worker_status: Callable[[], tuple[bool, str | None]],
        wake_worker: Callable[[], None],
    ) -> None:
        self._store = store
        self._worker_status = worker_status
        self._wake_worker = wake_worker

    def _refresh_status(self) -> tuple[bool, str | None]:
        worker_available, worker_error = self._worker_status()
        if not worker_available:
            return False, worker_error or "Refresh worker is unavailable."
        if not self._store.write_headroom_available():
            return False, self._store.write_headroom_error
        return True, None

    def _facet_view(
        self,
        *,
        system_id: str,
        object_id: str,
        object_type: str,
        evidence: FacetEvidenceRecord,
        last_action: FacetActionStatusRecord | None,
    ) -> FacetView:
        facet = evidence.facet
        interval_text = "Unknown"
        freshness = "unobserved"
        if facet.knowledge is KnowledgeState.UNSUPPORTED:
            freshness = "unsupported"
        elif facet.observed_at is not None:
            scope = RefreshScope(
                system_id=system_id,
                target=TargetRef(TargetKind.OBJECT, object_id),
                object_type=object_type,
                facet=facet.facet,
            )
            try:
                interval = self._store.effective_interval(scope)
                interval_text = str(interval)
                freshness = "current" if datetime.now(UTC) < facet.observed_at + interval else "due"
            except ValueError:
                freshness = "unsupported"
        active_states = {
            ActionState.READY.value,
            ActionState.LEASED.value,
            ActionState.RUNNING.value,
            ActionState.RETRY_WAIT.value,
        }
        if last_action is not None and last_action.state in active_states:
            freshness = "refreshing"
        elif (
            last_action is not None
            and last_action.state == ActionState.FAILED.value
            and (facet.observed_at is None or last_action.occurred_at > facet.observed_at)
        ):
            freshness = "failed"
        value = json.dumps(dict(facet.payload), ensure_ascii=False, sort_keys=True, default=str)
        provenance_parts: list[str] = []
        if evidence.adapter_key is not None:
            provenance_parts.append(
                f"{evidence.adapter_key} adapter"
                + (f" v{evidence.adapter_version}" if evidence.adapter_version else "")
            )
        if evidence.capability_key is not None:
            provenance_parts.append(
                evidence.capability_key
                + (f" v{evidence.capability_version}" if evidence.capability_version else "")
            )
        return FacetView(
            name=facet.facet,
            knowledge=facet.knowledge.value,
            value=value,
            known_as_of=facet.observed_at,
            freshness=freshness,
            effective_interval=interval_text,
            provenance=" · ".join(provenance_parts) or "Unknown source",
            provenance_observation_id=evidence.observation_id,
            provenance_action_id=evidence.action_id,
            last_action_id=last_action.action_id if last_action is not None else None,
            failure=(
                last_action.redacted_diagnostic
                if last_action is not None and freshness == "failed"
                else None
            ),
        )

    def _object_view(
        self,
        remote_object: RemoteObject,
        *,
        latest_facet_actions: Mapping[tuple[str, str, str], FacetActionStatusRecord],
    ) -> ObjectView:
        object_id = str(remote_object.object_id)
        system_id = str(remote_object.system_id)
        facet_evidence = self._store.list_facet_evidence(object_id)
        facets = tuple(evidence.facet for evidence in facet_evidence)
        facet_views = tuple(
            self._facet_view(
                system_id=system_id,
                object_id=object_id,
                object_type=remote_object.object_type,
                evidence=evidence,
                last_action=latest_facet_actions.get((system_id, object_id, evidence.facet.facet)),
            )
            for evidence in facet_evidence
        )
        path = next(
            (
                candidate
                for facet in facets
                if isinstance((candidate := facet.payload.get("path")), str)
            ),
            "",
        )
        return ObjectView(
            object_id=object_id,
            system_id=system_id,
            name=remote_object.display_name,
            object_type=remote_object.object_type,
            object_type_version=remote_object.object_type_version,
            source_kind=remote_object.source_kind,
            path=path,
            presence=remote_object.presence.value,
            first_seen_at=remote_object.first_seen_at,
            last_seen_at=remote_object.last_seen_at,
            facets=facet_views,
        )

    @staticmethod
    def _event_view(
        event: OperationalEventRecord, system_names: Mapping[str, str]
    ) -> OperationalEventView:
        return OperationalEventView(
            event_type=event.event_type,
            severity=event.severity,
            summary=event.redacted_summary,
            occurred_at=event.occurred_at,
            system_name=system_names.get(event.system_id or "", "Local runtime"),
            error_class=event.error_class,
            action_id=event.action_id,
            system_id=event.system_id,
        )

    @staticmethod
    def _action_activity_view(
        action: ActionActivityRecord, system_names: Mapping[str, str]
    ) -> ActionActivityView:
        return ActionActivityView(
            action_id=action.action_id,
            system_id=action.system_id,
            system_name=system_names.get(action.system_id, "Unknown system"),
            capability_key=action.capability_key,
            target_kind=action.target_kind,
            target_id=action.target_id,
            state=action.state,
            created_at=action.created_at,
            started_at=action.started_at,
            completed_at=action.completed_at,
            retry_at=action.retry_at,
            error_class=action.error_class,
            diagnostic=action.redacted_diagnostic,
        )

    @staticmethod
    def _action_attempt_view(attempt: ActionAttemptRecord) -> ActionAttemptView:
        return ActionAttemptView(
            ordinal=attempt.ordinal,
            started_at=attempt.started_at,
            ended_at=attempt.ended_at,
            outcome=attempt.outcome,
            error_class=attempt.error_class,
            retry_at=attempt.retry_at,
            diagnostic=attempt.redacted_diagnostic,
        )

    def _refresh_options(
        self,
        *,
        systems: Sequence[SystemRecord],
        objects: Sequence[RemoteObject],
        refresh_status: tuple[bool, str | None] | None = None,
    ) -> tuple[RefreshOption, ...]:
        options: list[RefreshOption] = []
        refresh_available, refresh_error = refresh_status or self._refresh_status()
        bindings_by_system = {
            system.system_id: tuple(
                binding
                for binding in self._store.list_connection_bindings(system_id=system.system_id)
                if binding.enabled
            )
            for system in systems
            if system.enabled
        }
        capabilities_by_system = {
            system.system_id: {
                capability.capability_key: capability
                for capability in self._store.list_capability_bindings(system_id=system.system_id)
                if capability.enabled
                and capability.connection_binding_id
                in {binding.binding_id for binding in bindings_by_system.get(system.system_id, ())}
            }
            for system in systems
            if system.enabled
        }

        for configured in self._store.list_configured_scopes():
            capabilities = capabilities_by_system.get(configured.system_id, {})
            key = (
                "databricks.workspace.children.read"
                if configured.object_type == "folder"
                else "databricks.uc.catalogs.read"
            )
            capability = capabilities.get(key)
            if configured.enabled and capability is not None:
                facet = "membership" if configured.object_type == "folder" else "attributes"
                options.append(
                    RefreshOption(
                        system_id=configured.system_id,
                        target_kind=TargetKind.CONFIGURED_SCOPE.value,
                        target_id=configured.scope_id,
                        capability_key=key,
                        facet=facet,
                        label=f"Refresh {configured.display_name}",
                        collateral_effects="; ".join(capability.collateral_effects)
                        or "None declared",
                        enabled=refresh_available,
                        disabled_reason=refresh_error if not refresh_available else None,
                    )
                )

        for remote_object in objects:
            if remote_object.presence is PresenceState.ABSENT:
                continue
            capabilities = capabilities_by_system.get(remote_object.system_id, {})
            candidates: list[tuple[str, str, str]] = []
            if remote_object.source_kind == "databricks.workspace.folder":
                candidates.extend(
                    [
                        (
                            "databricks.workspace.children.read",
                            "membership",
                            f"Refresh children of {remote_object.display_name}",
                        ),
                        (
                            "databricks.workspace.metadata.read",
                            "metadata",
                            f"Refresh metadata for {remote_object.display_name}",
                        ),
                    ]
                )
            elif remote_object.source_kind == "databricks.workspace.file":
                candidates.append(
                    (
                        "databricks.workspace.metadata.read",
                        "metadata",
                        f"Refresh metadata for {remote_object.display_name}",
                    )
                )
                content_enabled = any(
                    binding.non_secret_settings.get("content_capture_enabled") is True
                    for binding in bindings_by_system.get(remote_object.system_id, ())
                )
                if content_enabled:
                    candidates.append(
                        (
                            "databricks.workspace.content.read",
                            "content",
                            f"Refresh content for {remote_object.display_name}",
                        )
                    )
            elif remote_object.source_kind == "databricks.uc.catalog":
                candidates.append(
                    (
                        "databricks.uc.schemas.read",
                        "attributes",
                        f"Refresh schemas in {remote_object.display_name}",
                    )
                )
            elif remote_object.source_kind == "databricks.uc.schema":
                candidates.extend(
                    [
                        (
                            "databricks.uc.relations.read",
                            "attributes",
                            f"Refresh tables and views in {remote_object.display_name}",
                        ),
                        (
                            "databricks.uc.volumes.read",
                            "attributes",
                            f"Refresh volume metadata in {remote_object.display_name}",
                        ),
                    ]
                )
            for key, facet, label in candidates:
                capability = capabilities.get(key)
                if capability is None:
                    continue
                options.append(
                    RefreshOption(
                        system_id=remote_object.system_id,
                        target_kind=TargetKind.OBJECT.value,
                        target_id=remote_object.object_id,
                        capability_key=key,
                        facet=facet,
                        label=label,
                        collateral_effects="; ".join(capability.collateral_effects)
                        or "None declared",
                        enabled=refresh_available,
                        disabled_reason=refresh_error if not refresh_available else None,
                    )
                )
        return tuple(
            sorted(
                options,
                key=lambda option: (
                    option.system_id,
                    option.label.casefold(),
                    option.capability_key,
                ),
            )
        )

    @staticmethod
    def _cursor(values: tuple[str, ...]) -> str:
        encoded = json.dumps(values, ensure_ascii=True, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode("ascii")

    @staticmethod
    def _cursor_values(value: str, *, count: int) -> tuple[str, ...]:
        if not value:
            return ()
        try:
            padding = "=" * (-len(value) % 4)
            decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
            payload = json.loads(decoded)
        except (binascii.Error, UnicodeError, json.JSONDecodeError, ValueError):
            raise ValueError("invalid page cursor") from None
        if (
            not isinstance(payload, list)
            or len(payload) != count
            or not all(isinstance(item, str) and len(item) <= 512 for item in payload)
        ):
            raise ValueError("invalid page cursor")
        return tuple(payload)

    @staticmethod
    def _cursor_time(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("invalid page cursor") from None
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError("invalid page cursor")
        return parsed.astimezone(UTC)

    @staticmethod
    def _page_url(query: DashboardQuery, cursor: str) -> str:
        parameters: dict[str, str | int] = {}
        if query.object_query:
            parameters["q"] = query.object_query
        if cursor:
            parameters["after"] = cursor
        encoded = urlencode(parameters)
        return f"/?{encoded}" if encoded else "/"

    @staticmethod
    def _object_page_url(object_id: str, query: ObjectDetailQuery, cursor: str) -> str:
        parameters: dict[str, str | int] = {}
        if query.object_type:
            parameters["type"] = query.object_type
        if cursor:
            parameters["after"] = cursor
        encoded = urlencode(parameters)
        base = f"/objects/{object_id}"
        return f"{base}?{encoded}" if encoded else base

    async def dashboard(self, query: DashboardQuery | None = None) -> DashboardView:
        query = query or DashboardQuery()
        worker_available, _worker_error = self._worker_status()
        refresh_available, refresh_error = self._refresh_status()
        systems = self._store.list_systems()
        system_names = {system.system_id: system.display_name for system in systems}
        object_cursor = self._cursor_values(
            query.cursor,
            count=2 if query.object_query else 1,
        )
        after_name: str | None = None
        after_id: str | None = None
        if object_cursor:
            if query.object_query:
                after_name, after_id = object_cursor
            else:
                after_id = object_cursor[0]
            after_id = str(UUID(after_id))
        object_rows = self._store.list_objects_after(
            after_name=after_name,
            after_id=after_id,
            limit=query.object_page_size + 1,
            query=query.object_query,
        )
        has_more_objects = len(object_rows) > query.object_page_size
        objects = object_rows[: query.object_page_size]
        actions = self._store.list_latest_system_activity()
        latest_facet_actions = {
            (record.system_id, record.object_id, record.facet): record
            for record in self._store.list_latest_facet_actions(
                tuple(str(remote_object.object_id) for remote_object in objects)
            )
        }
        alerts = tuple(
            self._event_view(event, system_names)
            for event in self._store.list_operational_events(alertable_only=True, limit=10)
        )
        actions_by_system: dict[str, list[ActionActivityRecord]] = {
            system.system_id: [] for system in systems
        }
        for stored_action in actions:
            actions_by_system.setdefault(stored_action.system_id, []).append(stored_action)
        system_views: list[SystemView] = []
        for system in systems:
            system_actions = actions_by_system.get(system.system_id, ())
            last = system_actions[0] if system_actions else None
            activity = (
                ActivityView(
                    state=last.state,
                    occurred_at=last.completed_at or last.started_at or last.created_at,
                    summary=last.capability_key,
                    failure=last.redacted_diagnostic,
                )
                if last
                else None
            )
            scopes = self._store.list_configured_scopes(system_id=system.system_id)
            bindings = self._store.list_connection_bindings(system_id=system.system_id)
            binding_settings = bindings[0].non_secret_settings if len(bindings) == 1 else {}
            fingerprint = binding_settings.get("authority_fingerprint")
            authority_label = (
                f"Verified {fingerprint[:12]}"
                if isinstance(fingerprint, str) and fingerprint != "0" * 64
                else "Placeholder authority"
                if fingerprint == "0" * 64
                else "Legacy / unverified"
            )
            identity = self._store.get_configured_identity_for_system(system.system_id)
            retired = self._store.is_system_authority_retired(system.system_id)
            system_views.append(
                SystemView(
                    system_id=system.system_id,
                    name=system.display_name,
                    kind=system.system_kind,
                    enabled=system.enabled,
                    connection_state="configured" if bindings else "unknown",
                    configured_scopes=tuple(scope.display_name for scope in scopes),
                    worker_available=worker_available,
                    last_activity=activity,
                    config_id=identity[0] if identity is not None else "Legacy / unconfigured",
                    workspace_root=str(binding_settings.get("workspace_root", "Unknown")),
                    authority_label=(
                        f"{authority_label} · Retired" if retired else authority_label
                    ),
                    retired=retired,
                )
            )

        object_views = [
            self._object_view(
                remote_object,
                latest_facet_actions=latest_facet_actions,
            )
            for remote_object in objects
        ]
        system_views.sort(key=lambda item: (not item.enabled, item.name.casefold(), item.system_id))
        refresh_options = self._refresh_options(
            systems=systems,
            objects=objects,
            refresh_status=(refresh_available, refresh_error),
        )
        refresh_empty_reason = ""
        if not refresh_options:
            if systems and not any(system.enabled for system in systems):
                refresh_empty_reason = (
                    "Historical cache only; no enabled authority. Restore or configure an "
                    "authority to request refreshes."
                )
            elif not systems:
                refresh_empty_reason = "No system authority is configured."
            else:
                refresh_empty_reason = (
                    "No observation capability is registered for the enabled authority."
                )
        return DashboardView(
            systems=tuple(system_views),
            objects=tuple(object_views),
            refresh_options=refresh_options,
            loaded_at=datetime.now(UTC),
            disconnected=False,
            error=None,
            refresh_unavailable=not refresh_available,
            refresh_error=refresh_error,
            object_total=len(objects),
            object_page=1,
            object_page_count=2 if has_more_objects else 1,
            object_page_start=1 if objects else 0,
            object_page_end=len(objects),
            object_query=query.object_query,
            previous_page_url=(self._page_url(query, "") if query.cursor else None),
            next_page_url=(
                self._page_url(
                    query,
                    self._cursor(
                        (objects[-1].display_name, str(objects[-1].object_id))
                        if query.object_query
                        else (str(objects[-1].object_id),)
                    ),
                )
                if has_more_objects and objects
                else None
            ),
            alerts=alerts,
            refresh_empty_reason=refresh_empty_reason,
        )

    @staticmethod
    def _alert_page_url(query: AlertHistoryQuery, cursor: str) -> str:
        parameters: dict[str, str | int] = {}
        if query.event_type:
            parameters["type"] = query.event_type
        if query.severity:
            parameters["severity"] = query.severity
        if cursor:
            parameters["after"] = cursor
        encoded = urlencode(parameters)
        return f"/alerts?{encoded}" if encoded else "/alerts"

    @staticmethod
    def _action_page_url(query: ActionHistoryQuery, cursor: str) -> str:
        parameters: dict[str, str | int] = {}
        if query.state:
            parameters["state"] = query.state
        if query.system_id:
            parameters["system"] = query.system_id
        if query.action_id:
            parameters["action"] = query.action_id
        if cursor:
            parameters["after"] = cursor
        encoded = urlencode(parameters)
        return f"/actions?{encoded}" if encoded else "/actions"

    async def action_history(self, query: ActionHistoryQuery | None = None) -> ActionHistoryView:
        query = query or ActionHistoryQuery()
        system_id = query.system_id or None
        state = query.state or None
        action_id = query.action_id or None
        action_cursor = self._cursor_values(query.cursor, count=2)
        after_created_at: datetime | None = None
        after_action_id: str | None = None
        if action_cursor:
            after_created_at = self._cursor_time(action_cursor[0])
            after_action_id = str(UUID(action_cursor[1]))
        systems = self._store.list_systems()
        system_names = {system.system_id: system.display_name for system in systems}
        action_rows = self._store.list_action_activity_after(
            after_created_at=after_created_at,
            after_action_id=after_action_id,
            limit=query.page_size + 1,
            system_id=system_id,
            state=state,
            action_id=action_id,
        )
        has_more_actions = len(action_rows) > query.page_size
        page_rows = action_rows[: query.page_size]
        actions = tuple(self._action_activity_view(action, system_names) for action in page_rows)
        return ActionHistoryView(
            actions=actions,
            systems=tuple(
                ActionSystemOption(
                    system_id=system.system_id,
                    name=f"{system.display_name} · {system.system_id[:8]}",
                )
                for system in systems
            ),
            total=len(actions),
            page=1,
            page_count=2 if has_more_actions else 1,
            page_start=1 if actions else 0,
            page_end=len(actions),
            state_filter=query.state,
            system_filter=query.system_id,
            action_filter=query.action_id,
            previous_page_url=(self._action_page_url(query, "") if query.cursor else None),
            next_page_url=(
                self._action_page_url(
                    query,
                    self._cursor(
                        (
                            page_rows[-1]
                            .created_at.isoformat(timespec="microseconds")
                            .replace("+00:00", "Z"),
                            page_rows[-1].action_id,
                        )
                    ),
                )
                if has_more_actions and page_rows
                else None
            ),
            loaded_at=datetime.now(UTC),
        )

    async def action_detail(self, action_id: str) -> ActionDetailView | None:
        action = self._store.get_action_activity(action_id)
        if action is None:
            return None
        systems = self._store.list_systems()
        system_names = {system.system_id: system.display_name for system in systems}
        attempt_rows = self._store.list_action_attempts(action_id, limit=101)
        attempts_truncated = len(attempt_rows) > 100
        visible_attempts = attempt_rows[-100:]
        return ActionDetailView(
            action=self._action_activity_view(action, system_names),
            attempts=tuple(self._action_attempt_view(attempt) for attempt in visible_attempts),
            attempt_total=len(visible_attempts),
            attempts_truncated=attempts_truncated,
            loaded_at=datetime.now(UTC),
        )

    async def alert_history(self, query: AlertHistoryQuery | None = None) -> AlertHistoryView:
        query = query or AlertHistoryQuery()
        event_type = query.event_type or None
        severity = query.severity or None
        alert_cursor = self._cursor_values(query.cursor, count=2)
        after_occurred_at: datetime | None = None
        after_event_id: str | None = None
        if alert_cursor:
            after_occurred_at = self._cursor_time(alert_cursor[0])
            after_event_id = str(UUID(alert_cursor[1]))
        systems = self._store.list_systems()
        system_names = {system.system_id: system.display_name for system in systems}
        alert_rows = self._store.list_alertable_events_after(
            after_occurred_at=after_occurred_at,
            after_event_id=after_event_id,
            limit=query.page_size + 1,
            event_type=event_type,
            severity=severity,
        )
        has_more_alerts = len(alert_rows) > query.page_size
        page_rows = alert_rows[: query.page_size]
        alerts = tuple(self._event_view(event, system_names) for event in page_rows)
        return AlertHistoryView(
            alerts=alerts,
            total=len(alerts),
            page=1,
            page_count=2 if has_more_alerts else 1,
            page_start=1 if alerts else 0,
            page_end=len(alerts),
            event_type_filter=query.event_type,
            severity_filter=query.severity,
            previous_page_url=(self._alert_page_url(query, "") if query.cursor else None),
            next_page_url=(
                self._alert_page_url(
                    query,
                    self._cursor(
                        (
                            page_rows[-1]
                            .occurred_at.isoformat(timespec="microseconds")
                            .replace("+00:00", "Z"),
                            page_rows[-1].event_id,
                        )
                    ),
                )
                if has_more_alerts and page_rows
                else None
            ),
            loaded_at=datetime.now(UTC),
        )

    async def object_detail(
        self, object_id: str, query: ObjectDetailQuery | None = None
    ) -> ObjectDetailView | None:
        query = query or ObjectDetailQuery()
        remote_object = self._store.get_object_sync(object_id)
        if remote_object is None:
            return None
        systems = self._store.list_systems()
        latest_facet_actions = {
            (record.system_id, record.object_id, record.facet): record
            for record in self._store.list_latest_facet_actions((object_id,))
        }
        relationship_cursor = self._cursor_values(query.cursor, count=1)
        after_relationship_id = str(UUID(relationship_cursor[0])) if relationship_cursor else None
        relationship_rows = self._store.list_related_objects_after_sync(
            object_id,
            after_id=after_relationship_id,
            limit=query.relationship_page_size + 1,
            predicate="contains",
            object_type=query.object_type or None,
        )
        has_more_relationships = len(relationship_rows) > query.relationship_page_size
        related_children = relationship_rows[: query.relationship_page_size]
        children = tuple(
            RelatedObjectView(
                object_id=str(record.object.object_id),
                name=record.object.display_name,
                object_type=record.object.object_type,
                predicate=record.relationship.predicate,
                relationship_presence=record.relationship.presence.value,
                object_presence=record.object.presence.value,
                observed_at=record.relationship.observed_at,
            )
            for record in related_children
        )
        object_view = self._object_view(
            remote_object,
            latest_facet_actions=latest_facet_actions,
        )
        worker_available, worker_error = self._worker_status()
        refresh_status = self._refresh_status()
        refresh_options = tuple(
            option
            for option in self._refresh_options(
                systems=systems,
                objects=(remote_object,),
                refresh_status=refresh_status,
            )
            if option.target_kind == TargetKind.OBJECT.value and option.target_id == object_id
        )
        owning_system = next(
            (system for system in systems if system.system_id == str(remote_object.system_id)),
            None,
        )
        refresh_empty_reason = ""
        if not refresh_options:
            refresh_empty_reason = (
                "Historical cache only; this authority is paused. Restore or configure it to "
                "request a refresh."
                if owning_system is not None and not owning_system.enabled
                else "No compatible observation capability is registered for this object."
            )
        system_name = next(
            (
                system.display_name
                for system in systems
                if system.system_id == str(remote_object.system_id)
            ),
            "Unknown system",
        )
        return ObjectDetailView(
            object=object_view,
            system_name=system_name,
            children=children,
            refresh_options=refresh_options,
            relationship_total=len(children),
            relationship_page=1,
            relationship_page_count=2 if has_more_relationships else 1,
            relationship_page_start=1 if children else 0,
            relationship_page_end=len(children),
            object_type_filter=query.object_type,
            previous_page_url=(
                self._object_page_url(object_id, query, "") if query.cursor else None
            ),
            next_page_url=(
                self._object_page_url(
                    object_id,
                    query,
                    self._cursor((str(related_children[-1].object.object_id),)),
                )
                if has_more_relationships and related_children
                else None
            ),
            loaded_at=datetime.now(UTC),
            disconnected=not worker_available,
            error=worker_error,
            refresh_empty_reason=refresh_empty_reason,
        )

    async def is_refresh_registered(self, request: RefreshRequest) -> bool:
        systems = self._store.list_systems()
        objects: tuple[RemoteObject, ...] = ()
        if request.target_kind == TargetKind.OBJECT.value:
            remote_object = self._store.get_object_sync(request.target_id)
            if remote_object is None:
                return False
            objects = (remote_object,)
        options = self._refresh_options(systems=systems, objects=objects)
        return any(
            option.enabled
            and (
                option.system_id,
                option.target_kind,
                option.target_id,
                option.capability_key,
                option.facet,
            )
            == (
                request.system_id,
                request.target_kind,
                request.target_id,
                request.capability_key,
                request.facet,
            )
            for option in options
        )

    async def submit_refresh(self, request: RefreshRequest) -> str:
        if not await self.is_refresh_registered(request):
            raise ValueError("refresh selection is not registered")
        target_kind = TargetKind(request.target_kind)
        if target_kind is TargetKind.CONFIGURED_SCOPE:
            target = self._store.get_configured_scope(request.target_id)
            if target is None:
                raise ValueError("configured scope is unavailable")
            object_type = target.object_type
        else:
            remote_object = self._store.get_object_sync(request.target_id)
            if remote_object is None:
                raise ValueError("object target is unavailable")
            object_type = remote_object.object_type
        coverage = (
            RefreshCoverage.COLLECTION_MEMBERS
            if request.capability_key.endswith(
                (
                    "children.read",
                    "catalogs.read",
                    "schemas.read",
                    "relations.read",
                    "volumes.read",
                )
            )
            else RefreshCoverage.FACET
        )
        intent_id = str(uuid4())
        intent = RefreshIntent(
            intent_id=intent_id,
            idempotency_key=str(uuid4()),
            origin=RefreshOrigin.MANUAL,
            actor_id="local-user",
            ui_session_id=request.ui_session_id,
            scopes=(
                RefreshScope(
                    system_id=request.system_id,
                    target=TargetRef(target_kind, request.target_id),
                    object_type=object_type,
                    facet=request.facet,
                    capability_key=request.capability_key,
                    coverage=coverage,
                ),
            ),
            requested_at=datetime.now(UTC),
        )
        await self._store.submit_refresh(intent)
        self._wake_worker()
        return intent_id

    async def intent(self, intent_id: str) -> IntentView | None:
        unsupported_contract = False
        try:
            intent = self._store.get_refresh_intent(intent_id)
        except (TypeError, ValueError):
            intent = None
            unsupported_contract = True
        if intent is None:
            requested_at = self._store.get_refresh_intent_requested_at(intent_id)
            if requested_at is None:
                return None
        else:
            requested_at = intent.requested_at
        views: list[IntentScopeView] = []
        terminal = True
        updated_at = requested_at
        for record in self._store.list_intent_scopes(intent_id):
            state = record.state.value
            failure: str | None = None
            action_id = record.linked_action_id
            scope_terminal = record.state in _TERMINAL_SCOPE_STATES
            if action_id:
                action = self._store.get_stored_action(action_id)
                if action is not None:
                    updated_at = action.completed_at or action.started_at or updated_at
                    if not scope_terminal:
                        state = action.state.value
                        failure = action.redacted_diagnostic
                        terminal = terminal and action.state in _TERMINAL_ACTION_STATES
                else:
                    terminal = terminal and scope_terminal
            else:
                terminal = terminal and scope_terminal
            target_label = record.scope.target.target_id
            if record.scope.target.kind is TargetKind.CONFIGURED_SCOPE:
                configured = self._store.get_configured_scope(target_label)
                if configured is not None:
                    target_label = configured.display_name
            else:
                remote_object = self._store.get_object_sync(target_label)
                if remote_object is not None:
                    target_label = remote_object.display_name
            views.append(
                IntentScopeView(
                    label=f"{target_label}: {record.scope.facet}",
                    state=state,
                    system_id=record.scope.system_id,
                    target_kind=record.scope.target.kind.value,
                    target_id=record.scope.target.target_id,
                    capability_key=record.scope.capability_key or "",
                    facet=record.scope.facet,
                    action_id=action_id,
                    eligible_at=record.eligible_at,
                    failure=failure or record.disposition_reason
                    if state in {"failed", "rejected"}
                    else failure,
                    cached_context=target_label,
                )
            )
        return IntentView(
            intent_id=intent_id,
            requested_at=requested_at,
            scopes=tuple(views),
            updated_at=updated_at,
            terminal=terminal,
            error=(
                "Stored request contract is unsupported; durable dispositions remain available."
                if unsupported_contract
                else None
            ),
        )


@dataclass(slots=True)
class ApplicationRuntime:
    settings: ProjectSettings
    store: SQLiteStore
    coordinator: DurableCoordinator
    worker: DatabricksWorker
    runner: CliRunner
    backend: SQLiteWebBackend
    local_authorizer: LocalCallerAuthorizer
    app: FastAPI
    worker_available: bool = False
    worker_error: str | None = None
    _stop_event: asyncio.Event | None = field(default=None, init=False)
    _wake_event: asyncio.Event | None = field(default=None, init=False)
    _background_task: asyncio.Task[None] | None = field(default=None, init=False)
    _worker_started: bool = field(default=False, init=False)
    _lifecycle_closed: bool = field(default=False, init=False)
    _worker_recovery_generation: int | None = field(default=None, init=False)
    _component_errors: dict[str, str] = field(default_factory=dict, init=False)
    _failure_counts: dict[str, int] = field(default_factory=dict, init=False)

    def status(self) -> tuple[bool, str | None]:
        return self.worker_available, self.worker_error

    def wake(self) -> None:
        if self._wake_event is not None:
            self._wake_event.set()

    async def start(self) -> None:
        if self._background_task is not None or self._lifecycle_closed:
            raise RuntimeError("runtime lifecycle has already started")
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._component_errors.clear()
        self._failure_counts.clear()
        self._worker_started = False
        self.worker_available = False
        self.worker_error = "Worker compatibility check is in progress."
        self._background_task = asyncio.create_task(
            self._run_background(),
            name="async-api-view-runtime",
        )
        # Let fast compatibility checks settle without making web readiness depend on
        # an external CLI process that may take minutes or never return.
        await asyncio.sleep(0)

    async def stop(self) -> None:
        if self._lifecycle_closed:
            return
        if self._stop_event is not None:
            self._stop_event.set()
        if self._wake_event is not None:
            self._wake_event.set()
        task = self._background_task
        if task is not None and not task.done():
            task.cancel()
        try:
            if task is not None:
                await task
        except asyncio.CancelledError:
            pass
        finally:
            self.store.close()
            self._lifecycle_closed = True

    def _record_background_failure(self, component: str, error: BaseException) -> None:
        summary = f"{component} stopped unexpectedly ({type(error).__name__})"
        first_failure = component not in self._component_errors
        self._component_errors[component] = summary
        self._failure_counts[component] = self._failure_counts.get(component, 0) + 1
        self.worker_available = False
        self.worker_error = summary
        if not first_failure:
            logger.warning("%s; retrying", summary)
            return
        logger.error("%s; retrying", summary)
        event_type = {
            "coordinator": "queue.coordinator.failed",
            "worker": "queue.adapter_worker.failed",
        }[component]
        try:
            self.store.record_runtime_failure(
                event_type=event_type,
                summary=summary,
                occurred_at=datetime.now(UTC),
            )
        except Exception:
            logger.exception("Could not persist the redacted %s outage event", component)

    def _record_component_recovery(self, component: str) -> None:
        if component in self._component_errors:
            logger.info("%s recovered", component)
        self._component_errors.pop(component, None)
        self._failure_counts.pop(component, None)
        self.worker_available = self._worker_started and not self._component_errors
        self.worker_error = next(reversed(self._component_errors.values()), None)

    def _retry_delay(self, component: str) -> float:
        failures = self._failure_counts.get(component, 1)
        base = max(0.1, min(self.settings.app.worker_poll_seconds, 5.0))
        return min(30.0, base * (2 ** min(failures - 1, 5)))

    async def _wait_for_activity(self, timeout: float) -> None:
        if self._wake_event is None:
            raise RuntimeError("runtime was not started")
        if self._wake_event.is_set():
            self._wake_event.clear()
            return
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
        except TimeoutError:
            return
        self._wake_event.clear()

    async def _run_background(self) -> None:
        if self._stop_event is None or self._wake_event is None:
            raise RuntimeError("runtime was not started")
        while not self._stop_event.is_set():
            if not self._worker_started:
                try:
                    await self.worker.startup()
                except Exception as exc:
                    if isinstance(exc, LifecyclePersistenceFailure):
                        self._worker_recovery_generation = self.worker.ingestion_generation + 1
                    self._record_background_failure("worker", exc)
                    await self._wait_for_activity(self._retry_delay("worker"))
                    continue
                self._worker_started = True
            worked = False
            try:
                for _ in range(100):
                    coordinator_result = await self.coordinator.run_once()
                    if coordinator_result is None:
                        break
                    worked = True
            except Exception as exc:
                self._record_background_failure("coordinator", exc)
                await self._wait_for_activity(self._retry_delay("coordinator"))
                continue
            self._record_component_recovery("coordinator")
            await asyncio.sleep(0)
            try:
                for _ in range(100):
                    if not await self.worker.run_once():
                        break
                    worked = True
            except Exception as exc:
                self._worker_started = False
                if isinstance(exc, LifecyclePersistenceFailure):
                    self._worker_recovery_generation = self.worker.ingestion_generation + 1
                self._record_background_failure("worker", exc)
                await self._wait_for_activity(self._retry_delay("worker"))
                continue
            if (
                self._worker_recovery_generation is None
                or self.worker.ingestion_generation >= self._worker_recovery_generation
            ):
                self._worker_recovery_generation = None
                self._record_component_recovery("worker")
            await asyncio.sleep(0)
            if worked:
                continue
            await self._wait_for_activity(self.settings.app.worker_poll_seconds)


def build_runtime(
    settings: ProjectSettings,
    *,
    runner: CliRunner | None = None,
    clock: Callable[[], datetime] | None = None,
    available_bytes_probe: Callable[[], int] | None = None,
) -> ApplicationRuntime:
    """Initialize durable configuration and compose the one-process application."""
    if any(
        system.authority_fingerprint == PLACEHOLDER_AUTHORITY_FINGERPRINT
        for system in settings.databricks_systems
    ):
        raise ConfigError(
            "Databricks authority fingerprint is still the placeholder; run "
            "async-api-view fingerprint-profile and update authority_fingerprint"
        )
    config_ids = tuple(
        canonical_config_id(system.config_id)
        for system in settings.databricks_systems
        if system.config_id is not None
    )
    if len(set(config_ids)) != len(config_ids):
        raise ConfigError("Databricks system IDs must be unique")
    reserve = max(MIN_WRITE_RESERVE_BYTES, 2 * settings.app.cli_output_limit_bytes)
    store = SQLiteStore(
        settings.app.database_path,
        clock=clock,
        available_bytes_probe=available_bytes_probe,
        minimum_write_headroom_bytes=reserve + settings.app.cli_output_limit_bytes,
    )
    try:
        return _compose_runtime(settings, store=store, runner=runner)
    except BaseException:
        store.close()
        raise


def _apply_local_configuration(settings: ProjectSettings, store: SQLiteStore) -> None:
    bootstrap = SystemBootstrapService(store)
    configured_system_ids: set[str] = set()
    configured_binding_ids: set[str] = set()
    configured_capability_ids: set[str] = set()
    configured_scope_ids: set[str] = set()
    for system in settings.databricks_systems:
        config_id = canonical_config_id(system.config_id) if system.config_id is not None else None
        authority_material = json.dumps(
            [system.authority_fingerprint, system.workspace_root],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode()
        authority_key = f"databricks-host-v1:{hashlib.sha256(authority_material).hexdigest()}"
        legacy_stable = str(
            uuid5(
                NAMESPACE_URL,
                "async-api-view/databricks/legacy/"
                f"{system.name}/{system.authority_fingerprint}/{system.workspace_root}",
            )
        )
        stable = legacy_stable
        if config_id is not None:
            mapped = store.get_configured_system_identity(
                system_kind="databricks.workspace",
                config_id=config_id,
                authority_key=authority_key,
            )
            stable = mapped or str(
                uuid5(
                    NAMESPACE_URL,
                    f"async-api-view/databricks/{config_id}/{authority_key}",
                )
            )
        seeded = bootstrap.configure_databricks_workspace(
            display_name=system.name,
            profile=system.profile,
            workspace_root=system.workspace_root,
            enabled_capability_keys=tuple(sorted(CAPABILITIES)),
            non_secret_settings={
                "authority_fingerprint": system.authority_fingerprint,
                "content_capture_enabled": False,
                "content_max_bytes": settings.app.cli_output_limit_bytes,
                "content_retention_days": 365,
            },
            system_id=stable,
            connection_binding_id=str(uuid5(NAMESPACE_URL, f"{stable}/binding")),
            workspace_root_object_id=str(uuid5(NAMESPACE_URL, f"{stable}/workspace-root")),
            workspace_root_scope_id=str(uuid5(NAMESPACE_URL, f"{stable}/workspace-root-scope")),
        )
        if config_id is not None:
            store.upsert_configured_system_identity(
                system_kind="databricks.workspace",
                config_id=config_id,
                authority_key=authority_key,
                system_id=seeded.system.system_id,
            )
        configured_system_ids.add(seeded.system.system_id)
        configured_binding_ids.add(seeded.connection_binding_id)
        configured_capability_ids.update(seeded.capability_binding_ids)
        configured_scope_ids.add(seeded.workspace_root_scope.scope_id)
        if seeded.unity_catalog_root_scope is not None:
            configured_scope_ids.add(seeded.unity_catalog_root_scope.scope_id)
    store.reconcile_configured_resources(
        system_kind="databricks.workspace",
        system_ids=configured_system_ids,
        connection_binding_ids=configured_binding_ids,
        capability_binding_ids=configured_capability_ids,
        scope_ids=configured_scope_ids,
    )


def _compose_runtime(
    settings: ProjectSettings,
    *,
    store: SQLiteStore,
    runner: CliRunner | None,
) -> ApplicationRuntime:
    with store.configuration_transaction():
        _apply_local_configuration(settings, store)
    coordinator = DurableCoordinator(store)
    actual_runner = runner or CliRunner(
        timeout_seconds=settings.app.cli_timeout_seconds,
        stdout_cap=settings.app.cli_output_limit_bytes,
        stderr_cap=min(settings.app.cli_output_limit_bytes, 1024 * 1024),
    )
    ingestor = SQLiteObservationIngestor(store)
    resolver = SQLiteDatabricksTargetResolver(store)
    runtime_placeholder: dict[str, ApplicationRuntime] = {}
    backend = SQLiteWebBackend(
        store,
        worker_status=lambda: runtime_placeholder["runtime"].status(),
        wake_worker=lambda: runtime_placeholder["runtime"].wake(),
    )
    worker = DatabricksWorker(
        worker_id="databricks-worker-local",
        queue=store,
        lifecycle=store,
        guard=store,
        bindings=store,
        ingestion=ingestor,
        targets=resolver,
        runner=actual_runner,
        clock=store.authority_time,
    )
    local_authorizer = LocalCallerAuthorizer()
    app = create_app(
        backend,
        allowed_hosts=(local_authorizer.browser_host,),
        authorizer=local_authorizer,
    )
    runtime = ApplicationRuntime(
        settings=settings,
        store=store,
        coordinator=coordinator,
        worker=worker,
        runner=actual_runner,
        backend=backend,
        local_authorizer=local_authorizer,
        app=app,
    )
    runtime_placeholder["runtime"] = runtime

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            await runtime.start()
            yield
        finally:
            await runtime.stop()

    app.router.lifespan_context = lifespan
    return runtime
