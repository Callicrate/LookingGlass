"""Trusted local configuration helpers for the first Databricks slice."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid4, uuid5

from async_api_view.config import validate_databricks_profile
from async_api_view.contracts import (
    AbsenceAuthority,
    CapabilityBinding,
    CapabilityCoveragePolicy,
    CollectionCoverage,
    ConnectionBinding,
    OperationClass,
    PresenceState,
    RefreshCoverage,
    RemoteObject,
    TargetKind,
)
from async_api_view.contracts._validation import JSONValue, require_text
from async_api_view.storage import ConfiguredScopeRecord, SQLiteStore, SystemRecord


@dataclass(frozen=True, slots=True)
class DatabricksBootstrapResult:
    """IDs created or reused by :class:`SystemBootstrapService`."""

    system: SystemRecord
    connection_binding_id: str
    workspace_root_object_id: str
    workspace_root_scope: ConfiguredScopeRecord
    capability_binding_ids: tuple[str, ...]
    unity_catalog_root_object_id: str | None = None
    unity_catalog_root_scope: ConfiguredScopeRecord | None = None


_CAPABILITIES: dict[str, tuple[tuple[TargetKind, ...], tuple[str, ...]]] = {
    "databricks.workspace.children.read": (
        (TargetKind.CONFIGURED_SCOPE, TargetKind.OBJECT),
        ("membership", "metadata"),
    ),
    "databricks.workspace.metadata.read": ((TargetKind.OBJECT,), ("metadata",)),
    "databricks.workspace.content.read": ((TargetKind.OBJECT,), ("content",)),
    "databricks.uc.catalogs.read": ((TargetKind.CONFIGURED_SCOPE,), ("attributes",)),
    "databricks.uc.schemas.read": ((TargetKind.OBJECT,), ("attributes",)),
    "databricks.uc.relations.read": ((TargetKind.OBJECT,), ("attributes",)),
    "databricks.uc.volumes.read": ((TargetKind.OBJECT,), ("attributes",)),
}


def _coverage_policies(
    capability_key: str, target_kinds: tuple[TargetKind, ...]
) -> tuple[CapabilityCoveragePolicy, ...]:
    if capability_key == "databricks.workspace.metadata.read":
        return (
            CapabilityCoveragePolicy(
                TargetKind.OBJECT,
                "metadata",
                RefreshCoverage.FACET,
                CollectionCoverage.COMPLETE,
            ),
        )
    if capability_key == "databricks.workspace.content.read":
        return ()
    facet = "membership" if capability_key == "databricks.workspace.children.read" else "attributes"
    return tuple(
        CapabilityCoveragePolicy(
            target_kind,
            facet,
            coverage,
            CollectionCoverage.COMPLETE,
            (AbsenceAuthority.RELATIONSHIP,),
        )
        for target_kind in target_kinds
        for coverage in (RefreshCoverage.FACET, RefreshCoverage.COLLECTION_MEMBERS)
    )


class SystemBootstrapService:
    """Create systems, bindings, scopes, and non-secret capability declarations.

    This service never accepts a credential value. ``profile`` is a bounded
    reference to existing Databricks CLI configuration, not an auth token.
    """

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def create_system(
        self,
        *,
        display_name: str,
        system_kind: str,
        system_id: str | None = None,
        enabled: bool = True,
        now: datetime | None = None,
    ) -> SystemRecord:
        return self.store.create_system(
            system_id=system_id or str(uuid4()),
            display_name=display_name,
            system_kind=system_kind,
            enabled=enabled,
            now=now,
        )

    def configure_databricks_workspace(
        self,
        *,
        display_name: str,
        profile: str,
        workspace_root: str,
        enabled_capability_keys: Iterable[str] = ("databricks.workspace.children.read",),
        non_secret_settings: Mapping[str, JSONValue] | None = None,
        system_id: str | None = None,
        connection_binding_id: str | None = None,
        workspace_root_object_id: str | None = None,
        workspace_root_scope_id: str | None = None,
        now: datetime | None = None,
    ) -> DatabricksBootstrapResult:
        """Seed a Workspace root as a canonical unknown folder and configured scope."""
        profile = validate_databricks_profile(profile, "profile")
        require_text(workspace_root, "workspace_root", max_length=4096)
        capability_keys = tuple(dict.fromkeys(enabled_capability_keys))
        unknown_capabilities = set(capability_keys) - set(_CAPABILITIES)
        if unknown_capabilities:
            raise ValueError(f"unsupported Databricks capability {sorted(unknown_capabilities)!r}")
        extra_settings = set(non_secret_settings or ()) & {"profile", "workspace_root"}
        if extra_settings:
            raise ValueError(
                f"non_secret_settings cannot override reserved settings: {sorted(extra_settings)!r}"
            )
        timestamp = now or datetime.now(UTC)
        system = self.create_system(
            display_name=display_name,
            system_kind="databricks.workspace",
            system_id=system_id,
            now=timestamp,
        )
        binding_id = connection_binding_id or str(
            uuid5(NAMESPACE_URL, f"databricks-binding:{system.system_id}")
        )
        settings: dict[str, JSONValue] = {"profile": profile, "workspace_root": workspace_root}
        if non_secret_settings:
            settings.update(non_secret_settings)
        self.store.upsert_connection_binding(
            ConnectionBinding(
                binding_id=binding_id,
                system_id=system.system_id,
                adapter_key="databricks",
                adapter_version="1",
                enabled=True,
                non_secret_settings=settings,
            ),
            now=timestamp,
        )
        root = self.store.upsert_object(
            RemoteObject(
                object_id=workspace_root_object_id or str(uuid4()),
                system_id=system.system_id,
                object_type="folder",
                object_type_version="1",
                source_kind="databricks.workspace.folder",
                external_key=f"workspace:{workspace_root}",
                display_name=workspace_root,
                presence=PresenceState.UNKNOWN,
                first_seen_at=timestamp,
            )
        )
        configured_scope = self.store.create_configured_scope(
            scope_id=workspace_root_scope_id
            or str(
                uuid5(
                    NAMESPACE_URL, f"databricks-workspace-root:{system.system_id}:{workspace_root}"
                )
            ),
            system_id=system.system_id,
            object_id=root.object_id,
            object_type="folder",
            display_name=workspace_root,
            now=timestamp,
        )
        unity_catalog_root_object_id: str | None = None
        unity_catalog_root_scope: ConfiguredScopeRecord | None = None
        if "databricks.uc.catalogs.read" in capability_keys:
            unity_catalog_root = self.store.upsert_object(
                RemoteObject(
                    object_id=str(
                        uuid5(NAMESPACE_URL, f"databricks-uc-catalogs-root:{system.system_id}")
                    ),
                    system_id=system.system_id,
                    object_type="generic_object",
                    object_type_version="1",
                    source_kind="databricks.uc.catalog_collection",
                    external_key="catalogs",
                    display_name="Unity Catalog catalogs",
                    presence=PresenceState.UNKNOWN,
                    first_seen_at=timestamp,
                )
            )
            unity_catalog_root_object_id = unity_catalog_root.object_id
            unity_catalog_root_scope = self.store.create_configured_scope(
                scope_id=str(uuid5(NAMESPACE_URL, f"databricks-uc-catalogs:{system.system_id}")),
                system_id=system.system_id,
                object_id=unity_catalog_root.object_id,
                object_type="generic_object",
                display_name="Unity Catalog catalogs",
                now=timestamp,
            )
        capability_ids: list[str] = []
        for capability_key in capability_keys:
            target_kinds, produced_facets = _CAPABILITIES[capability_key]
            capability_id = str(
                uuid5(NAMESPACE_URL, f"databricks-capability:{binding_id}:{capability_key}")
            )
            self.store.upsert_capability_binding(
                CapabilityBinding(
                    capability_binding_id=capability_id,
                    connection_binding_id=binding_id,
                    capability_key=capability_key,
                    capability_version="1",
                    operation_class=OperationClass.OBSERVE,
                    target_kinds=target_kinds,
                    produced_facets=produced_facets,
                    enabled=True,
                    selection_priority=100,
                    collateral_effects=(
                        "remote audit or authentication record",
                        "remote API quota",
                    ),
                    mitigations=(
                        "fixed registered CLI command mapping",
                        "configured scope allowlist",
                    ),
                    coverage_policies=_coverage_policies(capability_key, target_kinds),
                ),
                now=timestamp,
            )
            capability_ids.append(capability_id)
        return DatabricksBootstrapResult(
            system=system,
            connection_binding_id=binding_id,
            workspace_root_object_id=root.object_id,
            workspace_root_scope=configured_scope,
            capability_binding_ids=tuple(capability_ids),
            unity_catalog_root_object_id=unity_catalog_root_object_id,
            unity_catalog_root_scope=unity_catalog_root_scope,
        )
