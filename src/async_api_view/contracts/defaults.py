"""Accepted version 1 type and facet defaults."""

from datetime import timedelta

from .models import FacetDefinition, ObjectTypeDefinition

ONE_DAY = timedelta(days=1)
SEVEN_DAYS = timedelta(days=7)

V1_TYPE_DEFINITIONS: tuple[ObjectTypeDefinition, ...] = (
    ObjectTypeDefinition(
        type_key="folder",
        version="1",
        facets=(
            FacetDefinition("metadata", "1", ONE_DAY),
            FacetDefinition("membership", "1", ONE_DAY),
        ),
    ),
    ObjectTypeDefinition(
        type_key="file",
        version="1",
        facets=(
            FacetDefinition("metadata", "1", ONE_DAY),
            FacetDefinition("content", "1", SEVEN_DAYS),
        ),
    ),
    ObjectTypeDefinition(
        type_key="service",
        version="1",
        facets=(
            FacetDefinition("runtime", "1", ONE_DAY),
            FacetDefinition("configuration", "1", SEVEN_DAYS),
        ),
    ),
    ObjectTypeDefinition(
        type_key="job",
        version="1",
        facets=(
            FacetDefinition("metadata", "1", ONE_DAY),
            FacetDefinition("status", "1", ONE_DAY),
            FacetDefinition("run_summary", "1", ONE_DAY),
        ),
    ),
    ObjectTypeDefinition(
        type_key="generic_object",
        version="1",
        facets=(FacetDefinition("attributes", "1", SEVEN_DAYS),),
    ),
)

V1_TYPE_DEFINITION_BY_KEY = {definition.type_key: definition for definition in V1_TYPE_DEFINITIONS}

# Generic Unity Catalog entities intentionally share the versioned generic metadata facet.
DATABRICKS_UNITY_CATALOG_SOURCE_KINDS: tuple[str, ...] = (
    "databricks.uc.catalog",
    "databricks.uc.schema",
    "databricks.uc.table",
    "databricks.uc.view",
    "databricks.uc.volume",
)
