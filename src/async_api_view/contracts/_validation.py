"""Small validation and JSON conversion helpers for contract DTOs."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

_CONTRACT_KEY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")


def require_text(value: str, field_name: str, *, max_length: int = 512) -> str:
    """Require bounded, non-empty text without NUL bytes."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text")
    if len(value) > max_length:
        raise ValueError(f"{field_name} must be at most {max_length} characters")
    if "\x00" in value:
        raise ValueError(f"{field_name} must not contain NUL bytes")
    return value


def require_contract_key(value: str, field_name: str) -> str:
    """Require a stable namespaced contract key, not executable text."""
    require_text(value, field_name, max_length=200)
    if _CONTRACT_KEY.fullmatch(value) is None:
        raise ValueError(f"{field_name} must contain only lower-case contract-key characters")
    return value


def require_uuid(value: str | UUID, field_name: str) -> str:
    """Return the canonical string form of a UUID."""
    try:
        return str(UUID(str(value)))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a UUID") from exc


def optional_uuid(value: str | UUID | None, field_name: str) -> str | None:
    if value is None:
        return None
    return require_uuid(value, field_name)


def require_utc(value: datetime, field_name: str) -> datetime:
    """Reject naive time and normalize an aware value to UTC."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must have a valid UTC offset")
    return value.astimezone(UTC)


def optional_utc(value: datetime | None, field_name: str) -> datetime | None:
    if value is None:
        return None
    return require_utc(value, field_name)


def require_positive_duration(value: timedelta, field_name: str) -> timedelta:
    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise ValueError(f"{field_name} must be a positive duration")
    return value


def require_instance[T](value: object, expected_type: type[T], field_name: str) -> T:
    if not isinstance(value, expected_type):
        raise ValueError(f"{field_name} must be a {expected_type.__name__}")
    return value


def normalize_instance_tuple[T](
    values: object, expected_type: type[T], field_name: str
) -> tuple[T, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be a sequence of {expected_type.__name__} values")
    return tuple(require_instance(value, expected_type, field_name) for value in values)


def require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def require_int(
    value: object,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} must be at most {maximum}")
    return value


def require_enum[T: Enum](value: object, enum_type: type[T], field_name: str) -> T:
    if not isinstance(value, enum_type):
        raise ValueError(f"{field_name} must be a {enum_type.__name__}")
    return value


def normalize_enum_tuple[T: Enum](
    values: object, enum_type: type[T], field_name: str
) -> tuple[T, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be a sequence of {enum_type.__name__} values")
    return tuple(dict.fromkeys(require_enum(value, enum_type, field_name) for value in values))


def validate_json(value: Any, field_name: str = "payload") -> JSONValue:
    """Validate and defensively copy a JSON value.

    Native object deserialization is intentionally unsupported. Contract payloads
    are plain JSON data, with finite numbers and string object keys.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field_name} must not contain non-finite numbers")
        return value
    if isinstance(value, Mapping):
        result: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field_name} object keys must be strings")
            result[key] = validate_json(item, f"{field_name}.{key}")
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [validate_json(item, f"{field_name}[]") for item in value]
    raise ValueError(f"{field_name} must contain only JSON-compatible values")


def validate_json_mapping(value: Any, field_name: str) -> dict[str, JSONValue]:
    normalized = validate_json(value, field_name)
    if not isinstance(normalized, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return normalized


def to_json_value(value: Any) -> JSONValue:
    """Convert a contract DTO recursively to a JSON-compatible value."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return validate_json(value)
    if isinstance(value, datetime):
        normalized = require_utc(value, "datetime")
        return normalized.isoformat().replace("+00:00", "Z")
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
        return int(seconds) if seconds.is_integer() else seconds
    if isinstance(value, Enum):
        return to_json_value(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: to_json_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return validate_json(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_json_value(item) for item in value]
    raise TypeError(f"cannot convert {type(value).__name__} to JSON")


class JSONDTO:
    """Mixin providing a standard JSON representation for frozen dataclasses."""

    def to_dict(self) -> dict[str, JSONValue]:
        value = to_json_value(self)
        if not isinstance(value, dict):  # pragma: no cover - dataclass invariant
            raise TypeError("a DTO must serialize to a JSON object")
        return value
