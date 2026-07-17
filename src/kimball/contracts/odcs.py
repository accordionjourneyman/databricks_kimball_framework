from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from jsonschema import Draft201909Validator, FormatChecker

from kimball.common.config import (
    ContractCDCConfig,
    ContractColumnConfig,
    ContractQualityRule,
    SourceContractConfig,
)

_SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
_LOGICAL_TO_SPARK = {
    "string": "STRING",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "STRING",
    "array": "ARRAY",
    "object": "STRUCT",
}


def _canonical_digest(document: dict[str, Any]) -> str:
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ODCSProperty:
    name: str
    logical_type: str | None
    physical_type: str | None
    required: bool
    primary_key: bool
    primary_key_position: int
    document: dict[str, Any]


@dataclass(frozen=True)
class ODCSObject:
    name: str
    properties: dict[str, ODCSProperty]
    document: dict[str, Any]

    @property
    def primary_key(self) -> tuple[str, ...]:
        keys = [prop for prop in self.properties.values() if prop.primary_key]
        return tuple(
            prop.name
            for prop in sorted(
                keys,
                key=lambda prop: (
                    prop.primary_key_position
                    if prop.primary_key_position >= 1
                    else 2**31,
                    prop.name,
                ),
            )
        )


@dataclass(frozen=True)
class ODCSContract:
    id: str
    version: str
    api_version: str
    status: str
    objects: dict[str, ODCSObject]
    document: dict[str, Any]
    digest: str

    def select_object(self, object_name: str | None = None) -> ODCSObject:
        if object_name:
            short_name = object_name.split(".")[-1]
            if object_name in self.objects:
                return self.objects[object_name]
            if short_name in self.objects:
                return self.objects[short_name]
            raise ValueError(
                f"Contract '{self.id}' does not define schema object '{object_name}'"
            )
        if len(self.objects) != 1:
            raise ValueError(
                f"Contract '{self.id}' has {len(self.objects)} objects; object_name is required"
            )
        return next(iter(self.objects.values()))


class ODCSContractLoader:
    """Validate canonical ODCS v3.1.0 documents using the pinned schema."""

    def __init__(self, schema_path: str | Path | None = None):
        self.schema_path = (
            Path(schema_path)
            if schema_path
            else (Path(__file__).parent / "schemas" / "odcs-v3.1.0.json")
        )
        self.schema = json.loads(self.schema_path.read_text(encoding="utf-8"))
        self.validator = Draft201909Validator(
            self.schema, format_checker=FormatChecker()
        )

    def load_file(self, path: str | Path) -> ODCSContract:
        contract_path = Path(path)
        document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError(f"ODCS contract must be a mapping: {contract_path}")
        contract = self.load_mapping(document)
        validate_contract_reference(str(contract_path), contract)
        return contract

    def load_mapping(self, document: dict[str, Any]) -> ODCSContract:
        document = deepcopy(document)
        self.validator.validate(document)
        if document["apiVersion"] != "v3.1.0":
            raise ValueError("Only canonical ODCS apiVersion v3.1.0 is supported")
        if not _SEMVER.fullmatch(document["version"]):
            raise ValueError("ODCS contract version must be semantic x.y.z")

        objects: dict[str, ODCSObject] = {}
        for raw_object in document.get("schema", []):
            properties: dict[str, ODCSProperty] = {}
            for raw_property in raw_object.get("properties", []):
                prop = ODCSProperty(
                    name=raw_property["name"],
                    logical_type=raw_property.get("logicalType"),
                    physical_type=raw_property.get("physicalType"),
                    required=bool(raw_property.get("required", False)),
                    primary_key=bool(raw_property.get("primaryKey", False)),
                    primary_key_position=int(
                        raw_property.get("primaryKeyPosition", -1)
                    ),
                    document=raw_property,
                )
                if prop.name in properties:
                    raise ValueError(
                        f"Duplicate property '{prop.name}' in object '{raw_object['name']}'"
                    )
                properties[prop.name] = prop
            obj = ODCSObject(raw_object["name"], properties, raw_object)
            if obj.name in objects:
                raise ValueError(f"Duplicate ODCS schema object '{obj.name}'")
            objects[obj.name] = obj

        return ODCSContract(
            id=document["id"],
            version=document["version"],
            api_version=document["apiVersion"],
            status=document["status"],
            objects=objects,
            document=document,
            digest=_canonical_digest(document),
        )


def _severity(rule: dict[str, Any]) -> Literal["warn", "error"]:
    return "warn" if rule.get("severity") in ("warning", "warn", "info") else "error"


def _adapt_quality(obj: ODCSObject) -> list[ContractQualityRule]:
    rules: list[ContractQualityRule] = []
    for prop in obj.properties.values():
        for quality in prop.document.get("quality", []):
            metric = quality.get("metric")
            if metric == "nullValues" and quality.get("mustBe") == 0:
                rules.append(
                    ContractQualityRule(
                        name=quality.get("id"),
                        rule="not_null",
                        column=prop.name,
                        severity=_severity(quality),
                    )
                )
            elif metric == "duplicateValues" and quality.get("mustBe") == 0:
                rules.append(
                    ContractQualityRule(
                        name=quality.get("id"),
                        rule="unique",
                        column=prop.name,
                        severity=_severity(quality),
                    )
                )
            elif (
                metric == "invalidValues"
                and quality.get("mustBe") == 0
                and "validValues" in quality.get("arguments", {})
            ):
                rules.append(
                    ContractQualityRule(
                        name=quality.get("id"),
                        rule="accepted_values",
                        column=prop.name,
                        values=quality["arguments"]["validValues"],
                        severity=_severity(quality),
                    )
                )
    for quality in obj.document.get("quality", []):
        columns = quality.get("arguments", {}).get("properties")
        if (
            quality.get("metric") == "duplicateValues"
            and quality.get("mustBe") == 0
            and columns
        ):
            rules.append(
                ContractQualityRule(
                    name=quality.get("id"),
                    rule="unique",
                    columns=columns,
                    severity=_severity(quality),
                )
            )
    return rules


def _custom_properties(document: dict[str, Any]) -> dict[str, Any]:
    return {
        item["property"]: item["value"] for item in document.get("customProperties", [])
    }


def adapt_odcs_to_source_contract(
    contract: ODCSContract, *, object_name: str | None = None
) -> SourceContractConfig:
    """Adapt ODCS to the existing executable consumer checks."""

    obj = contract.select_object(object_name)
    custom = _custom_properties(contract.document)
    schema = {
        prop.name: ContractColumnConfig(
            type=prop.physical_type
            or _LOGICAL_TO_SPARK.get(prop.logical_type or "", "STRING"),
            nullable=not prop.required,
            required=True,
        )
        for prop in obj.properties.values()
    }
    return SourceContractConfig(
        id=contract.id,
        version=contract.version,
        owner=custom.get("kimballOwner"),
        compatibility=custom.get("kimballCompatibility", "nullable_additions"),
        schema=schema,
        cdc=ContractCDCConfig(
            required=bool(custom.get("kimballCdfRequired", False)),
            primary_key=list(obj.primary_key),
        ),
        quality=_adapt_quality(obj),
    )


def validate_contract_reference(reference: str, contract: ODCSContract) -> None:
    normalized = reference.replace("\\", "/")
    endings = (
        f"/{contract.id}/{contract.version}.odcs.yaml",
        f"/{contract.id}/{contract.version}.odcs.yml",
    )
    if not any(("/" + normalized.lstrip("/")).endswith(end) for end in endings):
        raise ValueError(
            f"Contract reference for {contract.id} {contract.version} must end with "
            f"contracts/{contract.id}/{contract.version}.odcs.yaml"
        )
