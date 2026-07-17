from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from kimball.contracts.compatibility import (
    ContractCompatibilityReport,
    check_compatibility,
)
from kimball.contracts.odcs import ODCSContract, ODCSContractLoader


@dataclass(frozen=True)
class ContractGateResult:
    allowed: bool
    errors: tuple[str, ...]
    reports: tuple[ContractCompatibilityReport, ...]


def _semver(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def _index_contracts(
    contracts: Iterable[ODCSContract], errors: list[str]
) -> dict[str, dict[str, ODCSContract]]:
    indexed: dict[str, dict[str, ODCSContract]] = {}
    for contract in contracts:
        versions = indexed.setdefault(contract.id, {})
        existing = versions.get(contract.version)
        if existing and existing.digest != contract.digest:
            errors.append(
                f"Contract '{contract.id}' version {contract.version} has conflicting definitions"
            )
        else:
            versions[contract.version] = contract
    return indexed


def evaluate_contract_changes(
    previous: Iterable[ODCSContract], current: Iterable[ODCSContract]
) -> ContractGateResult:
    """Evaluate the latest producer versions while retaining all immutable pins."""

    errors: list[str] = []
    previous_by_id = _index_contracts(previous, errors)
    current_by_id = _index_contracts(current, errors)
    reports: list[ContractCompatibilityReport] = []

    for contract_id in sorted(previous_by_id):
        if contract_id not in current_by_id:
            errors.append(f"Contract '{contract_id}' was removed")
            continue
        old_versions = previous_by_id[contract_id]
        new_versions = current_by_id[contract_id]
        old = max(old_versions.values(), key=lambda item: _semver(item.version))
        new = max(new_versions.values(), key=lambda item: _semver(item.version))
        if new.digest == old.digest:
            continue
        report = check_compatibility(old, new)
        reports.append(report)
        if not report.allowed:
            errors.append(
                f"Contract '{contract_id}' {old.version} -> {new.version} is not compatible"
            )

    return ContractGateResult(not errors, tuple(errors), tuple(reports))


def load_contract_directory(path: str | Path) -> list[ODCSContract]:
    root = Path(path)
    if not root.exists():
        return []
    loader = ODCSContractLoader()
    paths = sorted([*root.rglob("*.odcs.yaml"), *root.rglob("*.odcs.yml")])
    return [loader.load_file(contract_path) for contract_path in paths]
