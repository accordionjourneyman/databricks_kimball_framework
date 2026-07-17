"""Versioned producer/consumer data-contract support."""

from kimball.contracts.compatibility import (
    ContractChange,
    ContractCompatibilityReport,
    check_compatibility,
)
from kimball.contracts.odcs import (
    ODCSContract,
    ODCSContractLoader,
    adapt_odcs_to_source_contract,
)

__all__ = [
    "ContractChange",
    "ContractCompatibilityReport",
    "ODCSContract",
    "ODCSContractLoader",
    "adapt_odcs_to_source_contract",
    "check_compatibility",
]
