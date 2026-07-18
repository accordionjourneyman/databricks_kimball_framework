import pytest
from pydantic import ValidationError

from kimball.common.config import NullPolicyConfig
from kimball.common.constants import DEFAULT_MEMBERS, RESERVED_DIMENSION_KEYS


def test_default_member_registry_is_stable_and_exhaustive() -> None:
    assert DEFAULT_MEMBERS == {
        -1: ("MISSING", "Missing"),
        -2: ("NOT_APPLICABLE", "Not Applicable"),
        -3: ("NOT_YET_AVAILABLE", "Not Yet Available"),
        -4: ("BAD_VALUE", "Bad Value"),
    }
    assert RESERVED_DIMENSION_KEYS == frozenset(DEFAULT_MEMBERS)


def test_null_policy_defaults_to_strict_kimball_semantics() -> None:
    policy = NullPolicyConfig()

    assert policy.mode == "kimball"
    assert policy.attribute_substitutes == {}


def test_blank_string_substitute_is_rejected() -> None:
    with pytest.raises(ValidationError, match="attribute_substitutes"):
        NullPolicyConfig(attribute_substitutes={"city": ""})
