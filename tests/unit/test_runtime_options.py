from __future__ import annotations

import pytest

from kimball.common.runtime import RuntimeOptions


def test_removed_validation_skip_environment_variable_fails_closed(monkeypatch):
    monkeypatch.setenv("KIMBALL_SKIP_VALIDATION_IF_UNCHANGED", "1")

    with pytest.raises(ValueError, match="was removed"):
        RuntimeOptions.from_environment()
