"""Unit tests for tools/run_databricks_tests.py."""

import importlib.util
import io
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RUNNER_PATH = REPO_ROOT / "tools" / "run_databricks_tests.py"


def _load_runner_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("run_databricks_tests", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_databricks_tests"] = module
    spec.loader.exec_module(module)
    return module


runner = _load_runner_module()


class FakeUser:
    def __init__(self, user_name: str):
        self.user_name = user_name
        self.display_name = user_name


class FakeCurrentUser:
    def __init__(self, user_name: str):
        self._user = FakeUser(user_name)

    def me(self):
        return self._user


class FakeFiles:
    def __init__(self):
        self.uploads: list[tuple[str, bytes, bool]] = []
        self.dirs: list[str] = []

    def create_directory(self, directory_path: str) -> None:
        self.dirs.append(directory_path)

    def upload(
        self, file_path: str, contents: io.BytesIO, *, overwrite: bool = False
    ) -> None:
        data = contents.getvalue() if contents else b""
        self.uploads.append((file_path, data, overwrite))


class FakeJobs:
    def __init__(self, success: bool = True):
        self._success = success
        self.submissions: list[dict] = []

    def submit(self, **kwargs):
        self.submissions.append(kwargs)

        class _Run:
            run_id = 42

        return _Run()

    def get_run(self, run_id: int):
        assert run_id == 42

        class _State:
            class life_cycle_state:
                value = "TERMINATED"

            class result_state:
                value = "SUCCESS" if self._success else "FAILED"

        class _Run:
            state = _State()

        return _Run()

    def get_run_output(self, run_id: int):
        class _Output:
            logs = "pytest output"

        return _Output()


class FakeWorkspaceClient:
    def __init__(self, user_name: str = "tester@example.com", success: bool = True):
        self.files = FakeFiles()
        self.jobs = FakeJobs(success=success)
        self._current_user = FakeCurrentUser(user_name)

    @property
    def current_user(self):
        return self._current_user


@pytest.fixture(autouse=True)
def _clear_env():
    """Clear workspace-dir override between tests."""
    orig = os.environ.pop("KIMBALL_WORKSPACE_DIR", None)
    yield
    if orig is not None:
        os.environ["KIMBALL_WORKSPACE_DIR"] = orig
    else:
        os.environ.pop("KIMBALL_WORKSPACE_DIR", None)


def test_get_remote_base_dir_env_override():
    os.environ["KIMBALL_WORKSPACE_DIR"] = "/Workspace/Users/custom/kimball_ci"
    ws = FakeWorkspaceClient()
    assert runner._get_remote_base_dir(ws) == "/Workspace/Users/custom/kimball_ci"


def test_get_remote_base_dir_from_current_user():
    ws = FakeWorkspaceClient("tester@example.com")
    assert (
        runner._get_remote_base_dir(ws)
        == "/Workspace/Users/tester@example.com/kimball_framework_ci"
    )


def test_get_remote_base_dir_missing_user_exits():
    ws = FakeWorkspaceClient("")
    ws._current_user = None
    with pytest.raises(SystemExit) as exc_info:
        runner._get_remote_base_dir(ws)
    assert exc_info.value.code == 1


def test_upload_wheel_creates_dir_and_uploads(tmp_path: Path):
    wheel = tmp_path / "kimball-1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel-data")
    ws = FakeWorkspaceClient("tester@example.com")

    remote_path = runner._upload_wheel(wheel, ws)

    assert (
        remote_path
        == "/Workspace/Users/tester@example.com/kimball_framework_ci/kimball-1.0-py3-none-any.whl"
    )
    assert "/Workspace/Users/tester@example.com/kimball_framework_ci" in ws.files.dirs
    assert len(ws.files.uploads) == 1
    file_path, data, overwrite = ws.files.uploads[0]
    assert file_path == remote_path
    assert data == b"wheel-data"
    assert overwrite is True


def test_sync_tests_uploads_source_files_and_skips_pycache(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(runner, "REPO_ROOT", tmp_path)
    tests_dir = tmp_path / "tests"
    (tests_dir / "unit").mkdir(parents=True)
    (tests_dir / "__pycache__").mkdir()
    (tests_dir / "unit" / "test_a.py").write_text("print(1)")
    (tests_dir / "__pycache__" / "test_a.cpython-311.pyc").write_bytes(b"pyc")

    ws = FakeWorkspaceClient("tester@example.com")
    runner._sync_tests("/Workspace/Users/tester@example.com/tests", ws)

    paths = [up[0] for up in ws.files.uploads]
    assert any("test_a.py" in p for p in paths)
    assert not any(".pyc" in p for p in paths)


def test_create_runner_script_uploads_and_returns_path():
    ws = FakeWorkspaceClient("tester@example.com")
    runner_path = runner._create_runner_script(
        ws, "/Workspace/Users/tester@example.com/tests"
    )

    assert (
        runner_path
        == "/Workspace/Users/tester@example.com/kimball_framework_ci/run_tests.py"
    )
    assert runner_path in [up[0] for up in ws.files.uploads]
    script_bytes = next(up[1] for up in ws.files.uploads if up[0] == runner_path)
    script = script_bytes.decode("utf-8")
    assert "import pytest" in script
    assert 'pytest.main([test_path, "-v"])' in script


def test_run_job_submits_and_polls_successfully():
    ws = FakeWorkspaceClient("tester@example.com", success=True)
    result = runner._run_job(
        ws,
        None,
        "/Workspace/Users/tester@example.com/kimball_framework_ci/kimball-0.1.0.whl",
        "/Workspace/Users/tester@example.com/kimball_framework_ci/run_tests.py",
        "/Workspace/Users/tester@example.com/tests/golden",
        "spark_catalog",
    )
    assert result == 0
    assert len(ws.jobs.submissions) == 1
    task = ws.jobs.submissions[0]["tasks"][0]
    assert task.task_key == "run_tests"


def test_run_job_reports_failure():
    ws = FakeWorkspaceClient("tester@example.com", success=False)
    result = runner._run_job(
        ws,
        None,
        "/path/wheel.whl",
        "/path/run_tests.py",
        "/path/tests",
        "spark_catalog",
    )
    assert result == 1


def test_load_env_file_skips_missing_file():
    runner._load_env_file("/nonexistent/.env")
    # should not raise


def test_ensure_credentials_exits_when_missing():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(SystemExit) as exc_info:
            runner._ensure_credentials()
        assert exc_info.value.code == 1
