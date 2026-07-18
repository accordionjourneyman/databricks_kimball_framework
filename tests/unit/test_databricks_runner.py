"""Unit tests for tools/run_databricks_tests.py."""

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RUNNER_PATH = REPO_ROOT / "tools" / "run_databricks_tests.py"


def _install_databricks_sdk_mock() -> None:
    """Provide a minimal stub for databricks.sdk so unit tests don't need it installed."""
    if "databricks.sdk.service.workspace" in sys.modules:
        return

    databricks_pkg = ModuleType("databricks")
    sdk_pkg = ModuleType("databricks.sdk")
    service_pkg = ModuleType("databricks.sdk.service")
    jobs_mod = ModuleType("databricks.sdk.service.jobs")
    compute_mod = ModuleType("databricks.sdk.service.compute")

    # Classes used by run_databricks_tests.py
    for name in [
        "SubmitTask",
        "SparkPythonTask",
        "Task",
        "JobEnvironment",
        "JobSettings",
    ]:
        setattr(
            jobs_mod,
            name,
            type(
                name,
                (),
                {"__init__": lambda self, **kwargs: setattr(self, "_kwargs", kwargs)},
            ),
        )

    class _PerformanceTarget:
        STANDARD = "STANDARD"

    jobs_mod.PerformanceTarget = _PerformanceTarget

    compute_mod.Environment = type(
        "Environment",
        (),
        {"__init__": lambda self, **kwargs: setattr(self, "_kwargs", kwargs)},
    )
    for name in ["Library", "PythonPyPiLibrary"]:
        setattr(
            compute_mod,
            name,
            type(
                name,
                (),
                {"__init__": lambda self, **kwargs: setattr(self, "_kwargs", kwargs)},
            ),
        )

    workspace_mod = ModuleType("databricks.sdk.service.workspace")

    class _ImportFormat:
        AUTO = "AUTO"

    workspace_mod.ImportFormat = _ImportFormat

    service_pkg.jobs = jobs_mod
    service_pkg.compute = compute_mod
    service_pkg.workspace = workspace_mod
    databricks_pkg.sdk = sdk_pkg

    sys.modules["databricks"] = databricks_pkg
    sys.modules["databricks.sdk"] = sdk_pkg
    sys.modules["databricks.sdk.service"] = service_pkg
    sys.modules["databricks.sdk.service.jobs"] = jobs_mod
    sys.modules["databricks.sdk.service.compute"] = compute_mod
    sys.modules["databricks.sdk.service.workspace"] = workspace_mod


def _load_runner_module() -> ModuleType:
    _install_databricks_sdk_mock()
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


class FakeWorkspace:
    def __init__(self):
        self.uploads: list[tuple[str, bytes, Any, bool]] = []
        self.dirs: list[str] = []

    def mkdirs(self, path: str) -> None:
        self.dirs.append(path)

    def upload(
        self, path: str, content: bytes, *, format=None, overwrite: bool = False
    ) -> None:
        data = content if content else b""
        self.uploads.append((path, data, format, overwrite))


class FakeJobs:
    def __init__(self, success: bool = True):
        self._success = success
        self.submissions: list[dict] = []
        self._jobs: dict[int, dict] = {}
        self._next_job_id = 100

    def submit(self, **kwargs):
        self.submissions.append(kwargs)

        class _Run:
            run_id = 42

        return _Run()

    def create(self, **kwargs):
        job_id = self._next_job_id
        self._next_job_id += 1
        self._jobs[job_id] = kwargs
        resp = type("CreateResponse", (), {"job_id": job_id})
        return resp()

    def update(self, job_id: int, *, new_settings=None) -> None:
        if new_settings:
            self._jobs[job_id] = getattr(new_settings, "_kwargs", new_settings)

    def list(self):
        for job_id, settings in self._jobs.items():
            yield type(
                "Job",
                (),
                {"job_id": job_id, "settings": type("JobSettings", (), settings)()},
            )()

    def run_now(self, job_id: int, **kwargs):
        self.submissions.append({"run_now": job_id})

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
        self.workspace = FakeWorkspace()
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
    assert (
        "/Workspace/Users/tester@example.com/kimball_framework_ci" in ws.workspace.dirs
    )
    assert len(ws.workspace.uploads) == 1
    file_path, data, fmt, overwrite = ws.workspace.uploads[0]
    assert file_path == remote_path
    assert data == b"wheel-data"
    assert fmt is not None
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

    paths = [up[0] for up in ws.workspace.uploads]
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
    assert runner_path in [up[0] for up in ws.workspace.uploads]
    script_bytes = next(up[1] for up in ws.workspace.uploads if up[0] == runner_path)
    script = script_bytes.decode("utf-8")
    assert "import pytest" in script
    assert "sys.dont_write_bytecode = True" in script
    assert "pytest.main([test_path" in script


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
    # Serverless path creates a persistent job and triggers run_now.
    assert any("run_now" in s for s in ws.jobs.submissions)
    job_ids = [job.job_id for job in ws.jobs.list()]
    assert len(job_ids) == 1


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
