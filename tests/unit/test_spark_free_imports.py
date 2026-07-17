from __future__ import annotations

import json
import subprocess
import sys


def test_package_and_configuration_import_without_pyspark() -> None:
    code = """
import builtins, json, sys
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'pyspark' or name.startswith('pyspark.') or name == 'delta' or name.startswith('delta.'):
        raise ModuleNotFoundError(name)
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
import kimball
from kimball.common.config import ConfigLoader
print(json.dumps({'spark_loaded': any(n == 'pyspark' or n.startswith('pyspark.') for n in sys.modules), 'loader': ConfigLoader.__name__, 'exports': 'Orchestrator' in kimball.__all__}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "spark_loaded": False,
        "loader": "ConfigLoader",
        "exports": True,
    }
