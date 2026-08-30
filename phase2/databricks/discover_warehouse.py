"""List usable Databricks SQL warehouses without printing credentials."""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value.strip().strip("\"'"))


load_env(Path(__file__).parents[2] / ".env")
host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]
request = urllib.request.Request(
    f"{host}/api/2.0/sql/warehouses",
    headers={"Authorization": f"Bearer {token}"},
)
with urllib.request.urlopen(request, timeout=30) as response:
    payload = json.load(response)

warehouses = [
    {
        "id": warehouse["id"],
        "name": warehouse["name"],
        "state": warehouse.get("state"),
        "warehouse_type": warehouse.get("warehouse_type"),
        "enable_serverless_compute": warehouse.get("enable_serverless_compute"),
    }
    for warehouse in payload.get("warehouses", [])
]
print(json.dumps(warehouses, indent=2, sort_keys=True))
