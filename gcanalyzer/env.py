"""Load optional .env from the project root (does not override existing env vars)."""

from __future__ import annotations

import os


def load_dotenv(path: str | None = None) -> None:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = path or os.path.join(root, ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
