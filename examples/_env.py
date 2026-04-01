"""Shared environment helpers for example scripts.

Loads aerospike.env (or aerospike.env.example) from the repo root so that
examples pick up the same connection settings as pytest without requiring
the user to manually export variables.
"""

import os
from pathlib import Path

def _load_env_file(path: Path, *, override: bool = True) -> None:
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if override or key not in os.environ:
                    os.environ[key] = value


def _ensure_env() -> None:
    root = Path(__file__).resolve().parent.parent
    env_local = root / "aerospike.env"
    env_example = root / "aerospike.env.example"
    if env_local.exists():
        _load_env_file(env_local, override=False)
    elif env_example.exists():
        _load_env_file(env_example, override=False)


_ensure_env()


def _host_and_port() -> tuple[str, int]:
    host = os.environ.get("AEROSPIKE_HOST", "localhost:3000")
    if ":" in host:
        hostname, port_str = host.split(":", 1)
        return hostname, int(port_str)
    return host, 3000


def _services_alternate() -> bool:
    return os.environ.get(
        "AEROSPIKE_USE_SERVICES_ALTERNATE", "",
    ).lower() in ("true", "1", "yes")


def connect():
    """Build an async ClusterDefinition from environment variables."""
    from aerospike_fluent import ClusterDefinition

    hostname, port = _host_and_port()
    defn = ClusterDefinition(hostname, port)
    if _services_alternate():
        defn = defn.using_services_alternate()
    return defn


def sync_connect():
    """Build a sync ClusterDefinition from environment variables."""
    from aerospike_fluent.sync import ClusterDefinition

    hostname, port = _host_and_port()
    defn = ClusterDefinition(hostname, port)
    if _services_alternate():
        defn = defn.using_services_alternate()
    return defn
