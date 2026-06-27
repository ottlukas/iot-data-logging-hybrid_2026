"""
Docker Health Tests

Tests that verify IoTDB connectivity from within the runtime environment.
When running inside a Docker network (detected by /.dockerenv or hostname resolution),
failures are strict. Otherwise, tests skip gracefully to avoid false CI failures.
"""

import os
import socket
import pytest

from app.config import settings


def _is_docker_env() -> bool:
    """Return True when the test suite is running inside a Docker container."""
    if os.path.exists("/.dockerenv"):
        return True
    try:
        socket.getaddrinfo("iotdb", None)
        return True
    except socket.gaierror:
        return False


def _can_reach(host: str, port: int, timeout: float = 3.0) -> bool:
    """Try a raw TCP connect to *host:port*."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


class TestDockerHealth:
    """IoTDB reachability tests, sensitive to the runtime environment."""

    def test_iotdb_tcp_reachable(self):
        """IoTDB RPC port should be reachable from the FastAPI service."""
        host = settings.IOTDB_HOST
        port = settings.IOTDB_PORT

        reachable = _can_reach(host, port)

        if _is_docker_env():
            # Inside Docker the service MUST be available
            assert reachable, (
                f"IoTDB at {host}:{port} is unreachable from inside the Docker network. "
                "Check that the 'iotdb' service is healthy in docker-compose."
            )
        else:
            if not reachable:
                pytest.skip(
                    f"IoTDB at {host}:{port} is not running locally – "
                    "skipping (not inside Docker)."
                )

    def test_iotdb_port_matches_compose(self):
        """Sanity-check that settings.IOTDB_PORT is the expected default."""
        assert settings.IOTDB_PORT == 6667, (
            f"Expected IoTDB RPC port 6667, got {settings.IOTDB_PORT}. "
            "Ensure IOTDB_PORT env var matches docker-compose.yml."
        )

    def test_iotdb_host_resolves(self):
        """In Docker, 'iotdb' hostname must resolve to a container IP."""
        host = settings.IOTDB_HOST

        if not _is_docker_env():
            pytest.skip("Not inside Docker – hostname resolution test skipped.")

        try:
            addrs = socket.getaddrinfo(host, None)
            assert len(addrs) > 0, f"DNS resolved '{host}' but returned no addresses."
        except socket.gaierror:
            pytest.fail(
                f"Cannot resolve hostname '{host}' inside Docker. "
                "Verify that both services share the same network in docker-compose.yml."
            )

    def test_fastapi_health_endpoint_responds(self, tmp_path, monkeypatch):
        """The /health endpoint must respond inside the container."""
        from fastapi.testclient import TestClient
        from app.main import app
        from app.storage import buffer_store

        monkeypatch.setattr(settings, "LOCAL_TSFILE_PATH", str(tmp_path / "buf.tsfile"))
        monkeypatch.setattr(settings, "LOCAL_ARCHIVE_DIR", str(tmp_path / "archive"))
        monkeypatch.setattr(settings, "LOCAL_INDEX_FILE", str(tmp_path / "index.json"))
        monkeypatch.setattr(buffer_store, "buffer_path", tmp_path / "buf.tsfile")

        with TestClient(app) as client:
            response = client.get("/health")
            assert response.status_code == 200
            body = response.json()
            assert body.get("status") == "healthy"
