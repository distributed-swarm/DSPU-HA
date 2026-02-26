import os
import subprocess
import sys
import time

import httpx
import pytest


def wait_http(url: str, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return
            last = f"{r.status_code} {r.text}"
        except Exception as e:
            last = repr(e)
        time.sleep(0.2)
    raise RuntimeError(f"Service not ready at {url}: {last}")


def start_controller(env: dict, port: int) -> subprocess.Popen:
    e = os.environ.copy()
    e.update(env)
    e["PORT"] = str(port)

    # Run controller module from the controller package directory
    return subprocess.Popen(
        [sys.executable, "-m", "controller"],
        cwd="controller",
        env=e,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


@pytest.mark.parametrize(
    "role, expected_status, expected_error",
    [
        ("STANDBY", 409, "NOT_LEADER"),
        ("LEADER", 204, None),
    ],
)
def test_mutating_endpoint_guard(role, expected_status, expected_error):
    port = 18080 if role == "STANDBY" else 18081
    base = f"http://127.0.0.1:{port}"

    p = start_controller(
        env={
            "ROLE": role,
            "NODE_ID": f"node-{role.lower()}",
            "LEADER_EPOCH": "1",
            "LEADER_ID": "node-leader",
            "LEADER_URL": "http://127.0.0.1:18081",
        },
        port=port,
    )

    try:
        wait_http(f"{base}/healthz")

        r_role = httpx.get(f"{base}/role")
        assert r_role.status_code == 200
        j = r_role.json()
        assert j["role"] == role
        assert j["node_id"].startswith("node-")

        r = httpx.post(f"{base}/v1/leases", json={"agent": "a", "capabilities": ["echo"]})
        assert r.status_code == expected_status

        if expected_error:
            body = r.json()
            assert body["error"] == expected_error
            assert body["role"] == role
            assert "node_id" in body
    finally:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
