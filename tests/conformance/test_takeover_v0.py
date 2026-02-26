import os
import subprocess
import sys
import time

import httpx


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


def start_controller(node_id: str, port: int, db_url: str) -> subprocess.Popen:
    e = os.environ.copy()
    e["PORT"] = str(port)
    e["NODE_ID"] = node_id
    e["DATABASE_URL"] = db_url
    e["LEADER_POLL_S"] = "0.2"
    return subprocess.Popen(
        [sys.executable, "-m", "controller"],
        cwd="controller",
        env=e,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def get_role(base: str) -> dict:
    r = httpx.get(f"{base}/role", timeout=2.0)
    r.raise_for_status()
    return r.json()


def post_lease(base: str) -> httpx.Response:
    return httpx.post(
        f"{base}/v1/leases",
        json={"agent": "a", "capabilities": ["echo"]},
        timeout=2.0,
    )


def wait_for_roles(a: str, b: str, timeout_s: float = 10.0):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        ra = get_role(a)
        rb = get_role(b)
        last = (ra, rb)
        if {ra["role"], rb["role"]} == {"LEADER", "STANDBY"}:
            if ra["role"] == "LEADER":
                return (a, ra), (b, rb)
            return (b, rb), (a, ra)
        time.sleep(0.2)
    raise AssertionError(f"never converged to one leader: {last}")


def test_leader_takeover_increments_epoch():
    db_url = os.environ["DATABASE_URL"]

    p1 = start_controller("node-a", 18080, db_url)
    p2 = start_controller("node-b", 18081, db_url)

    a = "http://127.0.0.1:18080"
    b = "http://127.0.0.1:18081"

    try:
        wait_http(f"{a}/healthz")
        wait_http(f"{b}/healthz")

        (leader_base, leader_role), (standby_base, standby_role) = wait_for_roles(a, b)
        epoch1 = leader_role["leader_epoch"]
        assert isinstance(epoch1, int) and epoch1 >= 1

        # sanity: leader mutates, standby rejects
        assert post_lease(leader_base).status_code == 204
        rS = post_lease(standby_base)
        assert rS.status_code == 409
        assert rS.json()["error"] == "NOT_LEADER"

        # kill leader
        leader_proc = p1 if leader_base.endswith(":18080") else p2
        leader_proc.terminate()
        try:
            leader_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            leader_proc.kill()

        # standby should become leader, epoch should increase
        # (we only have one process alive now; check it becomes leader)
        deadline = time.time() + 10
        last = None
        while time.time() < deadline:
            r = get_role(standby_base)
            last = r
            if r["role"] == "LEADER":
                epoch2 = r["leader_epoch"]
                assert isinstance(epoch2, int)
                assert epoch2 > epoch1
                break
            time.sleep(0.2)
        else:
            raise AssertionError(f"standby never became leader: {last}")

        # new leader can mutate
        assert post_lease(standby_base).status_code == 204

    finally:
        for p in (p1, p2):
            if p.poll() is None:
                p.terminate()
        for p in (p1, p2):
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
