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
    e.setdefault("PG_SCHEMA_RETRY_S", "15")
    e.setdefault("PG_SCHEMA_RETRY_INTERVAL_S", "0.5")

    return subprocess.Popen(
        [sys.executable, "-m", "controller"],
        cwd="controller",
        env=e,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def stop_and_dump(p: subprocess.Popen, label: str, timeout_s: float = 2.0) -> None:
    """
    Terminate a process and print its stdout without ever blocking CI.
    """
    try:
        if p.poll() is None:
            p.terminate()
        try:
            out, _ = p.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate(timeout=timeout_s)
        rc = p.returncode
        print(f"\n--- {label} (pid={p.pid}) returncode={rc} ---")
        if out:
            print(out)
        print(f"--- end {label} ---\n")
    except Exception as e:
        print(f"\n--- {label} stop/dump failed: {e!r} ---\n")


def get_role(base: str) -> dict:
    r = httpx.get(f"{base}/role", timeout=2.0)
    r.raise_for_status()
    return r.json()


def test_single_leader_and_not_leader_guard():
    db_url = os.environ["DATABASE_URL"]

    p1 = start_controller("node-a", 18080, db_url)
    p2 = start_controller("node-b", 18081, db_url)

    a = "http://127.0.0.1:18080"
    b = "http://127.0.0.1:18081"

    try:
        # If either controller fails to bind, dump both logs immediately.
        try:
            wait_http(f"{a}/healthz")
            wait_http(f"{b}/healthz")
        except Exception:
            stop_and_dump(p1, "controller-a")
            stop_and_dump(p2, "controller-b")
            raise

        # Wait until one becomes leader
        deadline = time.time() + 10
        leader = None
        standby = None
        last = None

        while time.time() < deadline:
            ra = get_role(a)
            rb = get_role(b)
            last = (ra, rb)

            if {ra["role"], rb["role"]} == {"LEADER", "STANDBY"}:
                if ra["role"] == "LEADER":
                    leader, standby = (a, ra), (b, rb)
                else:
                    leader, standby = (b, rb), (a, ra)
                break

            time.sleep(0.2)

        assert leader is not None, f"never converged to one leader: {last}"

        leader_base, leader_role = leader
        standby_base, standby_role = standby

        assert isinstance(leader_role["leader_epoch"], int)
        assert leader_role["leader_epoch"] >= 1

        # Mutating endpoint works on leader (204)
        rL = httpx.post(
            f"{leader_base}/v1/leases",
            json={"agent": "a", "capabilities": ["echo"]},
            timeout=2.0,
        )
        assert rL.status_code == 204

        # Mutating endpoint rejected on standby (409 NOT_LEADER)
        rS = httpx.post(
            f"{standby_base}/v1/leases",
            json={"agent": "a", "capabilities": ["echo"]},
            timeout=2.0,
        )
        assert rS.status_code == 409
        body = rS.json()
        assert body["error"] == "NOT_LEADER"
        assert body["role"] == "STANDBY"
        assert "node_id" in body

    finally:
        # Always stop+dump so CI shows controller output on failure.
        stop_and_dump(p1, "controller-a")
        stop_and_dump(p2, "controller-b")
