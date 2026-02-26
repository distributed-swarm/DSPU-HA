from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import psycopg


LOCK_KEY_DEFAULT = 915_707_001  # stable magic number; v0 uses constant


@dataclass(frozen=True)
class LeaderState:
    node_id: str
    role: str  # "LEADER" | "STANDBY"
    leader_epoch: int | None
    leader_id: str | None


class LeaderElector:
    """
    v0 leader election:
    - Postgres advisory lock (exclusive)
    - leader_epoch stored in table dspu_meta
    - polling loop updates in-memory state
    """

    def __init__(self) -> None:
        # Required
        self._db_url = os.environ["DATABASE_URL"]

        # Optional
        self._node_id = os.getenv("NODE_ID", "node-unknown")
        self._lock_key = int(os.getenv("LEADER_LOCK_KEY", str(LOCK_KEY_DEFAULT)))
        self._poll_s = float(os.getenv("LEADER_POLL_S", "0.5"))

        # Retry knobs (important for CI + real boot)
        self._schema_retry_s = float(os.getenv("PG_SCHEMA_RETRY_S", "10"))
        self._schema_retry_interval_s = float(os.getenv("PG_SCHEMA_RETRY_INTERVAL_S", "0.5"))

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = LeaderState(
            node_id=self._node_id,
            role="STANDBY",
            leader_epoch=None,
            leader_id=None,
        )
        self._state_lock = threading.Lock()

    def start(self) -> None:
        # Ensure DB is reachable and meta table exists before starting loop
        self._ensure_schema_with_retry()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def state(self) -> LeaderState:
        with self._state_lock:
            return self._state

    # --- internals ---

    def _ensure_schema_with_retry(self) -> None:
        deadline = time.time() + self._schema_retry_s
        last_exc: Exception | None = None

        while time.time() < deadline:
            try:
                self._ensure_schema_once()
                return
            except Exception as e:
                last_exc = e
                time.sleep(self._schema_retry_interval_s)

        raise RuntimeError(f"Could not initialize Postgres schema within {self._schema_retry_s}s: {last_exc}")

    def _ensure_schema_once(self) -> None:
        with psycopg.connect(self._db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dspu_meta (
                        k TEXT PRIMARY KEY,
                        v TEXT NOT NULL
                    );
                    """
                )

                # initialize leader_epoch if missing
                cur.execute("SELECT v FROM dspu_meta WHERE k='leader_epoch'")
                row = cur.fetchone()
                if row is None:
                    cur.execute("INSERT INTO dspu_meta (k, v) VALUES ('leader_epoch', '0')")
            conn.commit()

    def _get_epoch(self, conn) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM dspu_meta WHERE k='leader_epoch'")
            (v,) = cur.fetchone()
            return int(v)

    def _set_epoch(self, conn, epoch: int) -> None:
        with conn.cursor() as cur:
            cur.execute("UPDATE dspu_meta SET v=%s WHERE k='leader_epoch'", (str(epoch),))

    def _try_lock(self, conn) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (self._lock_key,))
            (ok,) = cur.fetchone()
            return bool(ok)

    def _unlock(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (self._lock_key,))
            cur.fetchone()

    def _run(self) -> None:
        # Keep a persistent connection so the advisory lock is held by session.
        # If the process dies, the session dies, lock is released.
        conn = psycopg.connect(self._db_url)
        conn.autocommit = True

        have_lock = False
        my_epoch: int | None = None

        try:
            while not self._stop.is_set():
                if not have_lock:
                    if self._try_lock(conn):
                        # became leader: increment epoch
                        epoch = self._get_epoch(conn) + 1
                        self._set_epoch(conn, epoch)
                        my_epoch = epoch
                        have_lock = True
                        self._set_state(role="LEADER", leader_epoch=epoch, leader_id=self._node_id)
                    else:
                        # remain standby: read current epoch for introspection
                        try:
                            epoch = self._get_epoch(conn)
                        except Exception:
                            epoch = 0
                        self._set_state(role="STANDBY", leader_epoch=epoch, leader_id=None)
                else:
                    # leader: keep advertising
                    self._set_state(role="LEADER", leader_epoch=my_epoch, leader_id=self._node_id)

                time.sleep(self._poll_s)

        finally:
            if have_lock:
                try:
                    self._unlock(conn)
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass

    def _set_state(self, *, role: str, leader_epoch: int | None, leader_id: str | None) -> None:
        with self._state_lock:
            self._state = LeaderState(
                node_id=self._node_id,
                role=role,
                leader_epoch=leader_epoch,
                leader_id=leader_id,
            )
