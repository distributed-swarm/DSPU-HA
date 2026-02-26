from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import psycopg


LOCK_KEY_DEFAULT = 915_707_001
SCHEMA_LOCK_ID = 7878787878


@dataclass(frozen=True)
class LeaderState:
    node_id: str
    role: str  # "LEADER" | "STANDBY"
    leader_epoch: int | None
    leader_id: str | None


class LeaderElector:
    """
    v0 leader election:
    - Postgres advisory lock (exclusive, session-level)
    - leader_epoch + leader_id stored in dspu_meta
    - background thread holds the lock for the lifetime of the process
    """

    def __init__(self) -> None:
        self._db_url = os.environ["DATABASE_URL"]
        self._node_id = os.getenv("NODE_ID", "node-unknown")
        self._lock_key = int(os.getenv("LEADER_LOCK_KEY", str(LOCK_KEY_DEFAULT)))
        self._poll_s = float(os.getenv("LEADER_POLL_S", "0.5"))
        self._schema_retry_s = float(os.getenv("PG_SCHEMA_RETRY_S", "15"))
        self._schema_retry_interval_s = float(os.getenv("PG_SCHEMA_RETRY_INTERVAL_S", "0.5"))

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._state = LeaderState(
            node_id=self._node_id,
            role="STANDBY",
            leader_epoch=None,
            leader_id=None,
        )

    def start(self) -> None:
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

    # --- schema ---

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
        raise RuntimeError(f"schema init failed after {self._schema_retry_s}s: {last_exc!r}")

    def _ensure_schema_once(self) -> None:
        with psycopg.connect(self._db_url) as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s);", (SCHEMA_LOCK_ID,))
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dspu_meta (
                        k TEXT PRIMARY KEY,
                        v TEXT NOT NULL
                    );
                    """
                )
                row = conn.execute("SELECT v FROM dspu_meta WHERE k='leader_epoch';").fetchone()
                if row is None:
                    conn.execute("INSERT INTO dspu_meta (k, v) VALUES ('leader_epoch', '0');")

    # --- election loop ---

    def _run(self) -> None:
        conn = psycopg.connect(self._db_url)
        conn.autocommit = True

        have_lock = False
        my_epoch: int | None = None

        try:
            while not self._stop.is_set():
                if not have_lock:
                    got = conn.execute(
                        "SELECT pg_try_advisory_lock(%s);", (self._lock_key,)
                    ).fetchone()[0]

                    if got:
                        # became leader — increment epoch and record leader_id
                        with conn.transaction():
                            row = conn.execute(
                                "SELECT v FROM dspu_meta WHERE k='leader_epoch';"
                            ).fetchone()
                            new_epoch = (int(row[0]) if row else 0) + 1
                            conn.execute(
                                "INSERT INTO dspu_meta (k,v) VALUES ('leader_epoch',%s) "
                                "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v;",
                                (str(new_epoch),),
                            )
                            conn.execute(
                                "INSERT INTO dspu_meta (k,v) VALUES ('leader_id',%s) "
                                "ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v;",
                                (self._node_id,),
                            )
                        my_epoch = new_epoch
                        have_lock = True
                        self._set_state(role="LEADER", leader_epoch=my_epoch, leader_id=self._node_id)
                    else:
                        # standby — read current epoch/leader best-effort
                        try:
                            row = conn.execute(
                                "SELECT v FROM dspu_meta WHERE k='leader_epoch';"
                            ).fetchone()
                            epoch = int(row[0]) if row else 0
                            lid = conn.execute(
                                "SELECT v FROM dspu_meta WHERE k='leader_id';"
                            ).fetchone()
                            leader_id = lid[0] if lid else None
                        except Exception:
                            epoch = 0
                            leader_id = None
                        self._set_state(role="STANDBY", leader_epoch=epoch, leader_id=leader_id)
                else:
                    self._set_state(role="LEADER", leader_epoch=my_epoch, leader_id=self._node_id)

                time.sleep(self._poll_s)

        finally:
            if have_lock:
                try:
                    conn.execute("SELECT pg_advisory_unlock(%s);", (self._lock_key,))
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
