import os
import time
from dataclasses import dataclass
from typing import Optional

import psycopg


# One global lock id for schema init. Any constant int8 is fine.
# Pick something stable and boring.
SCHEMA_LOCK_ID = 7878787878


@dataclass(frozen=True)
class RoleState:
    role: str  # "LEADER" | "STANDBY"
    node_id: str
    leader_id: str
    leader_epoch: int


def _now_ms() -> int:
    return int(time.time() * 1000)


def ensure_schema_with_retry(db_url: str) -> None:
    """
    Ensure required tables exist.

    IMPORTANT: This must be safe under concurrent startup of multiple controllers.
    We serialize schema init with pg_advisory_xact_lock to prevent catalog races.
    """
    total_retry_s = float(os.getenv("PG_SCHEMA_RETRY_S", "15"))
    interval_s = float(os.getenv("PG_SCHEMA_RETRY_INTERVAL_S", "0.5"))
    deadline = time.time() + total_retry_s
    last_err: Optional[Exception] = None

    while time.time() < deadline:
        try:
            with psycopg.connect(db_url) as conn:
                # Use an explicit transaction so advisory_xact_lock is held
                # for the duration of schema creation and released on commit/rollback.
                with conn.transaction():
                    conn.execute("SELECT pg_advisory_xact_lock(%s);", (SCHEMA_LOCK_ID,))

                    # Minimal schema for our HA v0 tests
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS dspu_meta (
                            k TEXT PRIMARY KEY,
                            v TEXT NOT NULL
                        );
                        """
                    )

                    # Leader lock / epoch record lives in dspu_meta:
                    # - k='leader_id' -> node_id
                    # - k='leader_epoch' -> int
                    # - k='updated_ms' -> ms since epoch
                    #
                    # Nothing else required yet for conformance tests.

            return  # success
        except Exception as e:
            last_err = e
            time.sleep(interval_s)

    raise RuntimeError(f"schema init failed after {total_retry_s}s: {last_err!r}") from last_err


def try_acquire_leader(db_url: str, node_id: str) -> RoleState:
    """
    Single-leader election using a PG advisory lock + durable epoch in dspu_meta.

    We use a session-level advisory lock to guarantee only one LEADER at a time.
    Epoch increments on each successful leader acquisition.
    """
    leader_id = node_id
    poll_s = float(os.getenv("LEADER_POLL_S", "0.5"))

    # The lock id used for leader election (distinct from schema lock).
    # Stable constant int8.
    leader_lock_id = 4242424242

    while True:
        ensure_schema_with_retry(db_url)

        try:
            conn = psycopg.connect(db_url)
            conn.autocommit = True  # required for session advisory locks to behave predictably

            # Try to take the leader lock immediately; if we can’t, we’re standby.
            got = conn.execute("SELECT pg_try_advisory_lock(%s);", (leader_lock_id,)).fetchone()[0]
            if not got:
                conn.close()
                # We are standby; read current leader info best-effort.
                return read_role(db_url, node_id)

            # We are leader. Bump epoch durably.
            with conn.transaction():
                # Read current epoch
                row = conn.execute("SELECT v FROM dspu_meta WHERE k='leader_epoch';").fetchone()
                cur_epoch = int(row[0]) if row else 0
                new_epoch = cur_epoch + 1

                conn.execute(
                    """
                    INSERT INTO dspu_meta (k, v) VALUES ('leader_epoch', %s)
                    ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
                    """,
                    (str(new_epoch),),
                )
                conn.execute(
                    """
                    INSERT INTO dspu_meta (k, v) VALUES ('leader_id', %s)
                    ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
                    """,
                    (leader_id,),
                )
                conn.execute(
                    """
                    INSERT INTO dspu_meta (k, v) VALUES ('updated_ms', %s)
                    ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
                    """,
                    (str(_now_ms()),),
                )

            return RoleState(role="LEADER", node_id=node_id, leader_id=leader_id, leader_epoch=new_epoch)

        except Exception:
            # If anything fails, wait and try again
            time.sleep(poll_s)


def read_role(db_url: str, node_id: str) -> RoleState:
    """
    Read current role info from dspu_meta.
    Standby nodes use this for /role responses and for NOT_LEADER errors.
    """
    ensure_schema_with_retry(db_url)

    leader_id = "unknown"
    leader_epoch = 0

    try:
        with psycopg.connect(db_url) as conn:
            r1 = conn.execute("SELECT v FROM dspu_meta WHERE k='leader_id';").fetchone()
            if r1:
                leader_id = r1[0]
            r2 = conn.execute("SELECT v FROM dspu_meta WHERE k='leader_epoch';").fetchone()
            if r2:
                leader_epoch = int(r2[0])
    except Exception:
        # best-effort; leave defaults
        pass

    # Role is computed by "am I the leader_id?"
    role = "LEADER" if leader_id == node_id else "STANDBY"
    return RoleState(role=role, node_id=node_id, leader_id=leader_id, leader_epoch=leader_epoch)
