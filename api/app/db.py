"""SQLite persistence layer.

Stores jobs, per-announcement fetch state and per-origin cooldowns so that a
restart can resume unfinished work without re-fetching successful documents
and without losing active cooldown deadlines.
"""
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    total      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS announcements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    url         TEXT NOT NULL,           -- originally submitted URL
    current_url TEXT NOT NULL,           -- URL currently being fetched (changes on redirect)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending|fetching|success|failed
    hops        INTEGER NOT NULL DEFAULT 0,       -- redirects followed so far
    attempts    INTEGER NOT NULL DEFAULT 0,       -- retries used on current_url
    title       TEXT,
    content     TEXT,
    error       TEXT,
    updated_at  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ann_job ON announcements(job_id);
CREATE TABLE IF NOT EXISTS cooldowns (
    origin   TEXT PRIMARY KEY,
    until_ts REAL NOT NULL
);
-- Continuation edges: a correction points back at the announcement it
-- replaces.  announcement_id is the PRIMARY KEY, so an announcement can have
-- at most ONE predecessor ever.  The partial unique index on previous_id (only
-- active edges) means an announcement can have at most ONE *active* successor:
-- a failed successor may be detached when a new correction is re-hung in its
-- place, and its detached history is kept for audit.
CREATE TABLE IF NOT EXISTS continuations (
    announcement_id INTEGER PRIMARY KEY REFERENCES announcements(id),
    previous_id     INTEGER NOT NULL REFERENCES announcements(id),
    created_at      REAL NOT NULL,
    detached_at     REAL,
    detached_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cont_prev_active
    ON continuations(previous_id) WHERE detached_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_cont_ann ON continuations(announcement_id);
"""

_UPDATABLE = {"current_url", "status", "hops", "attempts", "title", "content", "error"}


class DB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- jobs ---------------------------------------------------------
    def create_job(self, job_id: str, total: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (id, created_at, total) VALUES (?, ?, ?)",
                (job_id, time.time(), total),
            )

    def get_job(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()

    def list_jobs(self):
        with self._lock:
            return self._conn.execute(
                """
                SELECT j.id, j.created_at, j.total,
                       COALESCE(SUM(a.status = 'success'), 0)  AS success,
                       COALESCE(SUM(a.status = 'failed'), 0)   AS failed,
                       COALESCE(SUM(a.status IN ('pending','fetching')), 0) AS unfinished
                FROM jobs j LEFT JOIN announcements a ON a.job_id = j.id
                GROUP BY j.id ORDER BY j.created_at DESC
                """
            ).fetchall()

    # ---- announcements -------------------------------------------------
    def add_announcement(self, job_id: str, url: str) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO announcements (job_id, url, current_url, status, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?)",
                (job_id, url, url, time.time()),
            )
            return cur.lastrowid

    def update_ann(self, ann_id: int, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in _UPDATABLE}
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE announcements SET {cols} WHERE id = ?",
                (*fields.values(), ann_id),
            )

    def list_announcements(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()

    def unfinished_announcements(self):
        with self._lock:
            return self._conn.execute(
                "SELECT id, current_url, hops, attempts FROM announcements"
                " WHERE status IN ('pending', 'fetching') ORDER BY id"
            ).fetchall()

    # ---- continuations --------------------------------------------------
    def get_announcement(self, ann_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (ann_id,)
            ).fetchone()

    def predecessor_of(self, ann_id: int):
        """Active predecessor row of ann_id, or None."""
        with self._lock:
            edge = self._conn.execute(
                "SELECT previous_id FROM continuations"
                " WHERE announcement_id = ? AND detached_at IS NULL", (ann_id,)
            ).fetchone()
            if edge is None:
                return None
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (edge["previous_id"],)
            ).fetchone()

    def successor_of(self, ann_id: int):
        """Active successor row of ann_id, or None."""
        with self._lock:
            edge = self._conn.execute(
                "SELECT announcement_id FROM continuations"
                " WHERE previous_id = ? AND detached_at IS NULL", (ann_id,)
            ).fetchone()
            if edge is None:
                return None
            return self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (edge["announcement_id"],)
            ).fetchone()

    def register_continuation(self, previous_id: int, announcement_id: int):
        """Register `announcement_id` as the correction of `previous_id`.

        All validation and the write happen under one lock/transaction, so
        concurrent registrations can never yield two active successors or two
        active predecessors.  Returns (error_code, extra) where error_code is
        None on success.

        Rules:
          * both announcements must exist; the original must already be
            successfully fetched, the correction may still be fetching;
          * a correction has at most one predecessor (never replaceable);
          * an announcement has at most one active successor; a successor
            whose fetch *failed* may be detached to re-hang a new correction;
          * cycles are rejected;
          * re-registering the exact same active edge is idempotent.
        """
        with self._lock, self._conn:
            prev = self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (previous_id,)
            ).fetchone()
            corr = self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (announcement_id,)
            ).fetchone()
            if prev is None or corr is None:
                return "unknown_announcement", None
            if prev["status"] != "success":
                return "original_not_collected", prev["status"]
            if announcement_id == previous_id:
                return "cycle", None

            own = self._conn.execute(
                "SELECT previous_id, detached_at FROM continuations"
                " WHERE announcement_id = ?", (announcement_id,)
            ).fetchone()
            if own is not None:
                if own["detached_at"] is None and own["previous_id"] == previous_id:
                    return None, "already_registered"
                return "announcement_already_has_predecessor", None

            # A correction with its own successor is an interior node: re-hanging
            # it elsewhere would splice two chains together.
            if self._conn.execute(
                "SELECT 1 FROM continuations WHERE previous_id = ? AND detached_at IS NULL",
                (announcement_id,),
            ).fetchone() is not None:
                return "announcement_has_successor", None

            active = self._conn.execute(
                "SELECT announcement_id FROM continuations"
                " WHERE previous_id = ? AND detached_at IS NULL", (previous_id,),
            ).fetchone()
            replaced_failed = False
            if active is not None:
                old = self._conn.execute(
                    "SELECT status FROM announcements WHERE id = ?", (active["announcement_id"],)
                ).fetchone()
                if old is not None and old["status"] == "success":
                    return "previous_has_collected_successor", active["announcement_id"]
                if old is not None and old["status"] in ("pending", "fetching"):
                    return "previous_has_pending_successor", active["announcement_id"]
                replaced_failed = True  # failed correction -> replaced below

            # Cycle check BEFORE any mutation, so a rejected registration leaves
            # the existing graph untouched.  Walk back from previous_id: reaching
            # the correction being attached would close a cycle.
            cursor, seen = previous_id, set()
            while cursor is not None and cursor not in seen:
                if cursor == announcement_id:
                    return "cycle", None
                seen.add(cursor)
                row = self._conn.execute(
                    "SELECT previous_id FROM continuations"
                    " WHERE announcement_id = ? AND detached_at IS NULL", (cursor,)
                ).fetchone()
                cursor = row["previous_id"] if row else None

            if replaced_failed:
                self._conn.execute(
                    "UPDATE continuations SET detached_at = ?, detached_reason = ?"
                    " WHERE previous_id = ? AND detached_at IS NULL",
                    (time.time(), "replaced by a newly registered correction", previous_id),
                )

            try:
                self._conn.execute(
                    "INSERT INTO continuations (announcement_id, previous_id, created_at)"
                    " VALUES (?, ?, ?)",
                    (announcement_id, previous_id, time.time()),
                )
            except sqlite3.IntegrityError:
                # Belt-and-braces: the partial unique index also protects
                # against a second writer (multi-process), in which case the
                # in-process lock was not enough.  Roll back the detach too.
                self._conn.rollback()
                return "concurrent_conflict", None
            if replaced_failed:
                return None, "replaced_failed_correction"
            return None, None

    def detached_edge_of(self, ann_id: int):
        """Detached edge row of ann_id (previous_id, detached_at, reason), or None."""
        with self._lock:
            return self._conn.execute(
                "SELECT previous_id, detached_at, detached_reason FROM continuations"
                " WHERE announcement_id = ? AND detached_at IS NOT NULL", (ann_id,)
            ).fetchone()

    def chain_snapshot(self, ann_id: int):
        """Return (chain_rows, detached_rows) read under ONE lock.

        The whole read is a single transaction, so a correction completing
        concurrently either is fully visible or not visible at all: callers
        can never observe two effective versions or a transient gap.
        """
        with self._lock:
            if self._conn.execute(
                "SELECT 1 FROM announcements WHERE id = ?", (ann_id,)
            ).fetchone() is None:
                return None
            cursor, seen = ann_id, set()
            while cursor not in seen:
                seen.add(cursor)
                row = self._conn.execute(
                    "SELECT previous_id FROM continuations"
                    " WHERE announcement_id = ? AND detached_at IS NULL", (cursor,)
                ).fetchone()
                if row is None:
                    # A replaced (detached) correction is displayed with the
                    # chain that replaced it, so its owner can see why.
                    row = self._conn.execute(
                        "SELECT previous_id FROM continuations"
                        " WHERE announcement_id = ? AND detached_at IS NOT NULL",
                        (cursor,),
                    ).fetchone()
                    if row is None:
                        break
                cursor = row["previous_id"]
            chain = self._chain_rows(cursor) if cursor is not None else []
            if not chain:
                return chain, []
            detached = self._detached_rows([row["id"] for row in chain])
            return chain, detached

    def _chain_rows(self, root_id: int):
        """Rows of the active chain rooted at root_id.  Caller holds the lock."""
        chain, cursor, seen = [], root_id, set()
        while cursor is not None and cursor not in seen:
            seen.add(cursor)
            row = self._conn.execute(
                "SELECT * FROM announcements WHERE id = ?", (cursor,)
            ).fetchone()
            if row is None:
                break
            chain.append(row)
            nxt = self._conn.execute(
                "SELECT announcement_id FROM continuations"
                " WHERE previous_id = ? AND detached_at IS NULL", (cursor,)
            ).fetchone()
            cursor = nxt["announcement_id"] if nxt else None
        return chain

    def _detached_rows(self, chain_ids):
        """Detached corrections whose previous node is in chain_ids.  Lock held."""
        if not chain_ids:
            return []
        placeholders = ",".join("?" for _ in chain_ids)
        return self._conn.execute(
            f"""
            SELECT c.previous_id, c.detached_at, c.detached_reason,
                   a.id, a.job_id, a.url, a.status, a.error
            FROM continuations c JOIN announcements a ON a.id = c.announcement_id
            WHERE c.detached_at IS NOT NULL AND c.previous_id IN ({placeholders})
            ORDER BY c.detached_at, c.announcement_id
            """,
            list(chain_ids),
        ).fetchall()

    def corpus_snapshot(self, job_id: str | None = None):
        """All active chains read under ONE lock.

        Returns a list of (chain_rows, detached_rows), each chain exactly once.
        A registration racing with this read is therefore observed either
        before or after, never half-way: no duplicate chain entries and no
        interruption of supply.
        """
        with self._lock:
            if job_id is None:
                roots = [r["id"] for r in self._conn.execute(
                    "SELECT a.id FROM announcements a"
                    " WHERE NOT EXISTS (SELECT 1 FROM continuations c"
                    "                   WHERE c.announcement_id = a.id)"
                    " ORDER BY a.id").fetchall()]
            else:
                roots = set()
                job_anns = self._conn.execute(
                    "SELECT id FROM announcements WHERE job_id = ?", (job_id,)
                ).fetchall()
                for ann in job_anns:
                    cursor, seen = ann["id"], set()
                    while cursor not in seen:
                        seen.add(cursor)
                        row = self._conn.execute(
                            "SELECT previous_id FROM continuations"
                            " WHERE announcement_id = ? AND detached_at IS NULL",
                            (cursor,),
                        ).fetchone()
                        if row is None:
                            # Detached correction: attribute it to the chain
                            # that replaced it.
                            row = self._conn.execute(
                                "SELECT previous_id FROM continuations"
                                " WHERE announcement_id = ? AND detached_at IS NOT NULL",
                                (cursor,),
                            ).fetchone()
                            if row is None:
                                break
                        cursor = row["previous_id"]
                    roots.add(cursor)
                roots = sorted(roots)

            snapshots = []
            for root_id in roots:
                chain = self._chain_rows(root_id)
                if not chain:
                    continue
                detached = self._detached_rows([row["id"] for row in chain])
                snapshots.append((chain, detached))
            return snapshots

    # ---- cooldowns ------------------------------------------------------
    def set_cooldown(self, origin: str, until_ts: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO cooldowns (origin, until_ts) VALUES (?, ?)"
                " ON CONFLICT(origin) DO UPDATE SET until_ts = excluded.until_ts",
                (origin, until_ts),
            )

    def active_cooldowns(self, now: float):
        with self._lock:
            return self._conn.execute(
                "SELECT origin, until_ts FROM cooldowns WHERE until_ts > ?", (now,)
            ).fetchall()
