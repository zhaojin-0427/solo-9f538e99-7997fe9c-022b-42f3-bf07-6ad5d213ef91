"""Continuation chains and resolution of the effective corpus.

A collected announcement is either an *original* or a *correction* of exactly
one previous announcement.  Chains are linear: an announcement has at most one
predecessor and at most one successor, and cycles are rejected.

The effective document of a chain is the **last item that has been fetched
successfully**.  If the newest correction is still being fetched, or its fetch
failed, the previous successful item keeps being served and the pending /
failure reason is reported alongside it.  Because the whole chain is read in
one database snapshot, a correction completing concurrently either is or is
not yet visible: the response never contains two effective versions and never
loses the previous version.
"""
from __future__ import annotations

SUCCESS = "success"
FAILED = "failed"
PENDING_STATUSES = {"pending", "fetching"}


def _document(row) -> dict:
    return {
        "announcement_id": row["id"],
        "job_id": row["job_id"],
        "url": row["url"],
        "final_url": row["current_url"],
        "status": row["status"],
        "hops": row["hops"],
        "attempts": row["attempts"],
        "title": row["title"],
        "content": row["content"],
        "error": row["error"],
        "updated_at": row["updated_at"],
    }


def build_chain_view(rows, detached=()) -> dict | None:
    """Turn a root->tail list of announcement rows into an effective view.

    `detached` lists failed corrections that were replaced by a newer one; they
    are reported for audit but never considered effective.
    """
    if not rows:
        return None
    items = list(rows)

    effective_index = None
    for i, row in enumerate(items):
        if row["status"] == SUCCESS:
            effective_index = i  # last successful item wins

    tail = items[-1]
    chain = []
    for i, row in enumerate(items):
        item = {
            "position": i,
            "announcement_id": row["id"],
            "job_id": row["job_id"],
            "url": row["url"],
            "status": row["status"],
            "title": row["title"],
            "is_effective": i == effective_index,
            "superseded": effective_index is not None and i < effective_index,
            "error": row["error"],
            "updated_at": row["updated_at"],
        }
        if row["status"] in PENDING_STATUSES:
            item["reason"] = "correction is still being fetched"
        elif row["status"] == FAILED:
            item["reason"] = row["error"] or "fetch failed"
        chain.append(item)

    effective = _document(items[effective_index]) if effective_index is not None else None
    effective_id = items[effective_index]["id"] if effective_index is not None else None

    if tail["status"] in PENDING_STATUSES:
        state = "pending"
        pending_reason = (
            f"correction {tail['id']} is still being fetched (status={tail['status']}); "
            "serving the latest successfully fetched version"
            if effective is not None
            else f"correction {tail['id']} is still being fetched and nothing has been "
                 "fetched successfully yet"
        )
        failed_reason = None
    elif tail["status"] == FAILED:
        state = "failed"
        failed_reason = (
            f"correction {tail['id']} failed: {tail['error'] or 'unknown error'}; "
            + ("serving the latest successfully fetched version"
               if effective is not None
               else "nothing has been fetched successfully yet")
        )
        pending_reason = None
    else:
        state = "current"
        pending_reason = failed_reason = None

    return {
        "chain_root_id": items[0]["id"],
        "chain_length": len(items),
        "correction_count": len(items) - 1,
        "has_continuations": len(items) > 1,
        "state": state,
        "available": effective is not None,
        "effective_announcement_id": effective_id,
        "effective": effective,
        "tail_announcement_id": tail["id"],
        "tail_status": tail["status"],
        "pending_reason": pending_reason,
        "failed_reason": failed_reason,
        "replaced_corrections": [
            {
                "announcement_id": d["id"],
                "job_id": d["job_id"],
                "url": d["url"],
                "status": d["status"],
                "error": d["error"],
                "detached_at": d["detached_at"],
                "detached_reason": d["detached_reason"],
            }
            for d in detached
        ],
        "chain": chain,
    }
