#!/usr/bin/env python3
"""Self-test for announcement continuations (originals -> corrections).

Covers:
  * registration rules: predecessor must be collected, one predecessor and one
    active successor, cycles rejected, failed corrections replaceable;
  * effective-corpus policy: last successfully collected item per chain, with
    the previous version still supplied while a correction is pending/failed;
  * interleaving of registration and fetch completion: never two effective
    versions and never a transient gap (dense polling);
  * concurrent registrations cannot create two successors;
  * consistency across restarts (pending corrections resume, conclusions equal);
  * the original per-job download behaviour is unchanged.

Runs real services as local processes, like tests/e2e_test.py, on dedicated
ports so it can run next to the other suites.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8210"
A = "http://127.0.0.1:8211"
B = "http://127.0.0.1:8212"
PY = sys.executable

FAILURES = []
PROCS = []
LOGDIR = None
DB_DIR = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


def start_proc(name, args, env, port):
    logfile = open(LOGDIR / f"{name}.log", "a")
    p = subprocess.Popen(args, env=env, stdout=logfile, stderr=subprocess.STDOUT)
    PROCS.append((name, p))
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"{name} exited early, see {LOGDIR}/{name}.log")
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            return p
        except Exception:
            time.sleep(0.15)
    raise RuntimeError(f"{name} did not start on port {port}")


def start_api():
    env = dict(os.environ, DB_PATH=str(DB_DIR / "collector.db"))
    return start_proc("api", [PY, "-m", "uvicorn", "app.main:app",
                              "--app-dir", str(ROOT / "api"),
                              "--host", "127.0.0.1", "--port", "8210"], env, 8210)


def stop_api():
    for i, (name, p) in enumerate(PROCS):
        if name == "api" and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
            PROCS.pop(i)
            return


def submit(client, urls):
    r = client.post(f"{API}/jobs", json={"urls": urls})
    assert r.status_code == 202, r.text
    body = r.json()
    return body["job_id"], {a["announcement_id"]: a["url"] for a in body["announcements"]}


def wait_job(client, job_id, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        body = client.get(f"{API}/jobs/{job_id}").json()
        if body["status"] != "running":
            return body
        time.sleep(0.1)
    raise TimeoutError(f"job {job_id} still running after {timeout}s")


def collect_one(client, url):
    """Submit one URL, wait for it, return (announcement_id, status)."""
    job_id, ids = submit(client, [url])
    body = wait_job(client, job_id)
    ann_id = next(iter(ids))
    return ann_id, body["announcements"][0]["status"]


def register(client, previous_id, announcement_id):
    return client.post(f"{API}/api/continuations", json={
        "previous_id": previous_id, "announcement_id": announcement_id})


def chain_of(client, ann_id):
    r = client.get(f"{API}/api/announcements/{ann_id}/chain")
    assert r.status_code == 200, r.text
    return r.json()


def effective_entry(client, root_id):
    body = client.get(f"{API}/api/corpus/effective").json()
    for e in body["entries"]:
        if e["chain_root_id"] == root_id:
            return e
    return None


def wait_ann_status(client, ann_id, statuses, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = client.get(f"{API}/api/announcements/{ann_id}")
        if r.status_code == 200 and r.json()["status"] in statuses:
            return r.json()
        time.sleep(0.05)
    raise TimeoutError(f"announcement {ann_id} never reached {statuses}")


def main():
    global LOGDIR, DB_DIR
    work = Path(tempfile.mkdtemp(prefix="collector-continuation-"))
    LOGDIR = work / "logs"
    LOGDIR.mkdir(parents=True)
    DB_DIR = work / "db"
    DB_DIR.mkdir()
    log(f"workdir: {work}")

    start_proc("site_a", [PY, "-m", "uvicorn", "app:app", "--app-dir",
                          str(ROOT / "sites"), "--host", "127.0.0.1", "--port", "8211"],
               dict(os.environ, SITE_NAME="site_a", PEER_BASE=B), 8211)
    start_proc("site_b", [PY, "-m", "uvicorn", "app:app", "--app-dir",
                          str(ROOT / "sites"), "--host", "127.0.0.1", "--port", "8212"],
               dict(os.environ, SITE_NAME="site_b", PEER_BASE=A), 8212)
    start_api()
    client = httpx.Client(timeout=30.0)

    try:
        # ---------------- phase 1: registration + already-collected correction
        log("phase 1: register a correction that is already collected")
        _job, ids = submit(client, [f"{A}/article/1", f"{B}/article/2"])
        orig1 = next(i for i, u in ids.items() if u == f"{A}/article/1")
        corr1 = next(i for i, u in ids.items() if u == f"{B}/article/2")
        wait_job(client, _job)
        v = chain_of(client, orig1)
        check("p1 standalone chain state=current", v["state"] == "current", v["state"])
        check("p1 standalone effective is itself", v["effective_announcement_id"] == orig1)

        r = register(client, orig1, corr1)
        check("p1 register 201", r.status_code == 201, r.text[:200])
        v = chain_of(client, orig1)
        check("p1 chain length 2", v["chain_length"] == 2, str(v["chain_length"]))
        check("p1 state current after collected correction", v["state"] == "current")
        check("p1 effective switched to correction",
              v["effective_announcement_id"] == corr1, str(v["effective_announcement_id"]))
        check("p1 correction text is site_b's",
              "site_b" in (v["effective"]["title"] or ""), str(v["effective"]["title"]))
        e = effective_entry(client, orig1)
        check("p1 effective corpus has exactly one entry for chain",
              e is not None and e["effective_announcement_id"] == corr1)
        body = client.get(f"{API}/api/corpus/effective").json()
        same = [x for x in body["entries"] if x["chain_root_id"] == orig1]
        check("p1 no duplicate effective version in corpus", len(same) == 1, str(len(same)))

        # original job download unchanged: still both raw documents
        dl = client.get(f"{API}/jobs/{_job}/download")
        check("p1 original job download still 200", dl.status_code == 200)
        check("p1 original job download still has 2 docs",
              len(dl.json()["documents"]) == 2, str(len(dl.json()["documents"])))

        # idempotent re-registration
        r = register(client, orig1, corr1)
        check("p1 idempotent re-register 201", r.status_code == 201, r.text[:200])
        check("p1 idempotent status",
              r.json()["status"] == "already_registered", r.json()["status"])

        # ---------------- phase 2: pending correction + dense interleaving
        log("phase 2: pending correction keeps previous version; no gap on switch")
        orig2, st = collect_one(client, f"{A}/article/3")
        check("p2 original collected", st == "success", st)
        jobp, idsp = submit(client, [f"{A}/slow/1?delay=3"])
        corr2 = next(iter(idsp))
        r = register(client, orig2, corr2)  # correction is still being fetched
        check("p2 register pending correction 201", r.status_code == 201, r.text[:200])
        v = chain_of(client, orig2)
        check("p2 state=pending", v["state"] == "pending", v["state"])
        check("p2 previous version still supplied", v["available"] is True)
        check("p2 effective is original", v["effective_announcement_id"] == orig2)
        check("p2 pending reason reported", bool(v["pending_reason"]), v["pending_reason"])

        snapshots = []
        t0 = time.time()
        while time.time() - t0 < 30:
            body = client.get(f"{API}/api/corpus/effective").json()
            ent = [x for x in body["entries"] if x["chain_root_id"] == orig2]
            snapshots.append((len(ent), ent[0]["available"] if ent else None,
                              ent[0]["effective_announcement_id"] if ent else None))
            if body["entries"] and ent and ent[0]["effective_announcement_id"] == corr2:
                break
            time.sleep(0.01)
        final = wait_job(client, jobp)
        check("p2 pending job succeeded", final["status"] == "success", final["status"])
        check("p2 polled enough snapshots", len(snapshots) > 20, str(len(snapshots)))
        check("p2 exactly one chain entry in every snapshot",
              all(n == 1 for n, _, _ in snapshots),
              str({n for n, _, _ in snapshots}))
        check("p2 never lost supply", all(av for _, av, _ in snapshots))
        check("p2 effective id only orig or correction",
              all(eid in (orig2, corr2) for _, _, eid in snapshots),
              str(set(e for _, _, e in snapshots)))
        ids_seen = [e for _, _, e in snapshots]
        check("p2 monotonic switch orig -> correction",
              ids_seen == sorted(ids_seen, key=lambda x: 0 if x == orig2 else 1),
              str(ids_seen[:3] + ids_seen[-3:]))
        check("p2 switch happened", ids_seen[-1] == corr2, str(ids_seen[-1]))
        v = chain_of(client, corr2)
        check("p2 correction now effective", v["effective_announcement_id"] == corr2)

        # ---------------- phase 3: failed correction
        log("phase 3: failed correction falls back to previous version")
        orig3, st = collect_one(client, f"{A}/article/4")
        check("p3 original collected", st == "success", st)
        bad_job, bad_ids = submit(client, [f"{B}/busy"])
        corr_bad = next(iter(bad_ids))
        bad_body = wait_job(client, bad_job)
        check("p3 bad correction failed", bad_body["status"] == "failed")
        r = register(client, orig3, corr_bad)
        check("p3 register failed correction 201", r.status_code == 201, r.text[:200])
        v = chain_of(client, orig3)
        check("p3 state=failed", v["state"] == "failed", v["state"])
        check("p3 previous version still supplied", v["available"] is True)
        check("p3 effective is original", v["effective_announcement_id"] == orig3)
        check("p3 failure reason reported", "503" in (v["failed_reason"] or ""),
              v["failed_reason"])

        # Register while the correction is still fetching, then let it fail.
        orig3b, st = collect_one(client, f"{A}/article/22")
        check("p3 second original collected", st == "success", st)
        late_job, late_ids = submit(client, [f"{B}/busy"])
        late_corr = next(iter(late_ids))
        r = register(client, orig3b, late_corr)
        check("p3 register in-flight failing correction 201", r.status_code == 201,
              r.text[:200])
        v = chain_of(client, orig3b)
        if v["state"] == "pending":
            check("p3 in-flight correction keeps previous version",
                  v["available"] and v["effective_announcement_id"] == orig3b,
                  str(v["state"]))
        wait_job(client, late_job)
        v = chain_of(client, orig3b)
        check("p3 correction that failed after registration -> state failed",
              v["state"] == "failed", v["state"])
        check("p3 previous version still effective after late failure",
              v["available"] and v["effective_announcement_id"] == orig3b)
        check("p3 late failure reason included", bool(v["failed_reason"]))

        # ---------------- phase 4: re-hang a new correction (replace failed one)
        log("phase 4: failed correction replaced by a new one")
        corr_new, st = collect_one(client, f"{A}/article/5")
        check("p4 replacement collected", st == "success", st)
        old_ann = client.get(f"{API}/api/announcements/{corr_bad}").json()
        check("p4 old correction linked to original",
              old_ann["previous_id"] == orig3, str(old_ann["previous_id"]))
        r = register(client, orig3, corr_new)
        check("p4 re-hang 201", r.status_code == 201, r.text[:200])
        check("p4 replaced status", r.json()["status"] == "replaced_failed_correction",
              r.json().get("status"))
        v = chain_of(client, orig3)
        check("p4 state=current", v["state"] == "current", v["state"])
        check("p4 effective is new correction", v["effective_announcement_id"] == corr_new)
        check("p4 replaced correction listed for audit",
              [x["announcement_id"] for x in v["replaced_corrections"]] == [corr_bad],
              str(v["replaced_corrections"]))
        check("p4 chain does not contain failed node",
              corr_bad not in [c["announcement_id"] for c in v["chain"]])
        v_bad = chain_of(client, corr_bad)
        check("p4 replaced correction resolves to its chain",
              v_bad["chain_root_id"] == orig3
              and v_bad["effective_announcement_id"] == corr_new, str(v_bad["state"]))
        bad_job_id = client.get(
            f"{API}/api/announcements/{corr_bad}").json()["job_id"]
        filtered = client.get(f"{API}/api/corpus/effective", params={"job_id": bad_job_id}).json()
        check("p4 job filter includes the chain of a detached correction",
              [e["chain_root_id"] for e in filtered["entries"]] == [orig3],
              str([e["chain_root_id"] for e in filtered["entries"]]))
        check("p4 job filter has no duplicate chain entries",
              len(filtered["entries"]) == len({e["chain_root_id"] for e in filtered["entries"]}))
        old_ann = client.get(f"{API}/api/announcements/{corr_bad}").json()
        check("p4 old correction now detached",
              old_ann["successor_id"] is None and old_ann["detached_from_id"] == orig3,
              str(old_ann))
        r = register(client, orig3, corr_bad)
        check("p4 detached correction cannot be re-attached", r.status_code == 409,
              r.text[:200])
        check("p4 reason announcement_already_has_predecessor",
              r.json()["detail"]["error"] == "announcement_already_has_predecessor",
              r.text[:200])

        # ---------------- phase 5: three-deep chain
        log("phase 5: three-deep chain serves last collected")
        orig5, st = collect_one(client, f"{A}/article/6")
        corr5a, st = collect_one(client, f"{A}/article/7")
        job5, ids5 = submit(client, [f"{A}/slow/2?delay=4"])
        corr5b = next(iter(ids5))
        check("p5 register first correction", register(client, orig5, corr5a).status_code == 201)
        check("p5 register second correction", register(client, corr5a, corr5b).status_code == 201)
        v = chain_of(client, orig5)
        check("p5 chain length 3", v["chain_length"] == 3, str(v["chain_length"]))
        check("p5 pending tail", v["state"] == "pending", v["state"])
        check("p5 effective is middle correction", v["effective_announcement_id"] == corr5a)
        wait_job(client, job5, timeout=30)
        v = chain_of(client, orig5)
        check("p5 effective is deepest correction", v["effective_announcement_id"] == corr5b)
        check("p5 state current", v["state"] == "current")

        # ---------------- phase 6: rejections
        log("phase 6: registration constraints")
        r = register(client, 999999, 888888)
        check("p6 unknown ids 404", r.status_code == 404, str(r.status_code))
        corr6, st = collect_one(client, f"{A}/article/8")
        check("p6 correction collected before pending check", st == "success", st)
        pending_job, pending_ids = submit(client, [f"{A}/slow/3?delay=8"])
        pending_ann = next(iter(pending_ids))
        r = register(client, pending_ann, corr6)
        check("p6 original not collected 409", r.status_code == 409, str(r.status_code))
        check("p6 reason original_not_collected",
              r.json()["detail"]["error"] == "original_not_collected", r.text[:200])
        wait_job(client, pending_job, timeout=30)

        # predecessor already has a pending successor (corr5b's predecessor chain)
        orig6, st = collect_one(client, f"{A}/article/9")
        job6, ids6 = submit(client, [f"{A}/slow/4?delay=6"])
        corr6b = next(iter(ids6))
        register(client, orig6, corr6b)
        corr6c, st = collect_one(client, f"{B}/article/10")
        check("p6 second correction collected on other origin", st == "success", st)
        r = register(client, orig6, corr6c)
        check("p6 second successor while first pending 409", r.status_code == 409,
              str(r.status_code))
        check("p6 reason previous_has_pending_successor",
              r.json()["detail"]["error"] == "previous_has_pending_successor",
              r.text[:200])
        wait_job(client, job6, timeout=30)
        r = register(client, orig6, corr6c)
        check("p6 second successor after first collected 409", r.status_code == 409)
        check("p6 reason previous_has_collected_successor",
              r.json()["detail"]["error"] == "previous_has_collected_successor",
              r.text[:200])

        # correction already has a predecessor (re-hanging it to another one)
        other_orig, st = collect_one(client, f"{A}/article/13")
        check("p6 second original collected", st == "success", st)
        r = register(client, other_orig, corr6b)
        check("p6 correction with predecessor rejected", r.status_code == 409,
              r.text[:200])
        check("p6 reason announcement_already_has_predecessor",
              r.json()["detail"]["error"] == "announcement_already_has_predecessor",
              r.text[:200])
        r = register(client, orig6, corr6b)
        check("p6 same edge re-register is idempotent",
              r.status_code == 201 and r.json()["status"] == "already_registered",
              r.text[:200])

        # announcement with a successor cannot become a correction elsewhere
        r = register(client, corr6b, orig6)
        check("p6 root with successor cannot be re-hung", r.status_code == 409,
              r.text[:200])
        check("p6 reason announcement_has_successor",
              r.json()["detail"]["error"] == "announcement_has_successor",
              r.text[:200])

        # direct self-cycle
        r = register(client, orig6, orig6)
        check("p6 self-cycle rejected", r.status_code == 409, r.text[:200])
        check("p6 self-cycle reason", r.json()["detail"]["error"] == "cycle")

        # ---------------- phase 7: concurrent registrations
        log("phase 7: concurrent registration cannot create two successors")
        orig7, st = collect_one(client, f"{A}/article/11")
        jobc, idsc = submit(client, [f"{A}/slow/5?delay=5", f"{B}/slow/6?delay=5"])
        corr7 = list(idsc)
        results = []

        def do_reg(ann):
            c = httpx.Client(timeout=30.0)
            try:
                results.append(register(c, orig7, ann).status_code)
            finally:
                c.close()

        threads = [threading.Thread(target=do_reg, args=(a,)) for a in corr7]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("p7 exactly one registration accepted",
              sorted(results) == [201, 409], str(results))
        v = chain_of(client, orig7)
        check("p7 exactly one active correction", v["chain_length"] == 2,
              str(v["chain_length"]))
        wait_job(client, jobc, timeout=30)

        # ---------------- phase 7b: read/registration interleaving stress
        log("phase 7b: corpus reads stay consistent while registrations race")
        standalone = []
        for n in range(14, 30):
            ann, st = collect_one(client, f"{A}/article/{n}")
            standalone.append(ann)
            check(f"p7b {n} collected", st == "success", st)
        race_orig = standalone[0]
        race_corrs = standalone[1:]
        invariants = {"runs": 0, "dup": 0, "missing_new": 0}
        stop = threading.Event()

        def reader():
            c = httpx.Client(timeout=30.0)
            try:
                while not stop.is_set():
                    body = c.get(f"{API}/api/corpus/effective").json()
                    roots = [e["chain_root_id"] for e in body["entries"]]
                    invariants["runs"] += 1
                    if len(roots) != len(set(roots)):
                        invariants["dup"] += 1
                    for e in body["entries"]:
                        if e["chain_length"] > 1 and not e["available"]:
                            invariants["missing_new"] += 1
            finally:
                c.close()

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        accepted = []
        for corr in race_corrs:
            r = register(client, race_orig, corr)
            accepted.append(r.status_code)
            time.sleep(0.01)
        stop.set()
        for t in readers:
            t.join()
        check("p7b exactly one corrective registration accepted",
              accepted.count(201) == 1, str(accepted))
        check("p7b corpus read many times during race", invariants["runs"] > 20,
              str(invariants["runs"]))
        check("p7b no duplicate chain entry observed", invariants["dup"] == 0,
              str(invariants["dup"]))
        check("p7b no chain lost its document during race",
              invariants["missing_new"] == 0, str(invariants["missing_new"]))
        v = chain_of(client, race_orig)
        check("p7b final chain has exactly one correction", v["chain_length"] == 2,
              str(v["chain_length"]))

        # ---------------- phase 8: restart consistency
        log("phase 8: restart keeps chains, resumes pending corrections")
        orig8, st = collect_one(client, f"{A}/article/12")
        job8, ids8 = submit(client, [f"{B}/slow/8?delay=6"])
        corr8 = next(iter(ids8))
        register(client, orig8, corr8)
        wait_ann_status(client, corr8, {"fetching"})
        before = chain_of(client, orig8)
        check("p8 pending before restart", before["state"] == "pending", before["state"])
        check("p8 effective before restart", before["effective_announcement_id"] == orig8)

        stop_api()
        log("  api stopped, restarting...")
        start_api()
        after = chain_of(client, orig8)
        check("p8 chain length after restart", after["chain_length"] == 2,
              str(after["chain_length"]))
        check("p8 effective after restart", after["effective_announcement_id"] == orig8)
        check("p8 supply after restart", after["available"] is True)
        check("p8 pending reason after restart", bool(after["pending_reason"]))
        wait_job(client, job8, timeout=60)
        wait_ann_status(client, corr8, {"success"})
        settled = chain_of(client, orig8)
        check("p8 correction effective after resume",
              settled["effective_announcement_id"] == corr8)

        corpus_before = client.get(f"{API}/api/corpus/effective").json()
        stop_api()
        log("  api stopped again (settled state), restarting...")
        start_api()
        corpus_after = client.get(f"{API}/api/corpus/effective").json()
        check("p8 corpus entry count identical after restart",
              corpus_before["entry_count"] == corpus_after["entry_count"],
              f"{corpus_before['entry_count']} vs {corpus_after['entry_count']}")
        check("p8 effective ids identical after restart",
              [e["effective_announcement_id"] for e in corpus_before["entries"]]
              == [e["effective_announcement_id"] for e in corpus_after["entries"]])
        check("p8 states identical after restart",
              corpus_before["states"] == corpus_after["states"],
              f"{corpus_before['states']} vs {corpus_after['states']}")

        # ---------------- phase 9: downloads
        log("phase 9: effective corpus download")
        dl = client.get(f"{API}/api/corpus/effective/download?require_all_available=true")
        check("p9 effective download 200", dl.status_code == 200, str(dl.status_code))
        check("p9 attachment header",
              "attachment" in dl.headers.get("content-disposition", ""),
              dl.headers.get("content-disposition"))
        payload = dl.json()
        check("p9 one document per available chain",
              payload["document_count"] == len(payload["documents"]))
        check("p9 no chain has two documents",
              len({d["announcement_id"] for d in payload["documents"]})
              == len(payload["documents"]))
        chain_effective = {c["effective_announcement_id"]
                           for c in payload["chains"] if c["available"]}
        check("p9 documents are effective versions",
              {d["announcement_id"] for d in payload["documents"]} == chain_effective)

        # job download with a failed correction still behaves as before
        fail_job, fail_ids = submit(client, [f"{B}/article/20", f"{B}/busy"])
        wait_job(client, fail_job)
        check("p9 job download with failure still 409",
              client.get(f"{API}/jobs/{fail_job}/download").status_code == 409)

        # ---------------- phase 10: /docs exposes the endpoints
        log("phase 10: OpenAPI surface and unavailable chains")
        spec = client.get(f"{API}/openapi.json").json()
        paths = spec["paths"]
        check("p10 POST /api/continuations documented", "post" in paths["/api/continuations"])
        check("p10 effective corpus documented", "get" in paths["/api/corpus/effective"])
        check("p10 docs page 200", client.get(f"{API}/docs").status_code == 200)

        # A chain that never had a successful document is reported, not served.
        never_job, never_ids = submit(client, [f"{B}/busy"])
        never = next(iter(never_ids))
        wait_job(client, never_job)
        v = chain_of(client, never)
        check("p10 never-collected chain available=false", v["available"] is False)
        check("p10 never-collected chain has no effective document", v["effective"] is None)
        dl = client.get(f"{API}/api/corpus/effective/download",
                        params={"require_all_available": "true"})
        check("p10 strict download 409 when a chain is uncollected", dl.status_code == 409,
              str(dl.status_code))
        check("p10 strict download reports missing chain",
              never in dl.json()["detail"]["unavailable_chain_root_ids"],
              str(dl.json()["detail"]))
        dl = client.get(f"{API}/api/corpus/effective/download")
        check("p10 lenient download still 200", dl.status_code == 200)
        body = dl.json()
        check("p10 lenient download excludes null documents",
              all(d is not None for d in body["documents"]))
        check("p10 lenient download counts unavailable chain",
              body["unavailable_count"] >= 1, str(body["unavailable_count"]))
        r = client.get(f"{API}/api/corpus/effective", params={"job_id": "nope"})
        check("p10 unknown job filter 404", r.status_code == 404, str(r.status_code))

    finally:
        for name, p in PROCS:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        for name, p in PROCS:
            if p.poll() is None:
                p.kill()

    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        print(f"logs kept at {LOGDIR}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
