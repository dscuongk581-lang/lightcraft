#!/usr/bin/env python3
"""Local-only LightCraft batch worker pilot. No HTTP listener, no SaaS credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import time
import uuid

from PIL import Image

ROOT = Path("/opt/lightcraft/bridge")
INCOMING = ROOT / "incoming"
JOBS = ROOT / "jobs"
RESULTS = ROOT / "results"
DATABASE = ROOT / "queue.sqlite3"
IMAGE = "lightcraft-cli:poc-76b5377"
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$|^[a-z0-9]$")
FILE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\.(png|jpg|jpeg|webp)$", re.I)
KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")
MAX_BYTES = 40 * 1024 * 1024
MAX_PIXELS = 40_000_000
MAX_ATTEMPTS = 2


def db():
    c = sqlite3.connect(DATABASE, timeout=20)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=20000")
    c.execute("""CREATE TABLE IF NOT EXISTS jobs(
      id TEXT PRIMARY KEY, tenant TEXT NOT NULL,
      idem TEXT NOT NULL, input_name TEXT NOT NULL,
      input_sha TEXT NOT NULL, input_path TEXT NOT NULL,
      state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
      created INTEGER NOT NULL, updated INTEGER NOT NULL,
      result_path TEXT, result_sha TEXT, result_bytes INTEGER,
      result_width INTEGER, result_height INTEGER,
      source_deleted INTEGER NOT NULL DEFAULT 0, error TEXT,
      UNIQUE(tenant,idem)
    )""")
    c.commit()
    return c


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as reader:
        while chunk := reader.read(512 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validated_source(name):
    if not FILE_RE.fullmatch(name):
        raise ValueError("bad incoming filename")
    path = INCOMING / name
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        s = os.fstat(fd)
        if not stat.S_ISREG(s.st_mode) or s.st_size <= 0 or s.st_size > MAX_BYTES:
            raise ValueError("invalid source file size/type")
        with os.fdopen(os.dup(fd), "rb") as reader:
            with Image.open(reader) as img:
                w, h = img.size
                if w < 1 or h < 1 or w * h > MAX_PIXELS:
                    raise ValueError("image dimensions rejected")
                if img.format not in ("PNG", "JPEG", "WEBP"):
                    raise ValueError("unsupported image encoding")
        return path, fd
    except BaseException:
        os.close(fd)
        raise


def submit(tenant, idem, filename):
    if not SLUG_RE.fullmatch(tenant) or not KEY_RE.fullmatch(idem):
        raise ValueError("invalid tenant or idempotency key")
    src, fd = validated_source(filename)
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(fd), "rb") as reader:
            digest = hashlib.sha256()
            while chunk := reader.read(512 * 1024):
                digest.update(chunk)
            fingerprint = digest.hexdigest()
        c = db()
        try:
            c.execute("BEGIN IMMEDIATE")
            previous = c.execute("SELECT * FROM jobs WHERE tenant=? AND idem=?", (tenant, idem)).fetchone()
            if previous:
                if previous["input_sha"] != fingerprint:
                    raise ValueError("idempotency conflict: same key, different image")
                c.commit()
                src.unlink(missing_ok=True)
                return {"id": previous["id"], "state": previous["state"], "existing": True}
            job_id = str(uuid.uuid4())
            work = JOBS / job_id
            work.mkdir(mode=0o700)
            final_input = work / ("original" + src.suffix.lower())
            os.lseek(fd, 0, os.SEEK_SET)
            with os.fdopen(os.dup(fd), "rb") as reader, final_input.open("xb") as writer:
                shutil.copyfileobj(reader, writer)
            os.chmod(final_input, 0o644)
            if hash_file(final_input) != fingerprint:
                raise ValueError("source copy checksum mismatch")
            now = int(time.time())
            c.execute("""INSERT INTO jobs(
              id,tenant,idem,input_name,input_sha,input_path,state,created,updated)
              VALUES(?,?,?,?,?,?, 'QUEUED',?,?)""",
              (job_id, tenant, idem, filename, fingerprint, str(final_input), now, now))
            c.commit()
            # Job has its own checksummed copy; purge the upload staging slot.
            src.unlink(missing_ok=True)
            return {"id": job_id, "state": "QUEUED", "existing": False}
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()
    finally:
        os.close(fd)


def snapshot(row):
    return {k: row[k] for k in (
        "id", "tenant", "state", "attempts", "created", "updated",
        "result_sha", "result_bytes", "result_width", "result_height",
        "source_deleted", "error"
    )}


def status(tenant, job_id):
    if not SLUG_RE.fullmatch(tenant):
        raise ValueError("invalid tenant")
    uuid.UUID(job_id)
    c = db()
    row = c.execute("SELECT * FROM jobs WHERE tenant=? AND id=?", (tenant, job_id)).fetchone()
    c.close()
    if not row:
        raise ValueError("job not found")
    return snapshot(row)


def process_one():
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        # Recover abandoned processing after restart; never cancel an active short-lived job.
        cutoff = int(time.time()) - 600
        c.execute("UPDATE jobs SET state='QUEUED' WHERE state='PROCESSING' AND updated < ?", (cutoff,))
        row = c.execute("SELECT * FROM jobs WHERE state='QUEUED' ORDER BY created,id LIMIT 1").fetchone()
        if row is None:
            c.commit()
            return {"state": "EMPTY"}
        jid = row["id"]
        c.execute("UPDATE jobs SET state='PROCESSING',attempts=attempts+1,updated=? WHERE id=?",
                  (int(time.time()), jid))
        c.commit()
        input_path = Path(row["input_path"])
        result_dir = RESULTS / row["tenant"] / jid
        result_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chown(result_dir, 10001, 10001)
        partial = result_dir / "optimized.part.jpg"
        output = result_dir / "optimized.jpg"
        partial.unlink(missing_ok=True)
        command = [
            "docker", "run", "--rm", "--pull=never", "--network", "none",
            "--name", "lightcraft-job-" + jid,
            "--cpus", "2", "--memory", "4g", "--memory-swap", "4g",
            "--pids-limit", "128", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--read-only",
            "--tmpfs", "/tmp:rw,nosuid,nodev,size=512m,mode=1777",
            "-e", "HOME=/tmp", "-e", "XDG_CACHE_HOME=/tmp/cache",
            "-e", "RAYON_NUM_THREADS=2", "-e", "LIGHTCRAFT_GPU=0",
            "-v", f"{input_path}:/input{input_path.suffix}:ro",
            "-v", f"{result_dir}:/output:rw",
            IMAGE, "render", f"/input{input_path.suffix}", "-o",
            "/output/optimized.part.jpg", "--size", "1600",
            "--quality", "82", "--opt", "metadata=none"
        ]
        try:
            r = subprocess.run(command, text=True, capture_output=True, timeout=120, check=False)
            if r.returncode:
                raise RuntimeError("container render failed")
            if not partial.is_file() or partial.stat().st_size < 500:
                raise RuntimeError("output file missing or too small")
            with Image.open(partial) as im:
                im.verify()
            with Image.open(partial) as im:
                w, h = im.size
                if im.format != "JPEG" or max(w, h) > 1600 or min(w, h) < 1:
                    raise RuntimeError("output dimensions or type failed")
                if im.getexif():
                    raise RuntimeError("output retained EXIF")
            os.replace(partial, output)
            output_sha = hash_file(output)
            count = output.stat().st_size
            # Rendering is NOT downstream delivery. Keep both temporary files until
            # the authorized destination verifies storage and acknowledges checksum.
            c.execute("""UPDATE jobs SET state='READY_FOR_DELIVERY',result_path=?,result_sha=?,
              result_bytes=?,result_width=?,result_height=?,updated=?,error=NULL WHERE id=?""",
              (str(output), output_sha, count, w, h, int(time.time()), jid))
            c.commit()
            return {"id": jid, "state": "READY_FOR_DELIVERY", "width": w,
                    "height": h, "bytes": count, "source_deleted": False}
        except (Exception,) as e:
            # A timed-out Docker CLI can leave its container behind; force cleanup.
            try:
                subprocess.run(["docker", "rm", "-f", "lightcraft-job-" + jid],
                               capture_output=True, timeout=10, check=False)
            except Exception:
                pass
            partial.unlink(missing_ok=True)
            refreshed = c.execute("SELECT attempts FROM jobs WHERE id=?", (jid,)).fetchone()
            retryable = refreshed["attempts"] < MAX_ATTEMPTS
            code = type(e).__name__ + ": " + str(e)[:120]
            c.execute("UPDATE jobs SET state=?,error=?,updated=? WHERE id=?",
                      ("QUEUED" if retryable else "FAILED", code, int(time.time()), jid))
            c.commit()
            return {"id": jid, "state": "QUEUED" if retryable else "FAILED", "error": code}
    finally:
        c.close()


def acknowledge(tenant, jid, confirmed_sha):
    """Trusted internal operator only: call after durable tenant-library upload is verified."""
    if not SLUG_RE.fullmatch(tenant):
        raise ValueError("invalid tenant")
    uuid.UUID(jid)
    if not re.fullmatch(r"[0-9a-f]{64}", confirmed_sha):
        raise ValueError("invalid checksum")
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM jobs WHERE tenant=? AND id=?", (tenant, jid)).fetchone()
        if row is None or row["result_sha"] != confirmed_sha:
            raise ValueError("job not found or checksum mismatch")
        if row["state"] == "DONE":
            c.commit()
            return {"id": jid, "state": "DONE", "existing": True}
        if row["state"] != "READY_FOR_DELIVERY":
            raise ValueError("job not ready for delivery")
        final = Path(row["result_path"])
        if not final.is_file() or hash_file(final) != confirmed_sha:
            raise ValueError("optimized output missing or hash mismatch")
        # Only a trusted backend with a durable-storage ACK should call this.
        c.execute("UPDATE jobs SET state='DONE',updated=? WHERE id=?",
                  (int(time.time()), jid))
        c.commit()
        source_deleted = True
        for path in (Path(row["input_path"]), final):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                source_deleted = False
        if source_deleted:
            c.execute("UPDATE jobs SET source_deleted=1,updated=? WHERE id=?",
                      (int(time.time()), jid))
            c.commit()
        return {"id": jid, "state": "DONE", "temporary_files_deleted": source_deleted}
    except BaseException:
        c.rollback()
        raise
    finally:
        c.close()


def retry(tenant, jid):
    if not SLUG_RE.fullmatch(tenant):
        raise ValueError("invalid tenant")
    uuid.UUID(jid)
    c = db()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM jobs WHERE tenant=? AND id=?", (tenant, jid)).fetchone()
        if not row or row["state"] != "FAILED":
            raise ValueError("only FAILED jobs may be retried")
        if not Path(row["input_path"]).exists():
            raise ValueError("source unavailable")
        c.execute("UPDATE jobs SET state='QUEUED',attempts=0,updated=?,error=NULL WHERE id=?",
                  (int(time.time()), jid))
        c.commit()
        return {"id": jid, "state": "QUEUED"}
    except BaseException:
        c.rollback()
        raise
    finally:
        c.close()


def main():
    os.umask(0o077)
    a = argparse.ArgumentParser()
    sub = a.add_subparsers(dest="action", required=True)
    x = sub.add_parser("submit")
    x.add_argument("tenant")
    x.add_argument("idempotency_key")
    x.add_argument("staging_filename")
    y = sub.add_parser("status")
    y.add_argument("tenant")
    y.add_argument("job_id")
    z = sub.add_parser("retry")
    z.add_argument("tenant")
    z.add_argument("job_id")
    ack = sub.add_parser("ack-delivery")
    ack.add_argument("tenant")
    ack.add_argument("job_id")
    ack.add_argument("confirmed_sha256")
    sub.add_parser("run-once")
    args = a.parse_args()
    try:
        if args.action == "submit":
            reply = submit(args.tenant, args.idempotency_key, args.staging_filename)
        elif args.action == "status":
            reply = status(args.tenant, args.job_id)
        elif args.action == "retry":
            reply = retry(args.tenant, args.job_id)
        elif args.action == "ack-delivery":
            reply = acknowledge(args.tenant, args.job_id, args.confirmed_sha256)
        else:
            import fcntl
            with (ROOT / ".worker.lock").open("a+") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    reply = {"state": "BUSY"}
                else:
                    reply = process_one()
        print(json.dumps({"ok": True, **reply}, separators=(",", ":")))
    except Exception as e:
        print(json.dumps({"ok": False, "error": type(e).__name__ + ": " + str(e)[:150]}))
        sys.exit(1)


if __name__ == "__main__":
    main()
