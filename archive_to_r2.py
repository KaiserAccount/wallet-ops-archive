#!/usr/bin/env python3
"""
One-shot archive: Supabase → Cloudflare R2, then DELETE/TRUNCATE archived rows.

Flow per table:
  1. Stream rows out of Postgres via COPY TO STDOUT  (no full load into RAM)
  2. Compress on the fly with gzip
  3. Push to R2 in 64 MB multipart chunks
  4. Verify the R2 object exists and has non-zero size
  5. Only then DELETE / TRUNCATE the source table
  6. Run VACUUM ANALYZE to reclaim disk and refresh planner stats

Required env vars:
  SUPABASE_HOST, SUPABASE_PORT, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_DBNAME,
  R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT

SUPABASE_HOST must be the Supavisor session pooler (aws-0-<region>.pooler.supabase.com).
db.<ref>.supabase.co is AAAA-only. SUPABASE_PORT must be 5432 (session mode).
"""

import io
import os
import queue
import socket
import struct
import sys
import threading
import time
import zlib
from datetime import datetime
from pathlib import Path

import boto3
import psycopg2
from dotenv import load_dotenv

# Load .env from the same directory as this script — no-op if file is absent
load_dotenv(Path(__file__).parent / ".env")

# cachebust 2026-09-29T19:15Z — force Railway COPY of score_history image
# ── Date suffix used in every R2 filename (UTC, fixed at startup) ─────────────
DATE_SUFFIX = datetime.utcnow().strftime("%Y%m%d")

# ── Credentials — all sourced from env vars, never hardcoded ──────────────────
# DB params are kept separate (not a URL) so psycopg2 never parses the username —
# URL parsing silently truncates "postgres.trzfysszmrgogpeitzfk" to "postgres",
# which breaks Supavisor tenant lookup and causes password auth failures.
SUPABASE_HOST   = os.environ["SUPABASE_HOST"]
SUPABASE_PORT   = int(os.environ["SUPABASE_PORT"])
SUPABASE_USER   = os.environ["SUPABASE_USER"]
SUPABASE_PASS   = os.environ["SUPABASE_PASSWORD"]
SUPABASE_DBNAME = os.environ["SUPABASE_DBNAME"]

R2_ACCOUNT_ID   = os.environ["R2_ACCOUNT_ID"]
R2_BUCKET        = os.environ["R2_BUCKET"]
R2_ACCESS_KEY    = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_KEY    = os.environ["R2_SECRET_ACCESS_KEY"]
R2_ENDPOINT      = os.environ["R2_ENDPOINT"]

RUN_INTERVAL_HOURS   = int(os.environ.get("RUN_INTERVAL_HOURS", "168"))
ARCHIVE_RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "14"))

# Supavisor: 5432 is session mode (one backend for the whole client session).
# 6543 is transaction mode, which cannot hold a long COPY or run VACUUM.
SESSION_MODE_PORT = 5432
TRANSACTION_MODE_PORT = 6543

# ── 64 MB compressed per multipart part ───────────────────────────────────────
# R2 requires parts ≥ 5 MB (except the last). 64 MB keeps part count low for
# large tables while staying well within the pooler memory limit.
CHUNK_SIZE = 64 * 1024 * 1024


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def log(label, msg):
    """Timestamped print — every line carries a UTC clock so a long run is easy to trace."""
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] [{label}] {msg}", flush=True)


def ipv4_addresses(host):
    """A records only. Returns [] when the name is IPv6-only or does not resolve."""
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    seen = []
    for info in infos:
        addr = info[4][0]
        if addr not in seen:
            seen.append(addr)
    return seen


def ipv4_connect_target(host, port, user, resolve=ipv4_addresses):
    """
    Choose the libpq endpoint for SUPABASE_HOST.

    Returns (tls_host, user, ipv4_addrs). tls_host is passed as host= so TLS
    SNI stays a hostname. Each address is passed as hostaddr= so libpq dials
    an A record and never an AAAA.

    The region is not hardcoded. Railway sets SUPABASE_HOST to the session
    pooler (aws-0-<region>.pooler.supabase.com), which has A records.
    db.<ref>.supabase.co does not, and Railway IPv6 egress still cannot reach
    that AAAA (ENETUNREACH in europe-west4 with ipv6EgressEnabled).
    """
    host = host.strip().rstrip(".").lower()
    if port == TRANSACTION_MODE_PORT:
        raise RuntimeError(
            "SUPABASE_PORT=6543 is the transaction pooler. "
            f"Long COPY and VACUUM need session mode: set SUPABASE_PORT={SESSION_MODE_PORT} "
            "and SUPABASE_HOST to aws-0-<region>.pooler.supabase.com. "
            "SUPABASE_USER must be <role>.<project-ref>."
        )
    addrs = resolve(host)
    if not addrs:
        raise RuntimeError(
            f"No IPv4 address for {host}. "
            "db.<ref>.supabase.co is AAAA-only; forcing IPv4 on that name cannot work, "
            "and Railway IPv6 egress to it fails with Network is unreachable. "
            "Set SUPABASE_HOST to the session pooler "
            f"(aws-0-<region>.pooler.supabase.com) and SUPABASE_PORT={SESSION_MODE_PORT}. "
            "SUPABASE_USER must be <role>.<project-ref>."
        )
    return host, user, addrs


def _retryable_connect_error(exc):
    """True for dial failures. Auth and tenant errors must not be retried."""
    msg = str(exc).lower()
    if "fatal" in msg or "password" in msg or "authentication" in msg:
        return False
    return any(marker in msg for marker in (
        "network is unreachable",
        "no route to host",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "could not connect to server",
    ))


def connect_supabase(resolve=ipv4_addresses, connect=None):
    """
    Open one Postgres session over IPv4 using SUPABASE_HOST and SUPABASE_PORT.

    host= is the TLS/SNI name. hostaddr= is the A record libpq dials.
    Port 5432 on the shared pooler is session mode (required for COPY and VACUUM).
    """
    connect = connect or psycopg2.connect
    host, user, addrs = ipv4_connect_target(
        SUPABASE_HOST,
        SUPABASE_PORT,
        SUPABASE_USER,
        resolve=resolve,
    )

    last_exc = None
    for addr in addrs:
        # Host, port, and address only. The password is never logged.
        log("main", f"Connecting host={host} port={SUPABASE_PORT} hostaddr={addr}")
        try:
            return connect(
                host=host,
                hostaddr=addr,
                port=SUPABASE_PORT,
                user=user,
                password=SUPABASE_PASS,
                dbname=SUPABASE_DBNAME,
                sslmode="require",
                connect_timeout=30,
                # statement_timeout=0 and idle_in_transaction_session_timeout=0 are baked
                # into the archiver role via ALTER ROLE — no need to set them here.
                # TCP keepalives prevent the Supabase pooler from silently dropping the
                # connection during the long COPY stream (PGRES_COPY_OUT drop = libpq error).
                keepalives=1,
                keepalives_idle=30,       # start probing after 30 s of silence
                keepalives_interval=10,   # probe every 10 s
                keepalives_count=5,       # drop after 5 missed probes (~80 s total)
            )
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if addr == addrs[-1] or not _retryable_connect_error(exc):
                raise
            log("main", f"IPv4 {addr} unreachable, trying next A record")
    raise last_exc


def s3_client():
    """Return a boto3 S3 client pointed at the R2-compatible endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",   # R2 ignores region but boto3 requires a non-empty value
    )


def estimate_rows(conn, query):
    """
    Pull a planner row-count estimate via EXPLAIN — near-instant, no data read.
    Used only for operator-facing progress context, not for correctness decisions.
    Returns 0 if the plan output has no rows= token (e.g. empty table).
    """
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN {query}")
        for row in cur.fetchall():
            line = row[0]
            if "rows=" in line:
                return int(line.split("rows=")[1].split()[0])
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Core pipeline
# ─────────────────────────────────────────────────────────────────────────────

def stream_copy_to_r2(conn, copy_sql, s3_key, label):
    """
    Postgres COPY → queue → gzip → R2 multipart upload, using two threads.

    Root cause of prior timeouts: copy_expert is synchronous. While the main
    thread blocked on s3.upload_part() (60+ seconds for 67 MB), psycopg2 stopped
    reading from the Postgres socket. Supavisor's internal idle timer saw silence
    on the COPY stream and cancelled the statement — even with statement_timeout=0
    on the role, because Supavisor enforces its own client-side timeout.

    Fix: a dedicated reader thread drains the Postgres socket continuously into a
    unbounded queue. The main thread compresses and uploads from the queue at
    whatever pace S3 allows. Postgres never sees silence; Supavisor never fires.
    The queue is unbounded so the reader is never blocked by a slow S3 upload.
    """
    s3 = s3_client()

    log(label, f"Initiating multipart upload → s3://{R2_BUCKET}/{s3_key}")
    mp        = s3.create_multipart_upload(Bucket=R2_BUCKET, Key=s3_key, ContentEncoding="gzip")
    upload_id = mp["UploadId"]
    log(label, f"Upload ID: {upload_id}")

    # ── Reader thread: drain Postgres → queue ─────────────────────────────────
    # Queue items are raw bytes chunks from psycopg2; None is the end sentinel.
    # Unbounded (maxsize=0) so the reader never blocks waiting for the uploader.
    data_q      = queue.Queue(maxsize=0)
    reader_exc  = [None]   # thread-safe single-slot error channel

    class _QueueWriter(io.RawIOBase):
        def write(self, chunk):
            data_q.put(bytes(chunk))
            return len(chunk)

    def _pg_reader():
        writer = _QueueWriter()   # named ref prevents GC during copy_expert
        try:
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, writer)
        except Exception as exc:
            reader_exc[0] = exc
        finally:
            data_q.put(None)      # always send sentinel so main thread unblocks

    reader = threading.Thread(target=_pg_reader, daemon=True, name="pg-reader")
    reader.start()
    log(label, "COPY TO STDOUT started — reader thread draining Postgres ...")

    # ── Main thread: compress queue items → upload to R2 ─────────────────────
    parts       = []
    part_num    = 0
    total_bytes = 0
    t_start     = time.time()

    # Manual gzip stream: raw deflate (wbits<0) + our own header/footer.
    # Avoids gzip.GzipFile whose Python 3.13 internal BufferedWriter crashes on close().
    compressor = zlib.compressobj(level=6, method=zlib.DEFLATED, wbits=-zlib.MAX_WBITS)
    crc32_val  = 0
    raw_size   = 0

    # Minimal valid gzip header (RFC 1952): magic + deflate + no flags + mtime=0 + OS=unknown
    buf = bytearray(b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff')

    def upload_part(data: bytes):
        """Upload exactly `data` as one multipart part and log progress."""
        nonlocal part_num, total_bytes
        part_num += 1
        elapsed  = time.time() - t_start
        speed    = (total_bytes / elapsed / 1e6) if elapsed > 0 else 0
        log(label,
            f"Uploading part {part_num}  "
            f"({len(data)/1e6:.1f} MB this part, "
            f"{total_bytes/1e6:.1f} MB so far, "
            f"{speed:.1f} MB/s avg)")
        resp = s3.upload_part(
            Bucket=R2_BUCKET, Key=s3_key, UploadId=upload_id,
            PartNumber=part_num, Body=data,
        )
        parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
        total_bytes += len(data)

    def flush_full_parts():
        # R2 requires every non-trailing part to be EXACTLY CHUNK_SIZE bytes.
        # Slice precise CHUNK_SIZE windows from buf, leave the remainder for later.
        while len(buf) >= CHUNK_SIZE:
            upload_part(bytes(buf[:CHUNK_SIZE]))
            del buf[:CHUNK_SIZE]

    HEARTBEAT_INTERVAL = 30   # seconds between "still alive" log lines
    last_heartbeat = time.time()

    try:
        while True:
            try:
                chunk = data_q.get(timeout=HEARTBEAT_INTERVAL)
            except queue.Empty:
                elapsed = time.time() - t_start
                log(label,
                    f"  ⏳ still reading from Postgres ...  "
                    f"{raw_size/1e6:.1f} MB raw received, "
                    f"{len(buf)/1e6:.1f} MB compressed in buffer, "
                    f"{elapsed:.0f}s elapsed")
                last_heartbeat = time.time()
                continue

            if chunk is None:           # sentinel: reader finished or errored
                break
            crc32_val  = zlib.crc32(chunk, crc32_val) & 0xffffffff
            raw_size  += len(chunk)
            buf.extend(compressor.compress(chunk))
            flush_full_parts()          # only emits exact CHUNK_SIZE slices

            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                elapsed = now - t_start
                log(label,
                    f"  📥 reading ...  "
                    f"{raw_size/1e6:.1f} MB raw, "
                    f"{len(buf)/1e6:.1f} MB buffered, "
                    f"{part_num} parts uploaded, "
                    f"{elapsed:.0f}s elapsed")
                last_heartbeat = now

        reader.join()
        if reader_exc[0]:               # propagate any error from the reader thread
            raise reader_exc[0]

        # Finalize: flush deflate state, append gzip footer (CRC32 + original size mod 2^32)
        buf.extend(compressor.flush())
        buf.extend(struct.pack("<II", crc32_val, raw_size & 0xffffffff))
        # Upload whatever remains as the final (trailing) part — any size is allowed
        if buf:
            upload_part(bytes(buf))
            buf.clear()

        log(label, f"Completing multipart upload ({part_num} parts, {total_bytes/1e6:.1f} MB) ...")
        s3.complete_multipart_upload(
            Bucket=R2_BUCKET, Key=s3_key, UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        log(label, f"Upload finished in {time.time() - t_start:.1f}s — {s3_key}")
        return total_bytes

    except Exception as exc:
        reader.join(timeout=5)          # give reader a moment to exit cleanly
        log(label, f"✗ Upload failed — aborting. Source rows are untouched.")
        log(label, f"ERROR: {exc}")
        log(label, "Aborting multipart upload to clean up R2 partial data ...")
        s3.abort_multipart_upload(Bucket=R2_BUCKET, Key=s3_key, UploadId=upload_id)
        raise RuntimeError(f"[{label}] multipart upload aborted: {exc}") from exc


def verify_r2_object(s3_key, label):
    """
    HEAD the R2 object and confirm ContentLength > 0.

    This is the critical safety gate: we only allow DELETE/TRUNCATE after this
    passes. A silent upload failure (e.g. network drop after complete_multipart)
    would otherwise wipe the source table with no archive to fall back on.
    """
    log(label, f"Verifying R2 object exists and is non-empty: {s3_key}")
    s3   = s3_client()
    head = s3.head_object(Bucket=R2_BUCKET, Key=s3_key)
    size = head["ContentLength"]

    if size == 0:
        raise RuntimeError(
            f"[{label}] R2 object {s3_key} has ContentLength=0 — "
            "refusing to delete source rows to prevent data loss"
        )

    log(label,
        f"R2 object verified ✓  " 
        f"size={size / 1e6:.1f} MB  " 
        f"last-modified={head.get('LastModified')}")
    return size


def vacuum_table(conn, table, label):
    """
    Run VACUUM ANALYZE after deletion to reclaim disk space and refresh stats.

    VACUUM cannot run inside a transaction, so we temporarily drop to autocommit
    (isolation_level=0), run it, then restore the original isolation level.
    On large tables this can take several minutes — the log line makes that visible.
    """
    log(label, f"Running VACUUM ANALYZE {table}  (may take a few minutes on large tables) ...")
    t0 = time.time()

    # VACUUM requires autocommit — save and restore the connection's isolation level
    old_iso = conn.isolation_level
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute(f"VACUUM ANALYZE {table}")
        log(label, f"VACUUM ANALYZE done in {time.time() - t0:.1f}s")
    except Exception as exc:
        # archiver role lacks superuser — VACUUM will be skipped here.
        # Run manually via Supabase SQL editor: VACUUM ANALYZE {table};
        log(label, f"VACUUM skipped (permission denied for this role): {exc}")
    finally:
        conn.set_isolation_level(old_iso)


# ─────────────────────────────────────────────────────────────────────────────
# Per-table archive jobs
# ─────────────────────────────────────────────────────────────────────────────

def archive_events(conn):
    """
    Export wallet_intel.events rows older than ARCHIVE_RETENTION_DAYS to R2, then DELETE them.

    The same WHERE clause is used in COPY, the row estimate, and the DELETE so
    no rows can fall through the gap between export and cleanup.
    """
    label  = "wallet_intel.events"
    s3_key = f"wallet_intel_events_archive_{DATE_SUFFIX}.csv.gz"
    where  = f"occurred_at < NOW() - INTERVAL '{ARCHIVE_RETENTION_DAYS} days'"

    columns = (
        "id, event_source, event_type, signature, wallet_address, occurred_at, ingested_at, "
        "token_mint, token_symbol, action, amount_sol, amount_token, price_per_token, dedupe_key, "
        "source_ref, metadata, created_at, market_cap_at_entry"
    )
    select_sql = f"SELECT {columns} FROM wallet_intel.events WHERE {where}"
    copy_sql   = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT CSV, HEADER)"

    est = estimate_rows(conn, select_sql)
    log(label, f"Planner estimate: ~{est:,} rows matching ({where})")
    log(label, f"Destination: s3://{R2_BUCKET}/{s3_key}")
    t_section = time.time()

    # 1 — export
    stream_copy_to_r2(conn, copy_sql, s3_key, label)

    # 2 — verify before touching source data
    verify_r2_object(s3_key, label)

    # 3 — safe to delete now
    log(label, "Archive confirmed on R2 — running DELETE ...")
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM wallet_intel.events WHERE {where}")
        deleted = cur.rowcount
    conn.commit()
    log(label, f"Deleted {deleted:,} rows — transaction committed")

    # 4 — reclaim space, update planner stats
    vacuum_table(conn, "wallet_intel.events", label)

    log(label, f"Done in {time.time() - t_section:.1f}s total")


def archive_parser_failures(conn):
    """
    Export ALL rows in public.parser_failures to R2, then TRUNCATE the table.

    TRUNCATE (instead of DELETE) is used because we're clearing 100% of the
    table — it's instant and immediately reclaims the high-water storage mark.
    """
    label  = "public.parser_failures"
    s3_key = f"parser_failures_archive_{DATE_SUFFIX}.csv.gz"

    columns = (
        "id, source, failure_type, raw_data, error_message, signature, wallet_address, "
        "occurred_at, resolved, metadata"
    )
    select_sql = f"SELECT {columns} FROM public.parser_failures"
    copy_sql   = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT CSV, HEADER)"

    est = estimate_rows(conn, select_sql)
    log(label, f"Planner estimate: ~{est:,} rows (full table export)")
    log(label, f"Destination: s3://{R2_BUCKET}/{s3_key}")
    t_section = time.time()

    # 1 — export
    stream_copy_to_r2(conn, copy_sql, s3_key, label)

    # 2 — verify before touching source data
    verify_r2_object(s3_key, label)

    # 3 — safe to truncate now
    log(label, "Archive confirmed on R2 — running TRUNCATE ...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE public.parser_failures")
    conn.commit()
    log(label, "Table truncated — transaction committed")

    # 4 — refresh catalog stats (fast on a just-truncated table)
    vacuum_table(conn, "public.parser_failures", label)

    log(label, f"Done in {time.time() - t_section:.1f}s total")


def archive_wallet_score_history(conn):
    """
    Export wallet_intel.wallet_score_history rows older than ARCHIVE_RETENTION_DAYS to R2,
    then DELETE them and VACUUM.

    Retention is scored_at, not created_at. Same gate as events: COPY to R2,
    verify the object, then DELETE, then VACUUM.
    """
    label  = "wallet_intel.wallet_score_history"
    s3_key = f"wallet_score_history_archive_{DATE_SUFFIX}.csv.gz"
    where  = f"scored_at < NOW() - INTERVAL '{ARCHIVE_RETENTION_DAYS} days'"

    columns = (
        "id, wallet_address, scored_at, performance_score, earliness_score, "
        "predictive_score, consistency_score, trust_score, copyability_score, "
        "hot_streak_score, telegram_confluence_score, cohort_leadership_score, "
        "exit_timing_score, overall_rank, category_ranks, sample_size, "
        "confidence_interval, score_vector"
    )
    select_sql = f"SELECT {columns} FROM wallet_intel.wallet_score_history WHERE {where}"
    copy_sql   = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT CSV, HEADER)"

    est = estimate_rows(conn, select_sql)
    log(label, f"Planner estimate: ~{est:,} rows matching ({where})")
    log(label, f"Destination: s3://{R2_BUCKET}/{s3_key}")
    t_section = time.time()

    # 1 — export
    stream_copy_to_r2(conn, copy_sql, s3_key, label)

    # 2 — verify before touching source data
    verify_r2_object(s3_key, label)

    # 3 — safe to delete now
    log(label, "Archive confirmed on R2 — running DELETE ...")
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM wallet_intel.wallet_score_history WHERE {where}")
        deleted = cur.rowcount
    conn.commit()
    log(label, f"Deleted {deleted:,} rows — transaction committed")

    # 4 — reclaim the large index/TOAST overhead (392 MB per dashboard snapshot)
    vacuum_table(conn, "wallet_intel.wallet_score_history", label)

    log(label, f"Done in {time.time() - t_section:.1f}s total")


# Production archive list. score history is filtered on scored_at.
# parser_failures exports the whole table, then TRUNCATE.
ARCHIVE_JOBS = (
    ("wallet_intel.events", archive_events),
    ("wallet_intel.wallet_score_history", archive_wallet_score_history),
    ("public.parser_failures", archive_parser_failures),
)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print(f"  Supabase → R2 Archive   run date: {DATE_SUFFIX} UTC")
    print(f"  Bucket  : {R2_BUCKET}")
    print(f"  Endpoint: {R2_ENDPOINT}")
    print("=" * 64)

    mode = "session" if SUPABASE_PORT == SESSION_MODE_PORT else "custom"
    log(
        "main",
        f"Postgres connect host={SUPABASE_HOST} port={SUPABASE_PORT} mode={mode}",
    )
    conn = connect_supabase()
    conn.autocommit = False
    log("main", f"Connected host={SUPABASE_HOST} port={SUPABASE_PORT}")
    log("main", "Archive jobs: " + ", ".join(name for name, _job in ARCHIVE_JOBS))
    log("main", f"wallet_score_history retention: scored_at < now - {ARCHIVE_RETENTION_DAYS} days")

    t_total = time.time()
    try:
        for _name, job in ARCHIVE_JOBS:
            job(conn)
    except Exception as exc:
        try:
            conn.rollback()
            log("main", "Transaction rolled back. Source tables are untouched.")
        except Exception:
            log("main", "Connection already closed — rollback skipped. Source tables are untouched.")
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log("main", "Database connection closed")

    print("=" * 64)
    log("main", f"All done — total elapsed {time.time() - t_total:.1f}s")
    print("=" * 64)


if __name__ == "__main__":
    log("scheduler", f"Archive service starting — interval={RUN_INTERVAL_HOURS}h")
    retry_delay = 300  # 5 min between retries on failure — prevents circuit breaker on rapid restarts
    while True:
        try:
            main()
            log("scheduler", "━" * 40)
            log("scheduler", f"✓ All tables archived successfully")
            log("scheduler", f"Next run in {RUN_INTERVAL_HOURS}h ({RUN_INTERVAL_HOURS//24}d) — sleeping...")
            time.sleep(RUN_INTERVAL_HOURS * 3600)
        except Exception as exc:
            log("scheduler", f"✗ Run failed: {exc}")
            log("scheduler", f"Retrying in {retry_delay // 60} minutes ...")
            time.sleep(retry_delay)
