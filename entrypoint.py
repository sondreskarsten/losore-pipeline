"""Løsøreregisteret pipeline: collection + CDC in a single job.

Unlike the bulk-diff parsers (enheter, underenheter, roller) where
collection (brreg-downloader) and parsing are separate repos, this
pipeline combines API collection and CDC in one job. It scrapes the
løsøreregisteret API per orgnr, then diffs against stored state.

This is a PATTERN B pipeline: no dated snapshots. The changelog embeds
old/new values directly. snapshots.parquet is mutable (overwritten each run).

Flow:
  daily:   load pool → for each orgnr: fetch API → diff_orgnr() → changelog
  weekly:  download enhetsregisteret CSV → filter eligible → scan all 481K
           orgnrs → discover new ones → add to pool → diff
  bootstrap: read raw JSONL from GCS → backfill_orgnr() → initial state

Run modes via RUN_MODE env var:
  daily    — poll known orgnrs (pool.parquet), write changelog
  weekly   — full population scan, discover new orgnrs, diff all
  bootstrap — one-time load from raw regional JSONL files

Checkpoint: saves state every SAVE_EVERY (5000) orgnrs. Resumable on crash.
"""
import os
import sys
import csv
import gzip
import time
import json
import requests
from datetime import datetime, timezone

from cdc import StateManager, bootstrap_from_jsonl


BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
DELAY = float(os.environ.get("SCRAPE_DELAY", "0.05"))
SAVE_EVERY = int(os.environ.get("SAVE_EVERY", "5000"))
CHECKPOINT_EVERY = int(os.environ.get("CHECKPOINT_EVERY", "10000"))
GZ_FILE = "/tmp/enhetsregisteret_alle.csv.gz"

EXCLUDE_ORGFORMS = {"ENK", "UTLA", "KBO", "SAM", "ANNA", "VPFO", "PK", "PERS", "ADOS", "STAT"}


# ═══════════════════════════════════════════════════════════════
# Reuse collect_one from pipeline.py
# ═══════════════════════════════════════════════════════════════

from pipeline import collect_one, extract_rettsstiftelser, gcs_download, gcs_upload_json
from storage import GCSStore


def parse_rs_from_response(rsc_payload):
    """Extract rettsstiftelser list from an RSC payload string.

    Wrapper around ``extract_rettsstiftelser()`` with exception
    handling.  Returns empty list on any failure.

    Parameters
    ----------
    rsc_payload : str
        RSC flight stream line or decoded HTML push payload.

    Returns
    -------
    list[dict]
        Rettsstiftelser, or empty list on failure.
    """
    if not rsc_payload:
        return []
    try:
        rs_list = extract_rettsstiftelser(rsc_payload)
    except Exception:
        return []
    return rs_list if rs_list else []


def make_session():
    """Create an HTTP session pre-configured for RSC endpoint.

    Sets ``Rsc: 1`` and ``Accept: text/x-component`` headers so
    the server returns the flight stream format directly.

    Returns
    -------
    requests.Session
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "SparebankenNorge-LosoreAnalyse/2.0 (+https://sparebanken.no)",
        "Accept": "text/x-component",
        "Rsc": "1",
    })
    return session


def update_status(phase, detail=None, stats=None):
    """Write CDC-specific status to ``losore/state/cdc_status.json`` on GCS.

    Parameters
    ----------
    phase : str
        Current phase (``"collect"``, ``"saving"``, ``"done"``).
    detail : str or None
        Progress detail.
    stats : dict or None
        Changelog summary statistics.
    """
    status = {
        "phase": phase,
        "detail": detail,
        "stats": stats,
        "run_mode": RUN_MODE,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    gcs_upload_json(status, f"losore/state/cdc_status.json")
    print(f"[{RUN_MODE}/{phase}] {detail or ''}", flush=True)


# ═══════════════════════════════════════════════════════════════
# Daily: check all monitored orgnr
# ═══════════════════════════════════════════════════════════════

def run_daily():
    """Daily CDC mode: check all monitored orgnrs for changes.

    Loads pool from GCS, iterates all ~92K orgnrs, collects current
    rettsstiftelser via ``collect_one()``, diffs against stored
    snapshots via ``StateManager.diff_orgnr()``.  Saves state and
    changelog to GCS.
    """
    state = StateManager()
    state.load()

    orgnr_list = state.pool_orgnr_list()
    total = len(orgnr_list)
    update_status("collect", f"Checking {total:,} monitored orgnr")

    session = make_session()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    store = GCSStore(BUCKET)
    checked = 0
    errors = 0
    t0 = time.time()

    for i, orgnr in enumerate(orgnr_list):
        try:
            result = collect_one(orgnr, session)
        except Exception as e:
            errors += 1
            if errors % 100 == 0:
                print(f"  {errors} errors so far, last: {e}", flush=True)
            continue

        store.buffer_raw(orgnr, today, result)
        rsc_payload = result.get("rsc_payload", "") or ""
        rs_list = parse_rs_from_response(rsc_payload)
        changes = state.diff_orgnr(orgnr, rs_list, source="daily")
        state.update_pool_entry(orgnr, len(rs_list), len(changes) > 0)
        checked += 1

        if DELAY > 0:
            time.sleep(DELAY)

        if checked % 1000 == 0:
            elapsed = time.time() - t0
            rate = checked / elapsed
            eta_min = (total - checked) / rate / 60
            print(f"  {checked:,}/{total:,}  ({rate:.1f}/s, ETA {eta_min:.0f}m)  "
                  f"changes: {len(state._changelog):,}", flush=True)

        if checked % SAVE_EVERY == 0:
            store.flush()

        if checked % CHECKPOINT_EVERY == 0:
            update_status("collect", f"{checked:,}/{total:,}", state.changelog_summary())

    elapsed = time.time() - t0
    update_status("saving", f"Checked {checked:,} in {elapsed/60:.1f}m, {errors} errors")
    state.save()
    store.flush()

    summary = state.changelog_summary()
    update_status("done", f"Daily complete: {summary}", summary)


# ═══════════════════════════════════════════════════════════════
# Weekly: full population scan
# ═══════════════════════════════════════════════════════════════

def load_all_orgnr():
    """Load all eligible orgnrs from the enhetsregisteret CSV.

    Downloads the bulk CSV from GCS (cached from a prior regional
    scrape), filters out:

    - Excluded org forms: ENK, UTLA, KBO, SAM, ANNA, VPFO, PK,
      PERS, ADOS, STAT (sole proprietors, foreign entities,
      municipalities, etc.)
    - Entities not registered in Foretaksregisteret

    Remaining ~481K orgnrs (~42% of the register) are the scrape
    population for the weekly scan.

    Returns
    -------
    list[str]
        Sorted orgnrs to scan.
    """
    gz_path = f"losore/2026-03-21-agder/enhetsregisteret_alle.csv.gz"

    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    bucket = client.bucket(BUCKET)

    if not os.path.exists(GZ_FILE):
        blob = bucket.blob("losore/2026-03-21-agder/enhetsregisteret_alle.csv.gz")
        if blob.exists():
            blob.download_to_filename(GZ_FILE)
            print(f"  Downloaded enhetsregisteret ({os.path.getsize(GZ_FILE)/1e6:.1f} MB)")

    orgnr_list = []
    total_raw = 0
    excluded_orgform = 0
    excluded_not_foretak = 0
    with gzip.open(GZ_FILE, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_raw += 1
            orgnr = row.get("organisasjonsnummer", "")
            if not orgnr or len(orgnr) != 9:
                continue

            orgform = row.get("organisasjonsform.kode", "")
            if orgform in EXCLUDE_ORGFORMS:
                excluded_orgform += 1
                continue

            foretak = row.get("registrertIForetaksregisteret", "").lower() in ("j", "true", "ja")
            if not foretak:
                excluded_not_foretak += 1
                continue

            orgnr_list.append(orgnr)

    print(f"  Enhetsregisteret total:          {total_raw:,}", flush=True)
    print(f"  Excluded orgform (Tier 1):       {excluded_orgform:,}  {EXCLUDE_ORGFORMS}", flush=True)
    print(f"  Excluded not foretaksregisteret: {excluded_not_foretak:,}", flush=True)
    print(f"  Remaining to scan:               {len(orgnr_list):,}  ({len(orgnr_list)/total_raw*100:.1f}%)", flush=True)
    return orgnr_list


CURSOR_PATH = "losore/state/weekly_cursor.json"


def load_weekly_cursor():
    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    blob = client.bucket(BUCKET).blob(CURSOR_PATH)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def save_weekly_cursor(cursor):
    gcs_upload_json(cursor, CURSOR_PATH)


def delete_weekly_cursor():
    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    blob = client.bucket(BUCKET).blob(CURSOR_PATH)
    if blob.exists():
        blob.delete()
    print("  weekly cursor deleted", flush=True)


def run_weekly():
    """Weekly CDC mode: scan full population for new orgnrs with rettsstiftelser.

    Resumable across multiple executions via a cursor file on GCS.
    Each run picks up where the previous one left off until the full
    population is scanned, then deletes the cursor.
    """
    state = StateManager()
    state.load(lightweight=True)

    all_orgnr = load_all_orgnr()
    total = len(all_orgnr)

    cursor = load_weekly_cursor()
    start_index = 0
    new_discoveries = 0
    if cursor and cursor.get("total") == total:
        start_index = cursor.get("next_index", 0)
        new_discoveries = cursor.get("new_discoveries", 0)
        print(f"  Resuming from index {start_index:,}/{total:,} "
              f"(prior discoveries: {new_discoveries:,})", flush=True)
    elif cursor:
        print(f"  Cursor stale (total mismatch {cursor.get('total')} vs {total}), starting fresh", flush=True)

    update_status("collect", f"Scanning {start_index:,}→{total:,} ({total-start_index:,} remaining)")

    session = make_session()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    store = GCSStore(BUCKET)
    checked = 0
    errors = 0
    t0 = time.time()

    for i in range(start_index, total):
        orgnr = all_orgnr[i]
        try:
            result = collect_one(orgnr, session)
        except Exception as e:
            errors += 1
            continue

        store.buffer_raw(orgnr, today, result)
        rsc_payload = result.get("rsc_payload", "") or ""
        rs_list = parse_rs_from_response(rsc_payload)

        if rs_list and not state.is_in_pool(orgnr):
            state.add_to_pool(orgnr, len(rs_list), source="weekly_discovery")
            state.backfill_orgnr(orgnr, rs_list)
            new_discoveries += 1

        if state.is_in_pool(orgnr):
            changes = state.diff_orgnr(orgnr, rs_list, source="weekly")
            state.update_pool_entry(orgnr, len(rs_list), len(changes) > 0)

        checked += 1

        if DELAY > 0:
            time.sleep(DELAY)

        if checked % 5000 == 0:
            elapsed = time.time() - t0
            rate = checked / elapsed
            remaining = total - (start_index + checked)
            eta_h = remaining / rate / 3600
            print(f"  {start_index+checked:,}/{total:,}  ({rate:.1f}/s, ETA {eta_h:.1f}h)  "
                  f"new: {new_discoveries:,}  changes: {len(state._changelog):,}", flush=True)

        if checked % CHECKPOINT_EVERY == 0:
            update_status("collect", f"{start_index+checked:,}/{total:,} new:{new_discoveries}")

        if checked % SAVE_EVERY == 0:
            state.save()
            store.flush()
            save_weekly_cursor({
                "next_index": i + 1,
                "total": total,
                "new_discoveries": new_discoveries,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })

    elapsed = time.time() - t0
    update_status("saving", f"Scanned {checked:,} in {elapsed/3600:.1f}h")
    state.save()
    store.flush()
    delete_weekly_cursor()

    summary = state.changelog_summary()
    update_status("done", f"Weekly complete: new={new_discoveries}, {summary}", summary)


# ═══════════════════════════════════════════════════════════════
# Bootstrap: build initial state from existing JSONL on GCS
# ═══════════════════════════════════════════════════════════════

def run_bootstrap():
    """Bootstrap CDC state from existing regional scrape JSONL files.

    Downloads all ``raw_responses.jsonl`` files from
    ``gs://sondre_brreg_data/losore/2026-03-21-*/`` (16 regional
    scrapes), passes to ``bootstrap_from_jsonl()`` which builds
    the initial pool (~92K orgnrs) and snapshot index (~310K
    documents).
    """
    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    bucket = client.bucket(BUCKET)

    local_dir = "/tmp/raw"
    os.makedirs(local_dir, exist_ok=True)

    blobs = list(bucket.list_blobs(prefix="losore/2026-03-21-"))
    jsonl_blobs = [b for b in blobs if b.name.endswith("raw_responses.jsonl")]

    for blob in jsonl_blobs:
        region = blob.name.split("/")[1]
        region_dir = os.path.join(local_dir, region)
        os.makedirs(region_dir, exist_ok=True)
        local_path = os.path.join(region_dir, "raw_responses.jsonl")
        if not os.path.exists(local_path):
            update_status("download", f"Downloading {blob.name}")
            blob.download_to_filename(local_path)
            print(f"  {blob.name} → {local_path} ({os.path.getsize(local_path)/1e6:.1f} MB)")

    update_status("bootstrap", "Building initial state from JSONL")
    bootstrap_from_jsonl(local_dir)
    update_status("done", "Bootstrap complete")


def run_convert_bootstrap():
    """Convert bootstrap JSONL files to per-orgnr raw/ + manifest/ format.

    Processes each regional ``raw_responses.jsonl`` sequentially,
    writing one ``losore/raw/2026-03-21/{orgnr}.json`` per record
    and appending to ``losore/manifest/2026-03-21.jsonl``.
    """
    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    bucket = client.bucket(BUCKET)

    date_str = "2026-03-21"
    store = GCSStore(BUCKET)

    blobs = sorted(
        [b for b in bucket.list_blobs(prefix="losore/2026-03-21-")
         if b.name.endswith("raw_responses.jsonl")],
        key=lambda x: x.size
    )

    seen = set()
    total = 0

    for blob_ref in blobs:
        region = blob_ref.name.split("/")[1]
        blob_ref.reload()
        print(f"\n{region} ({blob_ref.size/1e6:.0f} MB)", flush=True)

        local_jsonl = f"/tmp/{region}.jsonl"
        blob_ref.download_to_filename(local_jsonl)

        count = 0
        with open(local_jsonl) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                orgnr = rec.get("orgnr", "")
                if not orgnr or orgnr in seen:
                    continue
                seen.add(orgnr)

                store.buffer_raw(orgnr, date_str, rec)
                count += 1

                if count % SAVE_EVERY == 0:
                    store.flush()
                    print(f"  {count:,} flushed", flush=True)

        store.flush()
        os.remove(local_jsonl)
        total += count
        print(f"  {region}: {count:,} orgnr", flush=True)

    update_status("done", f"Convert bootstrap complete: {total:,} orgnr → raw/{date_str}/")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    """CDC entrypoint: dispatch to daily, weekly, bootstrap, or convert_bootstrap mode."""
    print(f"{'='*60}", flush=True)
    print(f"  losore-pipeline CDC — mode: {RUN_MODE}", flush=True)
    print(f"  {datetime.now(timezone.utc).isoformat()}", flush=True)
    print(f"{'='*60}", flush=True)

    dispatch = {
        "daily": run_daily,
        "weekly": run_weekly,
        "bootstrap": run_bootstrap,
        "convert_bootstrap": run_convert_bootstrap,
    }

    if RUN_MODE not in dispatch:
        print(f"Unknown RUN_MODE: {RUN_MODE}. Use: {', '.join(dispatch)}")
        sys.exit(1)

    dispatch[RUN_MODE]()


if __name__ == "__main__":
    main()
