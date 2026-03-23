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


# ═══════════════════════════════════════════════════════════════
# Reuse collect_one from pipeline.py
# ═══════════════════════════════════════════════════════════════

from pipeline import collect_one, extract_rettsstiftelser, gcs_download, gcs_upload_json


def parse_rs_from_response(rsc_payload):
    if not rsc_payload:
        return []
    rs_list = extract_rettsstiftelser(rsc_payload)
    return rs_list if rs_list else []


def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "SparebankenNorge-LosoreAnalyse/2.0 (+https://sparebanken.no)",
        "Accept": "text/x-component",
        "Rsc": "1",
    })
    return session


def update_status(phase, detail=None, stats=None):
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
    state = StateManager()
    state.load()

    orgnr_list = state.pool_orgnr_list()
    total = len(orgnr_list)
    update_status("collect", f"Checking {total:,} monitored orgnr")

    session = make_session()
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

        rsc_payload = result if isinstance(result, str) else ""
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

        if checked % CHECKPOINT_EVERY == 0:
            update_status("collect", f"{checked:,}/{total:,}", state.changelog_summary())

    elapsed = time.time() - t0
    update_status("saving", f"Checked {checked:,} in {elapsed/60:.1f}m, {errors} errors")
    state.save()

    summary = state.changelog_summary()
    update_status("done", f"Daily complete: {summary}", summary)


# ═══════════════════════════════════════════════════════════════
# Weekly: full population scan
# ═══════════════════════════════════════════════════════════════

def load_all_orgnr():
    gz_path = f"losore/2026-03-21-agder/enhetsregisteret_alle.csv.gz"
    prefixes = [
        "losore/2026-03-21-telemark", "losore/2026-03-21-agder",
        "losore/2026-03-21-trondelag", "losore/2026-03-21-rogaland",
    ]

    from google.cloud import storage as gcs_lib
    client = gcs_lib.Client()
    bucket = client.bucket(BUCKET)

    if not os.path.exists(GZ_FILE):
        blob = bucket.blob("losore/2026-03-21-agder/enhetsregisteret_alle.csv.gz")
        if blob.exists():
            blob.download_to_filename(GZ_FILE)
            print(f"  Downloaded enhetsregisteret ({os.path.getsize(GZ_FILE)/1e6:.1f} MB)")

    orgnr_list = []
    with gzip.open(GZ_FILE, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            orgnr = row.get("organisasjonsnummer", "")
            if orgnr and len(orgnr) == 9:
                orgnr_list.append(orgnr)

    print(f"  Loaded {len(orgnr_list):,} orgnr from enhetsregisteret", flush=True)
    return orgnr_list


def run_weekly():
    state = StateManager()
    state.load()

    all_orgnr = load_all_orgnr()
    total = len(all_orgnr)
    update_status("collect", f"Scanning full population: {total:,} orgnr")

    session = make_session()
    checked = 0
    new_discoveries = 0
    errors = 0
    t0 = time.time()

    for i, orgnr in enumerate(all_orgnr):
        try:
            result = collect_one(orgnr, session)
        except Exception as e:
            errors += 1
            continue

        rsc_payload = result if isinstance(result, str) else ""
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
            eta_h = (total - checked) / rate / 3600
            print(f"  {checked:,}/{total:,}  ({rate:.1f}/s, ETA {eta_h:.1f}h)  "
                  f"new: {new_discoveries:,}  changes: {len(state._changelog):,}", flush=True)

        if checked % CHECKPOINT_EVERY == 0:
            update_status("collect", f"{checked:,}/{total:,} new:{new_discoveries}")

        if checked % SAVE_EVERY == 0:
            state.save()

    elapsed = time.time() - t0
    update_status("saving", f"Scanned {checked:,} in {elapsed/3600:.1f}h")
    state.save()

    summary = state.changelog_summary()
    update_status("done", f"Weekly complete: new={new_discoveries}, {summary}", summary)


# ═══════════════════════════════════════════════════════════════
# Bootstrap: build initial state from existing JSONL on GCS
# ═══════════════════════════════════════════════════════════════

def run_bootstrap():
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


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"{'='*60}", flush=True)
    print(f"  losore-pipeline CDC — mode: {RUN_MODE}", flush=True)
    print(f"  {datetime.now(timezone.utc).isoformat()}", flush=True)
    print(f"{'='*60}", flush=True)

    dispatch = {
        "daily": run_daily,
        "weekly": run_weekly,
        "bootstrap": run_bootstrap,
    }

    if RUN_MODE not in dispatch:
        print(f"Unknown RUN_MODE: {RUN_MODE}. Use: daily, weekly, bootstrap")
        sys.exit(1)

    dispatch[RUN_MODE]()


if __name__ == "__main__":
    main()
