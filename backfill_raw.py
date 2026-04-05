"""One-off backfill: convert bootstrap JSONL into per-orgnr raw/ + manifest.

Reads losore/2026-03-21-*/raw_responses.jsonl from GCS,
writes losore/raw/2026-03-21/{orgnr}.json + losore/manifest/2026-03-21.jsonl.

Run as: python backfill_raw.py
"""

import json
import os
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

BUCKET_NAME = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
DATE = "2026-03-21"
RAW_PREFIX = f"losore/raw/{DATE}"
MANIFEST_PATH = f"losore/manifest/{DATE}.jsonl"
LOCAL_DIR = "/tmp/losore_backfill"
WORKERS = 32


def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = sorted(
        [b for b in bucket.list_blobs(prefix="losore/2026-03-21-")
         if b.name.endswith("raw_responses.jsonl")],
        key=lambda x: x.size
    )
    print(f"Found {len(blobs)} regional JSONL files", flush=True)

    os.makedirs(LOCAL_DIR, exist_ok=True)
    manifest_lines = []
    total_uploaded = 0
    t0 = time.time()

    def upload_one(orgnr, local_path):
        gcs_path = f"{RAW_PREFIX}/{orgnr}.json"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path, content_type="application/json")
        os.remove(local_path)

    for blob_obj in blobs:
        region = blob_obj.name.split("/")[1].replace("2026-03-21-", "")
        print(f"\n=== {region} ({blob_obj.size/1e6:.0f} MB) ===", flush=True)

        local_jsonl = f"/tmp/{region}.jsonl"
        blob_obj.download_to_filename(local_jsonl)
        print(f"  downloaded", flush=True)

        pending = []
        with open(local_jsonl) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                orgnr = rec.get("orgnr", "")
                if not orgnr:
                    continue

                local_path = os.path.join(LOCAL_DIR, f"{orgnr}.json")
                with open(local_path, "w") as f:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str))
                pending.append((orgnr, local_path))

                rsc_payload = rec.get("rsc_payload", "") or ""
                h = hashlib.sha256(rsc_payload.encode("utf-8")).hexdigest()[:16]
                n_rs = rsc_payload.count('"dokumentnummer"') if rsc_payload else 0
                manifest_lines.append(json.dumps({
                    "orgnr": orgnr,
                    "content_hash": h,
                    "n_rettsstiftelser": n_rs,
                    "http_status": rec.get("http_status"),
                    "method": rec.get("method"),
                    "error": rec.get("error"),
                    "collected_at": rec.get("collected_at"),
                }, ensure_ascii=False))

        os.remove(local_jsonl)

        uploaded = 0
        errors = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(upload_one, o, p): o for o, p in pending}
            for future in as_completed(futures):
                try:
                    future.result()
                    uploaded += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  upload error: {e}", flush=True)

        total_uploaded += uploaded
        elapsed = time.time() - t0
        print(f"  {len(pending):,} orgnr → {uploaded:,} uploaded ({errors} errors) "
              f"[{elapsed/60:.0f}m elapsed, {total_uploaded:,} total]", flush=True)

    print(f"\n=== Writing manifest ({len(manifest_lines):,} entries) ===", flush=True)
    manifest_blob = bucket.blob(MANIFEST_PATH)
    manifest_blob.upload_from_string(
        "\n".join(manifest_lines) + "\n",
        content_type="application/jsonl"
    )

    elapsed = time.time() - t0
    print(f"\nDone: {total_uploaded:,} files + manifest in {elapsed/60:.0f}m")


if __name__ == "__main__":
    main()
