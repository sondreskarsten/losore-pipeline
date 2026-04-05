"""GCS storage operations for the løsøre collector.

Manages two GCS path hierarchies under a configurable prefix
(default ``losore/``)::

    gs://{bucket}/{prefix}/
    ├── raw/{date}/{orgnr}.json
    └── manifest/{date}.jsonl

Raw JSON files store the full collect_one() response per orgnr.
Manifests store per-orgnr metadata (content hash, count, status)
enabling O(1) provenance lookups without reading raw files.

Uploads are batched via a local buffer directory and flushed to
GCS using threaded parallel uploads at checkpoint intervals.

Typical usage::

    store = GCSStore("sondre_brreg_data", "losore")
    store.buffer_raw("811141402", "2026-04-05", response_dict)
    store.flush()  # uploads buffered files + writes manifest

"""

import os
import json
import hashlib
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage as gcs_lib


BUFFER_DIR = "/tmp/losore_raw"


class GCSStore:
    """GCS client for the løsøre collector.

    Args:
        bucket_name: GCS bucket name.
        prefix: Path prefix within bucket.  Default ``"losore"``.
    """

    def __init__(self, bucket_name, prefix="losore"):
        self._client = gcs_lib.Client()
        self.bucket = self._client.bucket(bucket_name)
        self.prefix = prefix.rstrip("/")
        self._manifest_buffer = []
        self._raw_buffer = []
        self._current_date = None

    def raw_path(self, orgnr, date_str):
        return f"{self.prefix}/raw/{date_str}/{orgnr}.json"

    def manifest_path(self, date_str):
        return f"{self.prefix}/manifest/{date_str}.jsonl"

    def buffer_raw(self, orgnr, date_str, response_dict):
        """Buffer a raw response for batch upload.

        Parameters
        ----------
        orgnr : str
        date_str : str
            Collection date (YYYY-MM-DD).
        response_dict : dict
            Full collect_one() response.
        """
        self._current_date = date_str
        raw_json = json.dumps(response_dict, ensure_ascii=False, default=str)

        local_dir = os.path.join(BUFFER_DIR, date_str)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, f"{orgnr}.json")
        with open(local_path, "w") as f:
            f.write(raw_json)
        self._raw_buffer.append((orgnr, date_str, local_path))

        rsc_payload = response_dict.get("rsc_payload", "")
        h = hashlib.sha256((rsc_payload or "").encode("utf-8")).hexdigest()[:16]
        n_rs = 0
        if rsc_payload and '"rettsstiftelser":[' in (rsc_payload or ""):
            n_rs = rsc_payload.count('"dokumentnummer"')

        self._manifest_buffer.append(json.dumps({
            "orgnr": orgnr,
            "content_hash": h,
            "n_rettsstiftelser": n_rs,
            "http_status": response_dict.get("http_status"),
            "method": response_dict.get("method"),
            "error": response_dict.get("error"),
            "collected_at": response_dict.get("collected_at"),
        }, ensure_ascii=False))

    def flush(self, max_workers=8):
        """Upload all buffered raw files and manifest to GCS.

        Uses ThreadPoolExecutor for parallel raw file uploads.
        Writes manifest as a single append operation.
        Clears buffers after upload.

        Parameters
        ----------
        max_workers : int
            Thread count for parallel uploads.
        """
        if not self._raw_buffer:
            return

        date_str = self._current_date
        n_files = len(self._raw_buffer)
        print(f"  uploading {n_files:,} raw files to {self.prefix}/raw/{date_str}/...", flush=True)

        uploaded = 0
        errors = 0

        def _upload_one(orgnr, ds, local_path):
            gcs_path = self.raw_path(orgnr, ds)
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path, content_type="application/json")
            os.remove(local_path)
            return True

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_upload_one, orgnr, ds, lp): orgnr
                for orgnr, ds, lp in self._raw_buffer
            }
            for future in as_completed(futures):
                try:
                    future.result()
                    uploaded += 1
                except Exception:
                    errors += 1

        print(f"  uploaded {uploaded:,} raw files ({errors} errors)", flush=True)

        if self._manifest_buffer:
            path = self.manifest_path(date_str)
            blob = self.bucket.blob(path)
            existing = ""
            if blob.exists():
                existing = blob.download_as_text()
                if existing and not existing.endswith("\n"):
                    existing += "\n"
            new_content = "\n".join(self._manifest_buffer) + "\n"
            blob.upload_from_string(existing + new_content, content_type="application/jsonl")
            print(f"  manifest: {path} ({len(self._manifest_buffer):,} entries)", flush=True)

        self._raw_buffer.clear()
        self._manifest_buffer.clear()

    def load_manifest(self, date_str):
        path = self.manifest_path(date_str)
        blob = self.bucket.blob(path)
        if blob.exists():
            return blob.download_as_text()
        return ""

    def known_orgnrs_from_manifest(self, date_str):
        text = self.load_manifest(date_str)
        orgnrs = set()
        for line in text.strip().split("\n"):
            if line.strip():
                orgnrs.add(json.loads(line)["orgnr"])
        return orgnrs

    def list_raw_orgnrs(self, date_str):
        prefix = f"{self.prefix}/raw/{date_str}/"
        orgnrs = set()
        for blob in self.bucket.list_blobs(prefix=prefix, fields="items(name),nextPageToken"):
            filename = blob.name.split("/")[-1]
            if filename.endswith(".json"):
                orgnrs.add(filename[:-5])
        return orgnrs

    def list_manifest_dates(self):
        prefix = f"{self.prefix}/manifest/"
        dates = []
        for blob in self.bucket.list_blobs(prefix=prefix, fields="items(name),nextPageToken"):
            filename = blob.name.split("/")[-1]
            if filename.endswith(".jsonl"):
                dates.append(filename[:-6])
        return sorted(dates)
