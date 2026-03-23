import hashlib
import json
import os
import io
from datetime import datetime, timezone, date
from dataclasses import dataclass, field, asdict
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
STATE_PREFIX = os.environ.get("STATE_PREFIX", "losore/state")
CHANGELOG_PREFIX = os.environ.get("CHANGELOG_PREFIX", "losore/changelog")
ANALYTICS_PREFIX = os.environ.get("ANALYTICS_PREFIX", "losore/analytics")


# ═══════════════════════════════════════════════════════════════
# Schemas
# ═══════════════════════════════════════════════════════════════

POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("region", pa.string()),
    ("discovered_date", pa.date32()),
    ("last_checked", pa.timestamp("ms", tz="UTC")),
    ("last_changed", pa.timestamp("ms", tz="UTC")),
    ("n_rettsstiftelser", pa.int32()),
    ("source", pa.string()),
])

SNAPSHOT_SCHEMA = pa.schema([
    ("dokumentnummer", pa.string()),
    ("orgnr", pa.string()),
    ("content_hash", pa.string()),
    ("full_json", pa.large_string()),
    ("first_seen", pa.timestamp("ms", tz="UTC")),
    ("last_seen", pa.timestamp("ms", tz="UTC")),
    ("status", pa.string()),
    ("absences", pa.int32()),
])

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("dokumentnummer", pa.string()),
    ("change_type", pa.string()),
    ("changed_fields", pa.string()),
    ("old_value", pa.large_string()),
    ("new_value", pa.large_string()),
    ("valid_time", pa.timestamp("ms", tz="UTC")),
    ("detected_time", pa.timestamp("ms", tz="UTC")),
    ("source", pa.string()),
    ("run_id", pa.string()),
])


# ═══════════════════════════════════════════════════════════════
# GCS Parquet I/O
# ═══════════════════════════════════════════════════════════════

_gcs_client = None


def gcs():
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client


def read_parquet_gcs(gcs_path, schema=None):
    bucket = gcs().bucket(BUCKET)
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        if schema:
            return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
        return None
    buf = blob.download_as_bytes()
    return pq.read_table(io.BytesIO(buf))


def write_parquet_gcs(table, gcs_path):
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=3)
    bucket = gcs().bucket(BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    size_mb = buf.tell() / 1e6
    print(f"  wrote {gcs_path} ({size_mb:.1f} MB)", flush=True)


# ═══════════════════════════════════════════════════════════════
# Content hashing
# ═══════════════════════════════════════════════════════════════

def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def content_hash(obj):
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════════
# State manager — reads/writes pool + snapshots as Parquet
# ═══════════════════════════════════════════════════════════════

class StateManager:

    def __init__(self):
        self.pool_path = f"{STATE_PREFIX}/pool.parquet"
        self.snapshot_path = f"{STATE_PREFIX}/snapshots.parquet"
        self._pool = None
        self._pool_dict = None
        self._snapshots = None
        self._snapshot_index = None
        self._changelog = []
        self._now = datetime.now(timezone.utc)
        self._run_id = self._now.strftime("%Y%m%dT%H%M%S")

    def load(self):
        print("Loading state...", flush=True)
        pool_table = read_parquet_gcs(self.pool_path, POOL_SCHEMA)
        self._pool = pool_table.to_pydict()
        self._pool_dict = {
            self._pool["orgnr"][i]: i
            for i in range(len(self._pool["orgnr"]))
        }
        print(f"  pool: {len(self._pool['orgnr']):,} orgnr", flush=True)

        snap_table = read_parquet_gcs(self.snapshot_path, SNAPSHOT_SCHEMA)
        snap_dict = snap_table.to_pydict()
        self._snapshot_index = {}
        for i in range(len(snap_dict["dokumentnummer"])):
            dok = snap_dict["dokumentnummer"][i]
            self._snapshot_index[dok] = {
                "dokumentnummer": dok,
                "orgnr": snap_dict["orgnr"][i],
                "content_hash": snap_dict["content_hash"][i],
                "full_json": snap_dict["full_json"][i],
                "first_seen": snap_dict["first_seen"][i],
                "last_seen": snap_dict["last_seen"][i],
                "status": snap_dict["status"][i],
                "absences": snap_dict["absences"][i],
            }
        print(f"  snapshots: {len(self._snapshot_index):,} dokumentnummer", flush=True)

    def pool_orgnr_list(self):
        return list(self._pool["orgnr"])

    def pool_size(self):
        return len(self._pool["orgnr"])

    def is_in_pool(self, orgnr):
        return orgnr in self._pool_dict

    def known_docs_for_orgnr(self, orgnr):
        return {
            k: v for k, v in self._snapshot_index.items()
            if v["orgnr"] == orgnr and v["status"] == "active"
        }

    # ─── CDC ────────────────────────────────────────────────────

    def diff_orgnr(self, orgnr, current_rs, source="daily"):
        current_docs = {}
        for rs in current_rs:
            dok = rs.get("dokumentnummer")
            if dok:
                current_docs[dok] = rs

        known = self.known_docs_for_orgnr(orgnr)
        changes = []

        for dok, rs in current_docs.items():
            h = content_hash(rs)
            if dok not in self._snapshot_index:
                changes.append(self._make_change("new", orgnr, dok, rs, source=source))
                self._upsert_snapshot(dok, orgnr, h, rs)
            elif self._snapshot_index[dok]["content_hash"] != h:
                old_json = self._snapshot_index[dok]["full_json"]
                old_rs = json.loads(old_json) if old_json else {}
                changed_fields = self._find_changed_fields(old_rs, rs)
                changes.append(self._make_change(
                    "modified", orgnr, dok, rs, old_rs=old_rs,
                    changed_fields=changed_fields, source=source
                ))
                self._upsert_snapshot(dok, orgnr, h, rs)
            else:
                snap = self._snapshot_index[dok]
                snap["last_seen"] = self._now
                snap["absences"] = 0
                if snap["status"] == "disappeared":
                    snap["status"] = "active"
                    changes.append(self._make_change("reappeared", orgnr, dok, rs, source=source))

        for dok, snap in known.items():
            if dok not in current_docs:
                snap["absences"] = snap["absences"] + 1
                if snap["absences"] >= 3 and snap["status"] == "active":
                    snap["status"] = "disappeared"
                    changes.append(self._make_change("disappeared", orgnr, dok, source=source))

        self._changelog.extend(changes)
        return changes

    def backfill_orgnr(self, orgnr, rettsstiftelser):
        for rs in rettsstiftelser:
            dok = rs.get("dokumentnummer")
            if not dok or dok in self._snapshot_index:
                continue
            h = content_hash(rs)
            self._upsert_snapshot(dok, orgnr, h, rs)
            self._changelog.append(self._make_change(
                "backfill", orgnr, dok, rs,
                valid_time=rs.get("innkomsttidspunkt"),
                source="backfill"
            ))

    def add_to_pool(self, orgnr, n_rs, source="weekly_discovery", region=None):
        if self.is_in_pool(orgnr):
            return
        idx = len(self._pool["orgnr"])
        self._pool["orgnr"].append(orgnr)
        self._pool["region"].append(region)
        self._pool["discovered_date"].append(date.today())
        self._pool["last_checked"].append(self._now)
        self._pool["last_changed"].append(self._now if n_rs > 0 else None)
        self._pool["n_rettsstiftelser"].append(n_rs)
        self._pool["source"].append(source)
        self._pool_dict[orgnr] = idx

    def update_pool_entry(self, orgnr, n_rs, had_changes):
        idx = self._pool_dict.get(orgnr)
        if idx is None:
            return
        self._pool["last_checked"][idx] = self._now
        self._pool["n_rettsstiftelser"][idx] = n_rs
        if had_changes:
            self._pool["last_changed"][idx] = self._now

    # ─── Save ───────────────────────────────────────────────────

    def save(self):
        print("Saving state...", flush=True)

        pool_table = pa.table(self._pool, schema=POOL_SCHEMA)
        write_parquet_gcs(pool_table, self.pool_path)

        snap_rows = list(self._snapshot_index.values())
        if snap_rows:
            snap_table = pa.table(
                {col: [r[col] for r in snap_rows] for col in SNAPSHOT_SCHEMA.names},
                schema=SNAPSHOT_SCHEMA
            )
            write_parquet_gcs(snap_table, self.snapshot_path)

        if self._changelog:
            cl_table = pa.table(
                {col: [r[col] for r in self._changelog] for col in CHANGELOG_SCHEMA.names},
                schema=CHANGELOG_SCHEMA
            )
            today_str = date.today().isoformat()
            write_parquet_gcs(cl_table, f"{CHANGELOG_PREFIX}/{today_str}.parquet")
            print(f"  changelog: {len(self._changelog):,} changes", flush=True)

        print(f"  pool: {self.pool_size():,} orgnr", flush=True)
        print(f"  snapshots: {len(self._snapshot_index):,} dokumentnummer", flush=True)

    def changelog_summary(self):
        from collections import Counter
        types = Counter(c["change_type"] for c in self._changelog)
        return dict(types)

    # ─── Internal ───────────────────────────────────────────────

    def _upsert_snapshot(self, dok, orgnr, h, rs):
        existing = self._snapshot_index.get(dok)
        self._snapshot_index[dok] = {
            "dokumentnummer": dok,
            "orgnr": orgnr,
            "content_hash": h,
            "full_json": canonical_json(rs),
            "first_seen": existing["first_seen"] if existing else self._now,
            "last_seen": self._now,
            "status": "active",
            "absences": 0,
        }

    def _make_change(self, change_type, orgnr, dok, rs=None, old_rs=None,
                     changed_fields=None, valid_time=None, source="daily"):
        vt = None
        if valid_time:
            try:
                vt = datetime.fromisoformat(valid_time.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass
        elif rs:
            try:
                vt = datetime.fromisoformat(
                    rs.get("innkomsttidspunkt", "").replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        return {
            "orgnr": orgnr,
            "dokumentnummer": dok,
            "change_type": change_type,
            "changed_fields": json.dumps(changed_fields or []),
            "old_value": canonical_json(old_rs) if old_rs else None,
            "new_value": canonical_json(rs) if rs else None,
            "valid_time": vt,
            "detected_time": self._now,
            "source": source,
            "run_id": self._run_id,
        }

    def _find_changed_fields(self, old, new, prefix=""):
        changed = []
        all_keys = set(list(old.keys()) + list(new.keys()))
        for key in sorted(all_keys):
            path = f"{prefix}.{key}" if prefix else key
            old_val = old.get(key)
            new_val = new.get(key)
            if isinstance(old_val, dict) and isinstance(new_val, dict):
                changed.extend(self._find_changed_fields(old_val, new_val, path))
            elif isinstance(old_val, list) and isinstance(new_val, list):
                if canonical_json(old_val) != canonical_json(new_val):
                    changed.append(path)
            elif old_val != new_val:
                changed.append(path)
        return changed


# ═══════════════════════════════════════════════════════════════
# Bootstrap — build initial pool + snapshots from existing JSONL
# ═══════════════════════════════════════════════════════════════

def bootstrap_from_jsonl(jsonl_dir):
    import re
    from pipeline import extract_rettsstiftelser

    state = StateManager()
    state.load()

    jsonl_files = []
    for root, dirs, files in os.walk(jsonl_dir):
        for f in files:
            if f.endswith(".jsonl"):
                jsonl_files.append(os.path.join(root, f))

    print(f"Bootstrapping from {len(jsonl_files)} JSONL files...", flush=True)

    for jf in sorted(jsonl_files):
        region = os.path.basename(os.path.dirname(jf))
        count = 0
        skipped = 0
        with open(jf) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                orgnr = rec.get("orgnr", "")
                payload = rec.get("rsc_payload", "")
                if not payload:
                    continue
                try:
                    rs_list = extract_rettsstiftelser(payload)
                except (json.JSONDecodeError, Exception):
                    skipped += 1
                    continue
                if not rs_list:
                    continue

                state.add_to_pool(orgnr, len(rs_list), source="initial_scrape", region=region)
                state.backfill_orgnr(orgnr, rs_list)
                count += 1
        msg = f"  {region}: {count:,} orgnr with RS"
        if skipped:
            msg += f" ({skipped} skipped)"
        print(msg, flush=True)

    state.save()
    summary = state.changelog_summary()
    print(f"\nBootstrap complete: {summary}", flush=True)
