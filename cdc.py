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
    """Return a cached google.cloud.storage.Client singleton."""
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client


def read_parquet_gcs(gcs_path, schema=None, columns=None):
    """Read a Parquet file from GCS into a PyArrow table.

    Parameters
    ----------
    gcs_path : str
        GCS object path relative to ``BUCKET``.
    schema : pa.Schema or None
        If the blob does not exist and schema is provided, returns
        an empty table with the given schema.  If None, returns None.
    columns : list[str] or None
        If provided, read only these columns.

    Returns
    -------
    pa.Table or None
    """
    bucket = gcs().bucket(BUCKET)
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        if schema:
            return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
        return None
    buf = blob.download_as_bytes()
    return pq.read_table(io.BytesIO(buf), columns=columns)


def write_parquet_gcs(table, gcs_path):
    """Write a PyArrow table to GCS as zstd-compressed Parquet.

    Parameters
    ----------
    table : pa.Table
        Data to write.
    gcs_path : str
        GCS object path relative to ``BUCKET``.
    """
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=3,
                   write_statistics=True, write_page_index=True)
    bucket = gcs().bucket(BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(buf.getvalue(), content_type="application/octet-stream")
    size_mb = buf.tell() / 1e6
    print(f"  wrote {gcs_path} ({size_mb:.1f} MB)", flush=True)


# ═══════════════════════════════════════════════════════════════
# Content hashing
# ═══════════════════════════════════════════════════════════════

def canonical_json(obj):
    """Serialise an object to deterministic JSON for hashing.

    Sorted keys, no whitespace, no ASCII escaping. Used by
    ``content_hash()`` to detect changes in rettsstiftelse documents.

    Parameters
    ----------
    obj : dict or list
        JSON-serialisable object.

    Returns
    -------
    str
    """
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def content_hash(obj):
    """SHA-256 hex digest of a rettsstiftelse document.

    Uses ``canonical_json()`` for deterministic serialisation so
    that two dicts with different key ordering but identical content
    produce the same hash.

    Parameters
    ----------
    obj : dict
        Rettsstiftelse document.

    Returns
    -------
    str
        64-character hex digest.
    """
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════════
# State manager — reads/writes pool + snapshots as Parquet
# ═══════════════════════════════════════════════════════════════

class StateManager:
    """Manages løsøreregisteret CDC state: monitoring pool + document snapshots.

    All state persisted as Parquet on GCS.  No databases, no migrations.
    Three data structures:

    - **Pool** (``pool.parquet``): orgnrs being monitored, with
      discovery date, last checked/changed timestamps, and source.
    - **Snapshots** (``snapshots.parquet``): per-document state keyed
      by ``dokumentnummer``, with content hash for change detection
      and full JSON for field-level diffing.
    - **Changelog**: append-only per-day Parquet files recording
      new and modified rettsstiftelser.
    """

    def __init__(self):
        """Initialise state manager with GCS paths and empty changelog."""
        self.pool_path = f"{STATE_PREFIX}/pool.parquet"
        self.snapshot_path = f"{STATE_PREFIX}/snapshots.parquet"
        self._pool = None
        self._pool_dict = None
        self._snapshots = None
        self._snapshot_index = None
        self._changelog = []
        self._now = datetime.now(timezone.utc)
        self._run_id = self._now.strftime("%Y%m%dT%H%M%S")

    def load(self, lightweight=False):
        """Load pool and snapshots from GCS Parquet into memory.

        Builds in-memory indices: ``_pool_dict`` for O(1) orgnr lookup,
        ``_snapshot_index`` for O(1) dokumentnummer lookup.
        Handles legacy files with extra columns (status, absences) by
        selecting only current schema columns.

        Parameters
        ----------
        lightweight : bool
            If True, skip loading ``full_json`` column from snapshots.
            Saves ~300 MB for large snapshot files.  Modifications
            detected during this session will lack field-level diffs.
        """
        self._lightweight = lightweight
        print("Loading state...", flush=True)
        pool_table = read_parquet_gcs(self.pool_path, POOL_SCHEMA)
        self._pool = pool_table.to_pydict()
        self._pool_dict = {
            self._pool["orgnr"][i]: i
            for i in range(len(self._pool["orgnr"]))
        }
        print(f"  pool: {len(self._pool['orgnr']):,} orgnr", flush=True)

        snap_cols = ["dokumentnummer", "orgnr", "content_hash", "first_seen", "last_seen"]
        if not lightweight:
            snap_cols.append("full_json")
        snap_table = read_parquet_gcs(self.snapshot_path, SNAPSHOT_SCHEMA, columns=snap_cols)
        want_cols = snap_cols
        have_cols = [c for c in want_cols if c in snap_table.column_names]
        snap_dict = snap_table.select(have_cols).to_pydict()
        self._snapshot_index = {}
        for i in range(len(snap_dict["dokumentnummer"])):
            dok = snap_dict["dokumentnummer"][i]
            self._snapshot_index[dok] = {
                "dokumentnummer": dok,
                "orgnr": snap_dict["orgnr"][i],
                "content_hash": snap_dict["content_hash"][i],
                "full_json": snap_dict["full_json"][i] if "full_json" in snap_dict else None,
                "first_seen": snap_dict["first_seen"][i],
                "last_seen": snap_dict["last_seen"][i],
            }
        mode_str = " (lightweight)" if lightweight else ""
        print(f"  snapshots: {len(self._snapshot_index):,} dokumentnummer{mode_str}", flush=True)

    def pool_orgnr_list(self):
        """Return all monitored orgnrs as a list."""
        return list(self._pool["orgnr"])

    def pool_size(self):
        """Return the number of monitored orgnrs."""
        return len(self._pool["orgnr"])

    def is_in_pool(self, orgnr):
        """Check whether an orgnr is in the monitoring pool."""
        return orgnr in self._pool_dict

    def known_docs_for_orgnr(self, orgnr):
        """Return all active snapshots for an orgnr.

        Returns
        -------
        dict[str, dict]
            Mapping from dokumentnummer to snapshot dict, filtered
            to ``status == "active"`` only.
        """
        return {
            k: v for k, v in self._snapshot_index.items()
            if v["orgnr"] == orgnr
        }

    # ─── CDC ────────────────────────────────────────────────────

    def diff_orgnr(self, orgnr, current_rs, source="daily"):
        """Compare current rettsstiftelser against stored snapshots for one orgnr.

        Two change categories:

        - **new**: dokumentnummer not in snapshot index.
        - **modified**: dokumentnummer exists but content hash differs.

        Parameters
        ----------
        orgnr : str
        current_rs : list[dict]
        source : str

        Returns
        -------
        list[dict]
            Changelog entries for this orgnr.
        """
        current_docs = {}
        for rs in current_rs:
            dok = rs.get("dokumentnummer")
            if dok:
                current_docs[dok] = rs

        changes = []

        for dok, rs in current_docs.items():
            h = content_hash(rs)
            if dok not in self._snapshot_index:
                changes.append(self._make_change("new", orgnr, dok, rs, source=source))
                self._upsert_snapshot(dok, orgnr, h, rs)
            elif self._snapshot_index[dok]["content_hash"] != h:
                old_json = self._snapshot_index[dok]["full_json"]
                if old_json:
                    old_rs = json.loads(old_json)
                    changed_fields = self._find_changed_fields(old_rs, rs)
                else:
                    old_rs = {}
                    changed_fields = None
                changes.append(self._make_change(
                    "modified", orgnr, dok, rs, old_rs=old_rs,
                    changed_fields=changed_fields, source=source
                ))
                self._upsert_snapshot(dok, orgnr, h, rs)
            else:
                self._snapshot_index[dok]["last_seen"] = self._now

        self._changelog.extend(changes)
        return changes

    def backfill_orgnr(self, orgnr, rettsstiftelser):
        """Add historical rettsstiftelser to snapshots without change detection.

        Used during bootstrap: all documents are inserted as new
        snapshots with ``source="backfill"`` changelog entries.  Skips
        documents already in the snapshot index (idempotent).

        Parameters
        ----------
        orgnr : str
        rettsstiftelser : list[dict]
        """
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
        """Add an orgnr to the monitoring pool.

        No-op if already in pool.  Sets ``discovered_date`` to today,
        ``last_checked`` to now, ``last_changed`` to now if ``n_rs > 0``.

        Parameters
        ----------
        orgnr : str
        n_rs : int
            Number of rettsstiftelser found.
        source : str
            Discovery source (``"weekly_discovery"``, ``"initial_scrape"``).
        region : str or None
            Geographic region label.
        """
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
        """Update last_checked and optionally last_changed for a pool entry.

        Parameters
        ----------
        orgnr : str
        n_rs : int
            Current rettsstiftelse count.
        had_changes : bool
            Whether any changelog entries were produced.
        """
        idx = self._pool_dict.get(orgnr)
        if idx is None:
            return
        self._pool["last_checked"][idx] = self._now
        self._pool["n_rettsstiftelser"][idx] = n_rs
        if had_changes:
            self._pool["last_changed"][idx] = self._now

    # ─── Save ───────────────────────────────────────────────────

    def save(self):
        """Write pool, snapshots, and changelog to GCS.

        Writes pool and snapshots as full Parquet overwrites.
        Changelog is written to ``{CHANGELOG_PREFIX}/{today}.parquet``
        (one file per day, not append — subsequent saves on the same
        day overwrite).
        """
        print("Saving state...", flush=True)

        pool_table = pa.table(self._pool, schema=POOL_SCHEMA).sort_by("orgnr")
        write_parquet_gcs(pool_table, self.pool_path)

        snap_rows = list(self._snapshot_index.values())
        if snap_rows:
            if getattr(self, '_lightweight', False):
                old = read_parquet_gcs(self.snapshot_path, columns=["dokumentnummer", "full_json"])
                if old and old.num_rows > 0:
                    old_d = old.to_pydict()
                    old_json = {old_d["dokumentnummer"][i]: old_d["full_json"][i] for i in range(old.num_rows)}
                    for snap in self._snapshot_index.values():
                        if snap["full_json"] is None:
                            snap["full_json"] = old_json.get(snap["dokumentnummer"])
                    del old, old_d, old_json
                    print(f"  merged full_json from old snapshots", flush=True)
            snap_table = pa.table(
                {col: [r[col] for r in snap_rows] for col in SNAPSHOT_SCHEMA.names},
                schema=SNAPSHOT_SCHEMA
            ).sort_by("orgnr")
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
        """Return a Counter of change_type values in the current changelog."""
        from collections import Counter
        types = Counter(c["change_type"] for c in self._changelog)
        return dict(types)

    # ─── Internal ───────────────────────────────────────────────

    def _upsert_snapshot(self, dok, orgnr, h, rs):
        """Insert or update a snapshot in the in-memory index."""
        existing = self._snapshot_index.get(dok)
        self._snapshot_index[dok] = {
            "dokumentnummer": dok,
            "orgnr": orgnr,
            "content_hash": h,
            "full_json": canonical_json(rs),
            "first_seen": existing["first_seen"] if existing else self._now,
            "last_seen": self._now,
        }

    def _make_change(self, change_type, orgnr, dok, rs=None, old_rs=None,
                     changed_fields=None, valid_time=None, source="daily"):
        """Create a changelog entry dict conforming to CHANGELOG_SCHEMA."""
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
        """Recursively compare two dicts, returning list of changed field paths.

        Uses dot-notation for nested paths (e.g.
        ``"roller.0.rolleinnehaver.navn"``).  Lists are compared via
        canonical JSON serialisation.

        Parameters
        ----------
        old, new : dict
        prefix : str
            Current path prefix for recursion.

        Returns
        -------
        list[str]
            Dot-separated field paths that differ.
        """
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
    """Build initial pool and snapshots from existing raw_responses.jsonl files.

    Walks ``jsonl_dir`` recursively for .jsonl files, extracts
    rettsstiftelser from each record's ``rsc_payload`` via
    ``extract_rettsstiftelser()``, adds orgnrs to pool and documents
    to snapshots.  Writes changelog entries with ``source="backfill"``.

    Parameters
    ----------
    jsonl_dir : str
        Local directory containing region-subdirectories with
        ``raw_responses.jsonl`` files from the pipeline.py scrape.
    """
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
