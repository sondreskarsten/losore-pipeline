# Løsøre CDC Pipeline — Implementation Plan

## Current architecture (what exists)

```
losore-pipeline/                          losore-analyse/
├── pipeline.py  (909 lines)             ├── R/00_config.R → 05_build_duckdb.R
│   collect_all() → per-region           │   entrypoint.R → Cloud Run
│   parse_all()   → parsed_data.json     │   JSONL → Parquet → DuckDB
│   export_all()  → rettsstiftelser.xlsx  └── Dockerfile (rocker/r-ver + PPM)
│   export()      → sparebanken_enhet.xlsx
└── Dockerfile (python:3.12-slim)
```

One-shot: scrape a region → parse → export. No state between runs.

## Target architecture (what we build)

```
losore-pipeline/                          losore-analyse/
├── pipeline.py    (unchanged)           ├── R/ (unchanged)
├── cdc.py         (new — all CDC logic) └── entrypoint.R reads from analytics/
├── entrypoint.py  (new — mode dispatch)
├── Dockerfile     (same base + pyarrow)
│
│  State on GCS (all Parquet, no databases):
│  losore/state/
│    pool.parquet              ← orgnr to check daily (~90K rows, ~2 MB)
│    snapshots.parquet         ← last known state per dokumentnummer (~250K rows, ~50 MB)
│  losore/changelog/
│    YYYY-MM-DD.parquet        ← daily change log (append-only)
│  losore/analytics/
│    current_rettsstiftelser.parquet  ← rebuilt after each run
```

## Three run modes, one Docker image

| Mode | Trigger | Scope | Cloud Run config |
|---|---|---|---|
| `daily` | 02:00 Europe/Oslo | All orgnr in `pool.parquet` (~90K+) | 1 vCPU, 1 GiB, 8h timeout |
| `weekly` | Saturday 00:00 | All ~1.1M orgnr from enhetsregisteret | 2 vCPU, 2 GiB, 24h timeout |
| `analyse` | After daily/weekly | R pipeline (existing losore-analyse) | 2 vCPU, 8 GiB, 2h timeout |

## State files — all Parquet, zero databases

### pool.parquet (~90K rows, grows over time)
```
orgnr            : string     (primary key)
region           : string     (fylke from enhetsregisteret)
discovered_date  : date       (when first seen with RS)
last_checked     : timestamp  (last scrape time)
last_changed     : timestamp  (last time any RS changed)
n_rettsstiftelser: int32      (count at last check)
source           : string     (initial_scrape | weekly_discovery)
```

### snapshots.parquet (~250K rows, one per dokumentnummer)
```
dokumentnummer   : string     (primary key)
orgnr            : string
content_hash     : string     (SHA-256 of canonical JSON)
full_json        : string     (complete rettsstiftelse as JSON)
first_seen       : timestamp
last_seen        : timestamp
status           : string     (active | disappeared)
absences         : int32      (consecutive scrapes not seen)
```

### changelog/YYYY-MM-DD.parquet (append-only per day)
```
orgnr            : string
dokumentnummer   : string
change_type      : string     (new | modified | disappeared | reappeared | backfill)
changed_fields   : string     (JSON array of dotted paths)
old_value        : string     (JSON of previous values)
new_value        : string     (JSON of new values)
valid_time       : timestamp  (innkomsttidspunkt — when brreg registered it)
detected_time    : timestamp  (when we found the change)
source           : string     (daily | weekly | backfill)
run_id           : string
```

## What changes in each repo

### losore-pipeline (Python scraper)
| File | Change |
|---|---|
| `pipeline.py` | No changes. Still used for ad-hoc regional scrapes. |
| `cdc.py` | **New.** State management + change detection. ~300 lines. |
| `entrypoint.py` | **New.** Mode dispatch (daily/weekly). Calls pipeline.collect_one + cdc. ~200 lines. |
| `Dockerfile` | Add `pyarrow` to requirements. |
| `requirements.txt` | **New.** `pyarrow`, `requests`, `google-cloud-storage`. |

### losore-analyse (R pipeline)
| File | Change |
|---|---|
| `R/01_parse_jsonl.R` | Add alternative: read from `analytics/current_rettsstiftelser.parquet` instead of JSONL. |
| `entrypoint.R` | Add `RUN_MODE=from_parquet` path that skips JSONL parsing. |

## Collection flow — reuses existing collect_one()

```python
# entrypoint.py — daily mode
pool = read_parquet("losore/state/pool.parquet")
snapshots = read_parquet("losore/state/snapshots.parquet")
snapshot_index = {row.dokumentnummer: row for row in snapshots}

session = requests.Session()  # F5 sticky session reuse (v7 pattern)

for orgnr in pool.orgnr:
    response = collect_one(orgnr, session)          # existing function
    rettsstiftelser = extract_rettsstiftelser(response)  # existing function
    changes = diff_orgnr(orgnr, rettsstiftelser, snapshot_index)
    changelog.extend(changes)
    update_snapshots(orgnr, rettsstiftelser, snapshot_index)
    update_pool_entry(pool, orgnr, len(rettsstiftelser))

write_parquet(snapshots, "losore/state/snapshots.parquet")
write_parquet(pool, "losore/state/pool.parquet")
write_parquet(changelog, f"losore/changelog/{today}.parquet")
```

## CDC logic — hash-first, no DeepDiff dependency

```python
def diff_orgnr(orgnr, current_rs, snapshot_index):
    changes = []
    current_docs = {rs["dokumentnummer"]: rs for rs in current_rs}
    known_docs = {k: v for k, v in snapshot_index.items() if v.orgnr == orgnr}

    # New documents
    for dok, rs in current_docs.items():
        if dok not in known_docs:
            changes.append(Change("new", orgnr, dok, rs))

    # Modified or unchanged
    for dok, rs in current_docs.items():
        if dok in known_docs:
            new_hash = content_hash(rs)
            if new_hash != known_docs[dok].content_hash:
                changes.append(Change("modified", orgnr, dok, rs, known_docs[dok]))

    # Disappeared
    for dok, snap in known_docs.items():
        if dok not in current_docs and snap.status == "active":
            changes.append(Change("disappeared", orgnr, dok))

    return changes
```

## Weekly discovery + backfill

```python
# entrypoint.py — weekly mode
all_orgnr = load_enhetsregisteret_orgnr()     # ~1.1M
pool = read_parquet("losore/state/pool.parquet")
pool_set = set(pool.orgnr)

for orgnr in all_orgnr:
    response = collect_one(orgnr, session)
    rs = extract_rettsstiftelser(response)

    if rs and orgnr not in pool_set:
        # New discovery — backfill all historical entries
        for doc in rs:
            changelog.append(Change("backfill", orgnr, doc["dokumentnummer"], doc,
                                    valid_time=doc["innkomsttidspunkt"]))
        pool.append(orgnr, discovered_date=today, source="weekly_discovery")
        pool_set.add(orgnr)

    if orgnr in pool_set:
        # Also run CDC for monitored orgnr
        changes = diff_orgnr(orgnr, rs, snapshot_index)
        changelog.extend(changes)
```

## Maskinporten transition (when ready)

Only `collect_one()` changes. Everything else (CDC, state, changelog, R analysis) stays identical.

```python
# Before (scraping)
def collect_one(orgnr, session):
    url = f"https://rettsstiftelser.brreg.no/nb/search/{orgnr}"
    resp = session.get(url, headers={"Rsc": "1"})
    return resp.content.decode("utf-8")

# After (Maskinporten API)
def collect_one(orgnr, session):
    url = f"https://losoreregisteret.brreg.no/registerinfo/api/v2/rettsstiftelse/orgnr/{orgnr}"
    resp = session.get(url, headers={"Authorization": f"Bearer {maskinporten_token()}"})
    return resp.json()
```

The `extract_rettsstiftelser()` function gets a second code path for the API JSON format (cleaner, no RSC parsing needed), but the output schema is identical.

## Cost estimate

| Component | Monthly |
|---|---|
| Cloud Run daily (90K × 0.3s = 7.5h × 30 days) | $0 (free tier) |
| Cloud Run weekly (1.1M × 0.3s = 92h × 4.3 weeks) | ~$2–4 |
| GCS storage (~5 GB state + changelog) | $0.10 |
| Cloud Scheduler (2 cron jobs) | $0 (free tier covers 3) |
| **Total** | **~$2–5/month** |
