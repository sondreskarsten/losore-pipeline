# Deprecation notice

This repository was split on 2026-05-05 into two separate repos to enable
independent scheduling of the collect and parse halves on AWS:

- **`sondreskarsten/losore-collector`** — fetches RSC payloads from
  `rettsstiftelser.brreg.no` per orgnr and writes raw JSON to
  `gs://sondre_brreg_data/losore/raw/{date}/{orgnr}.json`. No CDC.

- **`sondreskarsten/losore-parser`** — reads those raw JSONs, calls
  `extract_rettsstiftelser()`, runs `StateManager.diff_orgnr()`, appends to
  `gs://sondre_brreg_data/losore/changelog/{date}.parquet`. No HTTP.

`pipeline.py`, `storage.py`, `cdc.py`, `Dockerfile`, and `requirements.txt`
are byte-identical across all three repos. Only the `entrypoint.py` files
differ: the original `entrypoint.py` here interleaved both halves; the two
new ones each invoke only their respective half.

## Verified round-trip on split day

5-orgnr sample (`810034882, 810182482, 810324562, 810363142, 810392312`)
run end-to-end through both new repos against an isolated test prefix:
- Collector wrote 5 raw JSON files
- Parser read those raw files and produced 16 CDC `new` events
- Per-orgnr counts (5, 4, 3, 1, 3) matched the collector manifest exactly

## When this repo will be archived

Once both new Cloud Run Jobs are deployed and running on the daily schedule
(or replaced by ECS Fargate task definitions on AWS post-migration), this repo
should be renamed `DEPRECATED-losore-pipeline` per the project convention.

The `entrypoint.py` here continues to work in the meantime — the split is
purely additive.
