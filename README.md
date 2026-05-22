# losore-pipeline

Scrapes the Norwegian Register of Mortgages and Liens (Løsøreregisteret) for every company in the Enhetsregisteret, tracks changes daily, and produces a CDC changelog. This is how we know **which bank holds security interests in which company's assets**.

## What is Løsøreregisteret?

When a Norwegian company pledges assets as loan collateral — vehicles, inventory, accounts receivable, machinery, or floating charges over all assets — the security interest is registered in Løsøreregisteret (administered by Brønnøysund). It is a public register: anyone can look up any company and see every registered lien, who holds it, and for how much.

Each entry (rettsstiftelse) contains:
- **The debtor** (pantsetter): the company whose assets are pledged
- **The creditor** (rettighetshaver): the bank or leasing company holding the lien
- **The amount** (krav.belop): the maximum secured claim
- **The asset type** (formuesgodetype): what's pledged — vehicles, inventory, receivables, floating charge
- **Registration date** (innkomsttidspunkt): when the lien was filed
- **Status**: tinglyst (registered), slettet (cancelled), etc.
- **Annotations** (påtegninger): free-text notes, often identifying the creditor for older entries

## Why does a credit analyst care?

This is the **bank relationship map**. From Løsøreregisteret you can answer:

- **Which bank is the primary lender to this company?** The rettighetshaver with the largest belop (or the floating charge) is the main bank.
- **Does the company bank with us (DNB) or a competitor?** Filter rettighetshaver by `orgnr = '984851006'` (DNB Bank ASA).
- **Has the company recently taken on new secured debt?** A new rettsstiftelse = new lien = new borrowing. Visible from the CDC changelog as a `new` event.
- **Has a lien been cancelled?** A `disappeared` event on a rettsstiftelse means the lien was slettet — possibly because the loan was repaid, refinanced, or the asset was sold.
- **What's the total secured exposure?** Sum of belop across all active rettsstiftelser per orgnr gives the maximum secured claim. This is a floor on the company's total debt (unsecured debt is not in this register).
- **Portfolio segmentation**: by joining rettighetshaver.orgnr to the fleet panel, we segment fishing vessels into "DNB customer", "other bank", and "no lien" — revealing market share, exposure concentration, and relative portfolio quality.

## How collection works

Løsøreregisteret does not have a bulk download or a proper API. The data is served via a Blazor Server Component (RSC) web application that uses WebSocket-like push messages. This pipeline:

1. **Establishes an RSC session** with the Løsøreregisteret web app
2. **Sends search requests** per orgnr (one at a time, 0.05s delay)
3. **Parses the RSC push payload** to extract rettsstiftelser JSON
4. **Diffs against stored state** (Pattern B: previous state in mutable snapshots.parquet)
5. **Writes changelog** for any changes (new, modified, disappeared rettsstiftelser)

Three run modes:
- **daily**: re-check all known orgnrs (from pool.parquet, ~320K orgnrs)
- **weekly**: full population scan — download entire Enhetsregisteret CSV, filter eligible org forms, discover new orgnrs not yet in pool
- **bootstrap**: one-time initial load from raw regional JSONL dumps

## CDC shape (Pattern B)

Unlike the bulk-diff parsers (enheter, roller) where changelogs are indexes into dated snapshots, the løsøre changelog embeds the actual JSON content. Each changelog row contains the full rettsstiftelse JSON in `details_json`, making it self-contained.

## GCS layout

```
gs://sondre_brreg_data/losore/
├── state/
│   ├── pool.parquet              orgnr universe (~320K, mutable)
│   └── snapshots.parquet         current state per (orgnr, dokumentnummer), mutable
├── changelog/{date}.parquet      daily CDC events
├── raw/                          regional JSONL dumps (bootstrap input)
└── dimensions/                   lookup tables (formuesgodetype codes, etc.)
```

## Cloud Run

- **Job**: `losore-cdc` (region: `europe-west4`)
- **Schedule**: 02:00 Mon-Fri, 00:00 Saturday (weekly full scan)
- **Runtime**: daily ~2-4h (320K orgnrs × 0.05s); weekly ~6-8h (481K orgnrs)

## Downstream consumers

→ **fleet_panel**: rettighetshaver orgnr classifies vessels into bank segments (DNB / other / unknown)
→ **integration-layer**: pending ledger admission (changelog schema compatible but adapter not yet wired)
→ **portfolio monitor**: tracks new/cancelled liens as credit signals
