# losore-pipeline

Scrapes the Norwegian Register of Mortgages and Liens (Løsøreregisteret) for every company in the Enhetsregisteret. Tracks changes daily via CDC. Produces the **bank relationship map**: which company has liens, held by which creditor, for how much.

## Source

Løsøreregisteret records all security interests (pant) in moveable assets registered against Norwegian companies. The data is served via a Blazor Server Component (RSC) web application — no API, no bulk download. This pipeline reverse-engineers the RSC push protocol to extract structured JSON from the web app.

## LUAS

**(orgnr, dokumentnummer)**. One row in `snapshots.parquet` = one rettsstiftelse (registered security interest) for one company.

## Snapshots schema

| Column | Type | Description |
|---|---|---|
| `dokumentnummer` | string | Unique lien registration number |
| `orgnr` | string | Company bearing the lien (the debtor) |
| `content_hash` | string | SHA256 of full_json for change detection |
| `full_json` | large_string | Complete rettsstiftelse JSON (see structure below) |
| `first_seen` | timestamp | When first observed |
| `last_seen` | timestamp | When last confirmed present |

## Inside `full_json`

The JSON contains the full rettsstiftelse as returned by the Blazor app. Key paths:

```
$.roller[?(@.rollegruppetype=='rollegruppe.rett')].rolleinnehaver.navn          → creditor name (e.g., "DNB BANK ASA")
$.roller[?(@.rollegruppetype=='rollegruppe.rett')].rolleinnehaver.organisasjonsnummer → creditor orgnr (e.g., "984851006")
$.roller[?(@.rollegruppetype=='rollegruppe.forp')].rolleinnehaver.organisasjonsnummer → debtor orgnr
$.krav.belop[0].belop                                                           → secured amount (NOK, integer)
$.krav.belop[0].valuta                                                          → currency (almost always "NOK")
$.formuesgoder[].type                                                           → asset type code
$.formuesgoder[].typeBeskrivelse                                                → asset type description
$.innkomsttidspunkt                                                             → registration date
$.status                                                                        → "statusregistreringsobjekt.tl" (tinglyst = active)
$.paategninger[]                                                                → annotation strings (older entries have "Panthaver: ..." here)
```

**Gotcha**: older rettsstiftelser (pre-~2010) don't have structured `roller` arrays. The creditor name is buried in the `paategninger` (annotations) array as free text like `"Panthaver: Den norske Bank A/S."`. To get complete bank coverage, you must check both `roller[rollegruppe.rett]` AND parse paategninger text.

**Gotcha**: `belop` is the maximum secured claim, not the outstanding balance. A 50M NOK floating charge from 2005 may secure a loan that's been paid down to 5M. This is a ceiling, not a measurement.

## Bank segmentation

To classify a company by bank relationship:

```sql
-- Extract creditor from roller JSON
SELECT orgnr, dokumentnummer,
  json_extract_string(rolle, '$.rolleinnehaver.organisasjonsnummer') AS bank_orgnr,
  json_extract_string(rolle, '$.rolleinnehaver.navn') AS bank_name,
  json_extract_string(full_json, '$.krav.belop[0].belop')::BIGINT AS belop_nok
FROM snapshots,
  LATERAL unnest(from_json(json_extract(full_json, '$.roller'), '["JSON"]')) AS rolle
WHERE json_extract_string(rolle, '$.rollegruppetype') = 'rollegruppe.rett'
```

DNB group orgnrs: `984851006` (DNB Bank ASA), `816521432` (DNB Finans), `920953743` (DNB NOR Finans), `858043042` (DNB NOR Finans Bilfinans), `985621551` (DNB Boligkreditt), `914782007` (DNB Livsforsikring).

## Cardinality

- **Pool**: ~320K orgnrs monitored
- **Snapshots**: ~324K active rettsstiftelser
- **Top creditor**: DNB Bank ASA — 64,668 liens across 22,797 distinct orgnrs

## CDC (Pattern B)

Mutable snapshots — `snapshots.parquet` is overwritten each run. Changelog embeds old/new JSON in `details_json`, making it self-contained (no need to diff snapshots).

## GCS layout

```
gs://sondre_brreg_data/losore/
├── state/pool.parquet              orgnr universe (mutable)
├── state/snapshots.parquet         current state (mutable)
├── changelog/{date}.parquet        daily CDC events
└── dimensions/                     lookup tables
```

## Cloud Run

- **Job**: `losore-cdc` (region: `europe-west4`)
- **Schedule**: 02:00 Mon-Fri (daily), 00:00 Sat (weekly full scan)
- **Runtime**: daily ~2-4h (0.05s/orgnr × 320K), weekly ~6-8h

## Downstream

→ fleet_panel: `bank_segment` column (DNB only / DNB + other / Other bank / No lien)
→ portfolio analysis: DNB lien exposure per vessel, per length group, per gear type
