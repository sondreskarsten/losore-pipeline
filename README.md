# Løsøreregisteret Pipeline

Cloud Run Job that scrapes the Norwegian chattel registry (løsøreregisteret) for businesses in a given set of kommuner, extracts structured data from the Next.js RSC stream, and exports filtered xlsx files to GCS.

## Architecture

1. **Download** full BRREG enhetsregisteret CSV (~150MB gz)
2. **Filter** by kommunenummer to get target orgnr
3. **Scrape** løsøreregisteret per orgnr — extracts structured JSON from Next.js RSC payload (no HTML parsing)
4. **Checkpoint** every 100 entries to GCS — resumable on restart
5. **Export** filtered xlsx with enhetsregisteret enrichment to GCS

All config via env vars. Raw `rettsstiftelser` JSON preserved in checkpoint for re-processing.

## Config

| Env var | Default | Description |
|---------|---------|-------------|
| `GCS_BUCKET` | `sondre_brreg_data` | GCS bucket |
| `GCS_PREFIX` | `losore/agder` | Path prefix in bucket |
| `KOMMUNENUMMER` | Agder (42xx) | Comma-separated kommunenummer |
| `SEARCH_TERMS` | `SPAREBANKEN NORGE,SPAREBANKEN SØR` | Panthaver filter |
| `SCRAPE_DELAY` | `0.1` | Seconds between requests |
| `SAVE_EVERY` | `100` | Checkpoint interval |

## Deploy

```bash
./deploy.sh
```

Override for other regions:
```bash
GCS_PREFIX=losore/telemark \
KOMMUNENUMMER=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036 \
./deploy.sh
```

## Poll status

```bash
./poll.sh
# or
gsutil cat gs://sondre_brreg_data/losore/agder/status.json | python3 -m json.tool
```

## GCS outputs

```
gs://{bucket}/{prefix}/status.json            # Job status
gs://{bucket}/{prefix}/scraped_data.json      # Full checkpoint (raw + parsed)
gs://{bucket}/{prefix}/output/sparebanken_enhet.xlsx  # Filtered export
gs://{bucket}/{prefix}/enhetsregisteret_alle.csv.gz   # Cached bulk download
```
