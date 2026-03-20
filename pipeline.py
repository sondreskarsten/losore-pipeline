import os
import re
import csv
import json
import gzip
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from google.cloud import storage


BUCKET_NAME = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "losore/agder")

KOMMUNENUMMER = os.environ.get("KOMMUNENUMMER", ",".join([
    "4201", "4202", "4203", "4204", "4205", "4206", "4207",
    "4211", "4212", "4213", "4214", "4215", "4216", "4217",
    "4218", "4219", "4220", "4221", "4222", "4223", "4224",
    "4225", "4226", "4227", "4228",
])).split(",")

SEARCH_TERMS = os.environ.get("SEARCH_TERMS", "SPAREBANKEN NORGE,SPAREBANKEN SØR").split(",")

RELEVANT_CATEGORIES = [
    "Pant i driftstilbehør", "Pant i varelager", "Pant i landbruksredskaper",
    "Pant i fiskeredskaper", "Pant i fordringer (factoring)",
    "Pant i motorvogner, anleggsmaskiner og jernbanemateriell",
]

DELAY = float(os.environ.get("SCRAPE_DELAY", "0.1"))
SAVE_EVERY = int(os.environ.get("SAVE_EVERY", "100"))

GZ_FILE = "/tmp/enhetsregisteret_alle.csv.gz"
RAW_FILE = "/tmp/raw_responses.json"
PARSED_FILE = "/tmp/parsed_data.json"


# ═══════════════════════════════════════════════════════════════
# GCS helpers
# ═══════════════════════════════════════════════════════════════

def gcs_client():
    return storage.Client()


def gcs_upload(local_path, gcs_path):
    client = gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)


def gcs_download(gcs_path, local_path):
    client = gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    if blob.exists():
        blob.download_to_filename(local_path)
        return True
    return False


def gcs_upload_json(data, gcs_path):
    client = gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")


def update_status(phase, progress=None, detail=None, error=None):
    status = {
        "phase": phase,
        "progress": progress,
        "detail": detail,
        "error": error,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "bucket": BUCKET_NAME,
            "prefix": GCS_PREFIX,
            "kommunenummer": KOMMUNENUMMER,
            "search_terms": SEARCH_TERMS,
        },
    }
    gcs_upload_json(status, f"{GCS_PREFIX}/status.json")
    print(f"[{phase}] {progress or ''} {detail or ''}", flush=True)


# ═══════════════════════════════════════════════════════════════
# Stage 0: Download enhetsregisteret
# ═══════════════════════════════════════════════════════════════

def download_enhetsregisteret():
    if os.path.exists(GZ_FILE):
        update_status("download", detail="Using cached local file")
        return
    if gcs_download(f"{GCS_PREFIX}/enhetsregisteret_alle.csv.gz", GZ_FILE):
        update_status("download", detail="Restored from GCS cache")
        return
    url = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv"
    update_status("download", detail=f"Downloading from {url}")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    downloaded = 0
    with open(GZ_FILE, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
    gcs_upload(GZ_FILE, f"{GCS_PREFIX}/enhetsregisteret_alle.csv.gz")
    update_status("download", detail=f"Downloaded {downloaded / 1e6:.1f} MB, cached to GCS")


# ═══════════════════════════════════════════════════════════════
# Stage 1: COLLECT
#
# HTTP GET per orgnr. Store raw response with zero parsing.
# Every orgnr gets a record regardless of outcome.
#
# Record schema:
#   orgnr            str
#   url              str
#   collected_at     ISO timestamp
#   http_status      int | null (null on connection failure)
#   rsc_payloads     list[str]  decoded RSC push strings
#   rsc_payloads_raw list[str]  undecoded payloads (decode failed)
#   error            str | null
# ═══════════════════════════════════════════════════════════════

def collect_one(orgnr):
    url = f"https://rettsstiftelser.brreg.no/nb/oppslag/virksomhet/{orgnr}"
    record = {
        "orgnr": orgnr,
        "url": url,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "http_status": None,
        "rsc_payloads": [],
        "rsc_payloads_raw": [],
        "error": None,
    }
    try:
        resp = requests.get(url, timeout=30)
        record["http_status"] = resp.status_code
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        record["error"] = "timeout"
        return record
    except requests.exceptions.ConnectionError as e:
        record["error"] = f"connection: {str(e)[:200]}"
        return record
    except requests.exceptions.HTTPError:
        record["error"] = f"http_{resp.status_code}"
        return record

    pushes = re.findall(
        r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', resp.text, re.DOTALL
    )

    for payload in pushes:
        if "rettsstiftelser" not in payload:
            continue
        try:
            decoded = json.loads('"' + payload + '"')
            record["rsc_payloads"].append(decoded)
        except (json.JSONDecodeError, ValueError):
            record["rsc_payloads_raw"].append(payload)

    return record


def collect_all():
    checkpoint_gcs = f"{GCS_PREFIX}/raw_responses.json"
    if gcs_download(checkpoint_gcs, RAW_FILE):
        with open(RAW_FILE, "r") as f:
            raw_data = json.load(f)
        update_status("collect", detail=f"Restored checkpoint: {len(raw_data)} orgnr")
    else:
        raw_data = {}

    target_orgnr = set()
    with gzip.open(GZ_FILE, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("forretningsadresse.kommunenummer") in KOMMUNENUMMER:
                target_orgnr.add(row["organisasjonsnummer"])

    already = set(raw_data.keys())
    remaining = sorted(target_orgnr - already)
    total_target = len(target_orgnr)
    update_status("collect", progress=f"{len(already)}/{total_target}",
                  detail=f"Remaining: {len(remaining)}")

    for i, orgnr in enumerate(remaining, 1):
        raw_data[orgnr] = collect_one(orgnr)

        if i % SAVE_EVERY == 0:
            with open(RAW_FILE, "w") as f:
                json.dump(raw_data, f, ensure_ascii=False)
            gcs_upload(RAW_FILE, checkpoint_gcs)
            done = len(already) + i
            update_status("collect", progress=f"{done}/{total_target}",
                          detail=f"Last: {orgnr}")

        time.sleep(DELAY)

    with open(RAW_FILE, "w") as f:
        json.dump(raw_data, f, ensure_ascii=False)
    gcs_upload(RAW_FILE, checkpoint_gcs)
    update_status("collect", progress=f"{len(raw_data)}/{total_target}", detail="Complete")
    return raw_data


# ═══════════════════════════════════════════════════════════════
# Stage 2: PARSE
#
# Operates entirely on stored rsc_payloads. No network access.
# Parse failures are logged per-orgnr alongside the
# successfully parsed data — they never prevent storage.
# ═══════════════════════════════════════════════════════════════

def extract_rettsstiftelser(rsc_payload):
    match = re.search(r'"data":\s*(\{)', rsc_payload)
    if not match:
        return None
    raw = rsc_payload[match.start(1):]
    depth = 0
    for i, ch in enumerate(raw):
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
        if depth == 0:
            return json.loads(raw[:i + 1]).get("rettsstiftelser", [])
    return None


def parse_entry(rs):
    if not isinstance(rs, dict):
        return None

    entry = {
        "title": rs.get("typeBeskrivelse"),
        "document_number": rs.get("dokumentnummer"),
        "innkomsttidspunkt": rs.get("innkomsttidspunkt"),
        "rettsstiftelsen_er": rs.get("statusBeskrivelse"),
    }

    for rolle in rs.get("roller", []):
        if not isinstance(rolle, dict):
            continue
        ri = rolle.get("rolleinnehaver", {})
        if not isinstance(ri, dict):
            continue
        key = rolle.get("rolletype", "")
        if key == "rolletype.panthaver":
            entry["panthaver"] = ri.get("navn", "")
            entry["panthaver_orgnr"] = ri.get("organisasjonsnummer", "")
        elif key == "rolletype.pantsetter":
            entry["pantsetter"] = ri.get("navn", "")
            entry["pantsetter_orgnr"] = ri.get("organisasjonsnummer", "")
        elif key == "rolletype.eier":
            entry["eier"] = ri.get("navn", "")
            entry["eier_orgnr"] = ri.get("organisasjonsnummer", "")

    fgs = []
    for fg in rs.get("formuesgoder", []):
        if not isinstance(fg, dict):
            continue
        desc = fg.get("typeBeskrivelse", "")
        avgrensing = fg.get("avgrensingTingsinnbegrepBeskrivelse", "")
        if avgrensing:
            desc += f" : {avgrensing}"
        ident = fg.get("identifisering", "")
        if ident:
            desc += f" {ident}"
        regnr = fg.get("registreringsnummer", "")
        if regnr:
            desc += f" {regnr}"
        fgs.append(desc)
    entry["formuesgode"] = " | ".join(fgs) if fgs else ""

    krav = rs.get("krav")
    if isinstance(krav, dict):
        belop_list = krav.get("belop", [])
        if belop_list and isinstance(belop_list, list) and len(belop_list) > 0 and isinstance(belop_list[0], dict):
            entry["beløp"] = int(belop_list[0].get("belop", 0))
        else:
            entry["beløp"] = None
        entry["krav"] = krav.get("kravFordringerBeskrivelse", "") or krav.get("beskrivelse", "")
    else:
        entry["beløp"] = None
        entry["krav"] = ""

    paategninger = rs.get("paategninger", [])
    if paategninger and isinstance(paategninger, list):
        pts = [pt.get("paategning", "") for pt in paategninger if isinstance(pt, dict)]
        if pts:
            entry["påtegning"] = " | ".join(pts)

    return entry


def parse_all(raw_data):
    update_status("parse", detail="Parsing raw responses")
    parsed = {}
    parse_errors = 0

    for orgnr, record in raw_data.items():
        result = {
            "orgnr": orgnr,
            "entries": [],
            "entries_count": 0,
            "raw_rettsstiftelser": [],
            "url": record.get("url", ""),
            "collected_at": record.get("collected_at", ""),
            "collect_error": record.get("error"),
            "parse_error": None,
        }

        if record.get("error"):
            parsed[orgnr] = result
            continue

        for payload in record.get("rsc_payloads", []):
            try:
                rettsstiftelser = extract_rettsstiftelser(payload)
                if rettsstiftelser is None:
                    continue
                result["raw_rettsstiftelser"] = rettsstiftelser
                for rs in rettsstiftelser:
                    entry = parse_entry(rs)
                    if entry:
                        result["entries"].append(entry)
                result["entries_count"] = len(result["entries"])
            except Exception as e:
                result["parse_error"] = str(e)[:500]
                parse_errors += 1

        for raw_payload in record.get("rsc_payloads_raw", []):
            try:
                decoded = json.loads('"' + raw_payload + '"')
                rettsstiftelser = extract_rettsstiftelser(decoded)
                if rettsstiftelser is None:
                    continue
                result["raw_rettsstiftelser"] = rettsstiftelser
                for rs in rettsstiftelser:
                    entry = parse_entry(rs)
                    if entry:
                        result["entries"].append(entry)
                result["entries_count"] = len(result["entries"])
            except Exception as e:
                if not result["parse_error"]:
                    result["parse_error"] = f"raw_fallback: {str(e)[:400]}"
                parse_errors += 1

        parsed[orgnr] = result

    with open(PARSED_FILE, "w") as f:
        json.dump(parsed, f, ensure_ascii=False)
    gcs_upload(PARSED_FILE, f"{GCS_PREFIX}/parsed_data.json")

    total = len(parsed)
    with_entries = sum(1 for v in parsed.values() if v["entries_count"] > 0)
    collect_errors = sum(1 for v in parsed.values() if v.get("collect_error"))
    p_errors = sum(1 for v in parsed.values() if v.get("parse_error"))
    update_status("parse", progress=f"{with_entries}/{total}",
                  detail=f"collect_errors={collect_errors} parse_errors={p_errors}")
    return parsed


# ═══════════════════════════════════════════════════════════════
# Stage 3: EXPORT
# ═══════════════════════════════════════════════════════════════

def match_panthaver(entry):
    ph = entry.get("panthaver", "") or entry.get("eier", "")
    return any(t.lower() in ph.lower() for t in SEARCH_TERMS)


def collect_rows(parsed):
    belop_by_orgnr = defaultdict(int)
    count_by_orgnr = defaultdict(int)
    for orgnr, d in parsed.items():
        for e in d.get("entries", []):
            if not match_panthaver(e):
                continue
            if e.get("title", "") not in RELEVANT_CATEGORIES:
                continue
            b = e.get("beløp")
            if b:
                belop_by_orgnr[orgnr] += b
            count_by_orgnr[orgnr] += 1

    rows = []
    for orgnr, d in parsed.items():
        for e in d.get("entries", []):
            if not match_panthaver(e):
                continue
            if e.get("title", "") not in RELEVANT_CATEGORIES:
                continue
            ts = e.get("innkomsttidspunkt", "")
            dt = None
            kl = None
            if ts:
                try:
                    p = datetime.fromisoformat(ts)
                    dt = p.replace(tzinfo=None)
                    if p.hour or p.minute:
                        kl = p.strftime("%H:%M")
                except ValueError:
                    pass

            rows.append({
                "orgnr": orgnr,
                "title": e.get("title"),
                "panthaver": e.get("panthaver", ""),
                "panthaver_orgnr": e.get("panthaver_orgnr", ""),
                "pantsetter": e.get("pantsetter", ""),
                "pantsetter_orgnr": e.get("pantsetter_orgnr", ""),
                "beløp": e.get("beløp"),
                "document_number": e.get("document_number"),
                "innkomst_dato": dt,
                "innkomst_kl": kl,
                "rettsstiftelsen_er": e.get("rettsstiftelsen_er"),
                "formuesgode": e.get("formuesgode", ""),
                "krav": e.get("krav", ""),
                "total_pant_hos_bank": count_by_orgnr[orgnr],
                "total_beløp_hos_bank": belop_by_orgnr[orgnr],
            })
    return rows


def write_xlsx(rows, headers, output_file, fmt_map=None):
    wb = Workbook()
    ws = wb.active
    ws.title = "Løsøre"
    hfont = Font(bold=True, size=11, name="Arial")
    hfill = PatternFill("solid", fgColor="D9E1F2")
    halign = Alignment(horizontal="center", vertical="center", wrap_text=True)
    cfont = Font(size=11, name="Arial")
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=ci, value=h)
        c.font = hfont
        c.fill = hfill
        c.alignment = halign
    for ri, row in enumerate(rows, 2):
        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=ri, column=ci, value=row.get(h))
            c.font = cfont
    if fmt_map:
        for col_name, nf in fmt_map.items():
            if col_name in headers:
                ci = headers.index(col_name) + 1
                for ri in range(2, len(rows) + 2):
                    ws.cell(row=ri, column=ci).number_format = nf
    for ci, h in enumerate(headers, 1):
        sample_lens = [len(str(ws.cell(row=ri, column=ci).value or ""))
                       for ri in range(2, min(len(rows) + 2, 100))]
        max_len = max([len(h)] + sample_lens) if sample_lens else len(h)
        ws.column_dimensions[get_column_letter(ci)].width = min(max(12, max_len + 3), 42)
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    wb.save(output_file)
    return len(rows)


def export(parsed):
    update_status("export", detail="Building xlsx")
    rows = collect_rows(parsed)
    fmt = {
        "beløp": '#,##0" NOK"',
        "total_beløp_hos_bank": '#,##0" NOK"',
        "innkomst_dato": "DD.MM.YYYY",
        "innkomst_kl": "@",
    }

    ENHET_FIELDS = [
        "navn", "organisasjonsform.kode", "naeringskode1.kode", "naeringskode1.beskrivelse",
        "antallAnsatte", "forretningsadresse.kommune", "forretningsadresse.postnummer",
        "forretningsadresse.poststed", "stiftelsesdato", "sisteInnsendteAarsregnskap",
        "konkurs", "underAvvikling", "erIKonsern",
    ]
    orgnr_set = set(r["orgnr"] for r in rows)
    enhet_lookup = {}
    with gzip.open(GZ_FILE, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            org = row.get("organisasjonsnummer")
            if org in orgnr_set:
                enhet_lookup[org] = {k: row.get(k, "") for k in ENHET_FIELDS}

    for r in rows:
        enhet = enhet_lookup.get(r["orgnr"], {})
        for k in ENHET_FIELDS:
            val = enhet.get(k, "")
            if k == "antallAnsatte" and val:
                try:
                    val = int(val)
                except ValueError:
                    pass
            r[k] = val

    enhet_headers = [
        "orgnr", "navn", "organisasjonsform.kode", "naeringskode1.kode", "naeringskode1.beskrivelse",
        "antallAnsatte", "forretningsadresse.kommune", "forretningsadresse.postnummer",
        "forretningsadresse.poststed", "stiftelsesdato", "sisteInnsendteAarsregnskap",
        "konkurs", "underAvvikling", "erIKonsern",
        "title", "panthaver", "panthaver_orgnr", "pantsetter", "pantsetter_orgnr",
        "beløp", "document_number", "innkomst_dato", "innkomst_kl", "rettsstiftelsen_er",
        "formuesgode", "krav", "total_pant_hos_bank", "total_beløp_hos_bank",
    ]
    local_xlsx = "/tmp/output_enhet.xlsx"
    n = write_xlsx(rows, enhet_headers, local_xlsx, fmt)
    gcs_xlsx = f"{GCS_PREFIX}/output/sparebanken_enhet.xlsx"
    gcs_upload(local_xlsx, gcs_xlsx)
    update_status("done", progress=f"{n} rows",
                  detail=f"gs://{BUCKET_NAME}/{gcs_xlsx}")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    try:
        update_status("starting")
        download_enhetsregisteret()
        raw_data = collect_all()
        parsed = parse_all(raw_data)
        export(parsed)
    except Exception as e:
        update_status("error", error=str(e))
        raise


if __name__ == "__main__":
    main()
