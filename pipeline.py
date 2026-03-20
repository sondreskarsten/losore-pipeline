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
JSON_FILE = "/tmp/scraped_data.json"


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


def scrape_losore(orgnr):
    url = f"https://rettsstiftelser.brreg.no/nb/oppslag/virksomhet/{orgnr}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    pushes = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', resp.text, re.DOTALL)

    for payload in pushes:
        try:
            p = json.loads('"' + payload + '"')
        except (json.JSONDecodeError, ValueError):
            continue
        if '"rettsstiftelser"' not in p:
            continue

        match = re.search(r'"data":\s*(\{)', p)
        if not match:
            continue

        raw = p[match.start(1):]
        depth = 0
        end = 0
        for i, ch in enumerate(raw):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
            if depth == 0:
                end = i + 1
                break

        data = json.loads(raw[:end])
        rettsstiftelser = data.get("rettsstiftelser", [])

        entries = []
        for rs in rettsstiftelser:
            entry = {
                "title": rs.get("typeBeskrivelse"),
                "document_number": rs.get("dokumentnummer"),
                "innkomsttidspunkt": rs.get("innkomsttidspunkt"),
                "rettsstiftelsen_er": rs.get("statusBeskrivelse"),
            }
            for rolle in rs.get("roller", []):
                ri = rolle.get("rolleinnehaver", {})
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

            krav = rs.get("krav") or {}
            belop_list = krav.get("belop", [])
            if belop_list:
                entry["beløp"] = int(belop_list[0].get("belop", 0))
            else:
                entry["beløp"] = None

            krav_desc = krav.get("kravFordringerBeskrivelse", "") or krav.get("beskrivelse", "")
            entry["krav"] = krav_desc

            paategninger = rs.get("paategninger", [])
            if paategninger:
                entry["påtegning"] = " | ".join(
                    pt.get("paategning", "") for pt in paategninger
                )

            entries.append(entry)

        return {
            "orgnr": orgnr,
            "entries_count": len(entries),
            "entries": entries,
            "raw_rettsstiftelser": rettsstiftelser,
            "url": url,
        }

    return {
        "orgnr": orgnr,
        "entries_count": 0,
        "entries": [],
        "raw_rettsstiftelser": [],
        "url": url,
    }


def scrape_all():
    checkpoint_gcs = f"{GCS_PREFIX}/scraped_data.json"
    if gcs_download(checkpoint_gcs, JSON_FILE):
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        rescrape = [k for k, v in data.items()
                     if v.get("entries_count", 0) == 0
                     and "raw_rettsstiftelser" not in v
                     and "error" not in v]
        if rescrape:
            for k in rescrape:
                del data[k]
            update_status("scrape", detail=f"Restored checkpoint: {len(data)} orgnr, dropped {len(rescrape)} stale zeros")
        else:
            update_status("scrape", detail=f"Restored checkpoint: {len(data)} orgnr")
    else:
        data = {}

    target_orgnr = set()
    with gzip.open(GZ_FILE, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("forretningsadresse.kommunenummer") in KOMMUNENUMMER:
                target_orgnr.add(row["organisasjonsnummer"])

    already = set(data.keys())
    remaining = sorted(target_orgnr - already)
    total_target = len(target_orgnr)
    update_status("scrape", progress=f"{len(already)}/{total_target}",
                  detail=f"Remaining: {len(remaining)}")

    for i, orgnr in enumerate(remaining, 1):
        try:
            data[orgnr] = scrape_losore(orgnr)
        except Exception as e:
            data[orgnr] = {"orgnr": orgnr, "entries_count": 0, "entries": [],
                           "raw_rettsstiftelser": [], "error": str(e)}

        if i % SAVE_EVERY == 0:
            with open(JSON_FILE, "w") as f:
                json.dump(data, f, ensure_ascii=False)
            gcs_upload(JSON_FILE, checkpoint_gcs)
            done = len(already) + i
            update_status("scrape", progress=f"{done}/{total_target}",
                          detail=f"Last: {orgnr}")

        time.sleep(DELAY)

    with open(JSON_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    gcs_upload(JSON_FILE, checkpoint_gcs)
    update_status("scrape", progress=f"{len(data)}/{total_target}", detail="Complete")
    return data


def match_panthaver(entry):
    ph = entry.get("panthaver", "") or entry.get("eier", "")
    return any(t.lower() in ph.lower() for t in SEARCH_TERMS)


def collect_rows(data):
    belop_by_orgnr = defaultdict(int)
    count_by_orgnr = defaultdict(int)
    for orgnr, d in data.items():
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
    for orgnr, d in data.items():
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
                    parsed = datetime.fromisoformat(ts)
                    dt = parsed.replace(tzinfo=None)
                    if parsed.hour or parsed.minute:
                        kl = parsed.strftime("%H:%M")
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


def export(data):
    update_status("export", detail="Building xlsx")
    rows = collect_rows(data)
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


def main():
    try:
        update_status("starting")
        download_enhetsregisteret()
        data = scrape_all()
        export(data)
    except Exception as e:
        update_status("error", error=str(e))
        raise


if __name__ == "__main__":
    main()
