#!/usr/bin/env python3
"""Step 1: inspect the live DTCC PPD feed and confirm the real schema.

Run on your own machine (stdlib only, Python 3.9+):
    python inspect_dtcc.py

It will:
  1. Resolve the public bucket name from DTCC's own API (it can change).
  2. Fetch Ticker.json / Slice.json / Cumulative.json and print their shape.
  3. Download one recent CFTC cumulative RATES file and print its columns,
     plus any columns that look like correction/cancel fields.
Everything is saved under ./samples so you only hit DTCC once.
Be polite: no loops, small sleeps, cache locally.
"""
import csv
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

API = "https://pddata.dtcc.com/ppd/api/general"
UA = {"User-Agent": "dtcc-lakehouse-learning-project/0.1 (personal portfolio)"}
OUT = Path("samples")
OUT.mkdir(exist_ok=True)


def get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def get_json(url: str):
    return json.loads(get(url))


def describe(name: str, data) -> None:
    print(f"\n=== {name} ===")
    if isinstance(data, dict):
        print(f"dict with {len(data)} keys; first keys: {list(data)[:8]}")
        k = next(iter(data))
        print(f"sample [{k!r}]: {json.dumps(data[k], indent=2)[:600]}")
    elif isinstance(data, list):
        print(f"list with {len(data)} items")
        if data:
            print(f"sample item: {json.dumps(data[0], indent=2)[:600]}")
    else:
        print(str(data)[:600])


def main() -> None:
    # 1. Resolve bucket (do not hardcode: it can change)
    bucket = get(f"{API}/bucketname").decode().strip().strip('"')
    region = get(f"{API}/regionname").decode().strip().strip('"')
    print(f"bucket={bucket!r} region={region!r}")
    host = f"https://{bucket}.s3.amazonaws.com"

    # 2. Dashboard JSON indexes
    for name in ("Ticker", "Slice", "Cumulative"):
        try:
            data = get_json(f"{host}/dashboard/{name}.json")
            (OUT / f"{name}.json").write_text(json.dumps(data, indent=2))
            describe(name, data)
        except Exception as e:  # noqa: BLE001 - this is an exploration script
            print(f"\n{name}.json failed: {e}")
        time.sleep(1)

    # 3. One cumulative file (try the last few UTC days)
    today = datetime.now(timezone.utc).date()
    for back in range(1, 6):
        d = today - timedelta(days=back)
        url = f"{host}/cftc/eod/CFTC_CUMULATIVE_RATES_{d:%Y_%m_%d}.zip"
        try:
            blob = get(url)
        except Exception as e:  # noqa: BLE001
            print(f"\n{d}: {e}")
            continue
        (OUT / f"CFTC_CUMULATIVE_RATES_{d:%Y_%m_%d}.zip").write_bytes(blob)
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            member = z.namelist()[0]
            text = z.read(member).decode("utf-8", errors="replace")
        rows = list(csv.DictReader(io.StringIO(text)))
        cols = list(rows[0].keys()) if rows else []
        print(f"\n=== {url} ===")
        print(f"{len(rows)} rows, {len(cols)} columns")
        print("columns:", cols)
        hints = [c for c in cols if any(k in c.lower() for k in
                 ("action", "event", "dissemination", "original", "correct", "cancel"))]
        print("\ncorrection/cancel-related columns:", hints)
        for c in hints:
            vals = sorted({r[c] for r in rows if r[c]})[:10]
            print(f"  {c}: {vals}")
        break


if __name__ == "__main__":
    main()
