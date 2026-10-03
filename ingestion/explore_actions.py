#!/usr/bin/env python3
"""Step 0.3: how do corrections relate to original trades? Stdlib only."""
import csv
import io
import zipfile
from collections import Counter
from pathlib import Path

SAMPLES = Path(__file__).resolve().parents[1] / "samples"
ID = "Dissemination Identifier"
ORIG = "Original Dissemination Identifier"
ACTION = "Action type"
EVENT = "Event type"
TS = "Event timestamp"
AMEND = "Amendment indicator"


def load_rows():
    zips = sorted(SAMPLES.glob("CFTC_CUMULATIVE_RATES_*.zip"))
    if not zips:
        raise SystemExit(f"No zip found in {SAMPLES}. Run inspect_dtcc.py first.")
    path = zips[-1]
    with zipfile.ZipFile(path) as z:
        text = z.read(z.namelist()[0]).decode("utf-8", errors="replace")
    return path.name, list(csv.DictReader(io.StringIO(text)))


def main() -> None:
    name, rows = load_rows()
    print(f"{name}: {len(rows)} rows")

    print("\nAction type counts:", dict(Counter(r[ACTION] for r in rows)))
    print("Event type counts: ", dict(Counter(r[EVENT] for r in rows)))
    print("Amendment indicator:", dict(Counter(r[AMEND] for r in rows)))

    ids = Counter(r[ID] for r in rows)
    dups = sum(1 for c in ids.values() if c > 1)
    print(f"\nDissemination IDs that repeat in this file: {dups}")

    print("\nOriginal ID populated, by Action type:")
    for action in sorted({r[ACTION] for r in rows}):
        group = [r for r in rows if r[ACTION] == action]
        filled = [r for r in group if r[ORIG].strip()]
        in_file = sum(1 for r in filled if r[ORIG] in ids)
        print(f"  {action}: {len(group)} rows, {len(filled)} with Original ID, "
              f"{in_file} of those point to an ID inside this same file")

    print("\nExamples (2 per action type):")
    for action in sorted({r[ACTION] for r in rows}):
        for r in [x for x in rows if x[ACTION] == action][:2]:
            print(f"  {action} | id={r[ID]} | orig={r[ORIG]!r} | "
                  f"event={r[EVENT]} | ts={r[TS]} | amend={r[AMEND]!r}")


if __name__ == "__main__":
    main()