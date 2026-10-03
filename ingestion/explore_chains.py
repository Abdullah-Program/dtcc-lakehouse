#!/usr/bin/env python3
"""Step 0.5: do corrections point to the previous message or to the root trade? Stdlib only."""
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


def load_rows():
    zips = sorted(SAMPLES.glob("CFTC_CUMULATIVE_RATES_*.zip"))
    if not zips:
        raise SystemExit(f"No zip found in {SAMPLES}. Run inspect_dtcc.py first.")
    with zipfile.ZipFile(zips[-1]) as z:
        text = z.read(z.namelist()[0]).decode("utf-8", errors="replace")
    return zips[-1].name, list(csv.DictReader(io.StringIO(text)))


def main() -> None:
    name, rows = load_rows()
    print(f"{name}: {len(rows)} rows")
    by_id = {r[ID]: r for r in rows}
    non_new = [r for r in rows if r[ACTION] != "NEWT"]

    selfref = Counter(r[ACTION] for r in non_new if r[ORIG] == r[ID])
    print("\n1. Self-referencing rows by action:", dict(selfref))

    print("\n2. (child action -> action of the row it points to), same-file targets only:")
    pairs = Counter()
    for r in non_new:
        t = by_id.get(r[ORIG])
        if t is not None and t is not r:
            pairs[(r[ACTION], t[ACTION])] += 1
    for (c, t), n in sorted(pairs.items()):
        print(f"   {c} -> {t}: {n}")

    print("\n3. How many children does one original have? {children: how many originals}")
    children = Counter(r[ORIG] for r in non_new if r[ORIG] != r[ID])
    print("  ", dict(sorted(Counter(children.values()).items())))

    print("\n4. Is the child's ID larger than the ID it points to? (same-file targets)")
    bigger = smaller = 0
    for r in non_new:
        t = by_id.get(r[ORIG])
        if t is None or t is r:
            continue
        if int(r[ID]) > int(t[ID]):
            bigger += 1
        else:
            smaller += 1
    print(f"   child ID larger: {bigger}, child ID not larger: {smaller}")

    print("\n5. Empty Event type, by action:")
    ev = Counter((r[ACTION], r[EVENT] == "") for r in rows)
    for action in sorted({r[ACTION] for r in rows}):
        print(f"   {action}: empty={ev[(action, True)]}, filled={ev[(action, False)]}")

    print("\n6. ID length distribution:")
    print("   Dissemination ID:", dict(Counter(len(r[ID]) for r in rows)))
    print("   Original ID (non-NEWT):", dict(Counter(len(r[ORIG]) for r in non_new)))


if __name__ == "__main__":
    main()