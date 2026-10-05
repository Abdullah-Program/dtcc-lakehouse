#!/usr/bin/env python3
"""Phase 4: Streaming Kafka Producer for DTCC Ticker Events.

Reads real-time trade messages from samples/Ticker.json (CFTC_IR rates),
standardizes camelCase headers to snake_case, derives trade_key,
and publishes JSON records into the Redpanda topic 'dtcc.rates.raw'.
Stdlib only: streams via Redpanda rpk CLI pipe (zero pip dependencies required).
"""
import json
from pathlib import Path
import shutil
import subprocess
import sys

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "samples"
TICKER_JSON = SAMPLES_DIR / "Ticker.json"
TOPIC_NAME = "dtcc.rates.raw"


def get_docker_cmd() -> str:
    """Find a functional Docker CLI (checks 'docker' first, falls back to 'docker.exe')."""
    for candidate in ["docker", "docker.exe"]:
        if shutil.which(candidate):
            try:
                res = subprocess.run(
                    [candidate, "version"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=3,
                )
                if res.returncode == 0:
                    return candidate
            except Exception:
                continue
    return "docker"


def load_ticker_events() -> list:
    """Load CFTC_IR trade events from samples/Ticker.json."""
    if not TICKER_JSON.exists():
        raise FileNotFoundError(
            f"Cannot find {TICKER_JSON}. Run ingestion/inspect_dtcc.py first."
        )

    with open(TICKER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Prefer CFTC_IR (Interest Rates) feed
    if "CFTC_IR" in data and data["CFTC_IR"]:
        events = data["CFTC_IR"]
        print(f"Loaded {len(events)} events from CFTC_IR feed.")
        return events

    # Fallback to first available non-empty feed
    for feed_name, items in data.items():
        if items and isinstance(items, list):
            print(f"Loaded {len(items)} events from {feed_name} feed.")
            return items

    raise ValueError(f"No trade events found in {TICKER_JSON}")


def standardize_ticker_event(raw: dict) -> dict:
    """Map raw DTCC camelCase ticker payload to clean Lakehouse schema."""
    dissem_id = str(raw.get("disseminationIdentifier", "")).strip()
    orig_id = str(raw.get("originalDisseminationIdentifier", "") or "").strip()

    # trade_key derivation: root ID for NEWT, target original ID for mutations
    trade_key = orig_id if (orig_id and orig_id.upper() != "NULL" and orig_id != "None") else dissem_id

    # Extract date for partitioning from dissemination or event timestamp
    ts = raw.get("disseminationTimestamp") or raw.get("eventTimestamp") or "2026-10-02"
    file_date = ts[:10] if len(ts) >= 10 else "2026-10-02"

    return {
        "trade_key": trade_key,
        "dissemination_identifier": dissem_id,
        "original_dissemination_identifier": orig_id if orig_id else None,
        "action_type": raw.get("actionType", "NEWT"),
        "event_type": raw.get("eventType"),
        "event_timestamp": raw.get("eventTimestamp"),
        "execution_timestamp": raw.get("executionTimestamp"),
        "asset_class": raw.get("assetClass", "IR"),
        "effective_date": raw.get("effectiveDate"),
        "expiration_date": raw.get("expirationDate"),
        "notional_amount_leg_1": raw.get("notionalAmountLeg1"),
        "notional_amount_leg_2": raw.get("notionalAmountLeg2"),
        "file_date": file_date,
    }


def publish_events_to_redpanda(events: list) -> int:
    """Stream standardized events into Redpanda via rpk topic produce pipe."""
    docker_bin = get_docker_cmd()
    cmd = [
        docker_bin, "compose", "exec", "-T", "redpanda",
        "rpk", "topic", "produce", TOPIC_NAME,
        "-f", "%k:%v\n",
    ]

    print(f"Opening streaming channel to topic '{TOPIC_NAME}' via Redpanda rpk...")
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    lines = []
    for raw in events:
        clean = standardize_ticker_event(raw)
        key = clean["trade_key"]
        payload = json.dumps(clean)
        lines.append(f"{key}:{payload}")

    input_data = "\n".join(lines) + "\n"
    stdout, stderr = proc.communicate(input=input_data)

    if proc.returncode != 0:
        raise RuntimeError(f"Redpanda produce failed (code {proc.returncode}): {stderr}")

    return len(events)


def main() -> None:
    print(f"\n--- Starting DTCC Real-Time Kafka Producer (Topic: {TOPIC_NAME}) ---")
    events = load_ticker_events()

    count = publish_events_to_redpanda(events)
    print(f"Successfully streamed {count} trade events to Redpanda topic '{TOPIC_NAME}'!")
    print("--- Kafka Producer Completed Cleanly ---\n")


if __name__ == "__main__":
    main()
