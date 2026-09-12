#!/usr/bin/env python3
"""Retain continuous coordinator event history and selected Jobs' Grant ordering.

Read-only. The cursor directory must be visible to the selected native CLI.
No resource capacity is inferred from sampled process load.
"""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cli", required=True)
    p.add_argument("--destination", type=Path, required=True)
    p.add_argument("--job-id", action="append", default=[])
    args = p.parse_args()
    directory = args.destination.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + "\n")
    def query(*command):
        return json.loads(subprocess.check_output([args.cli, *command], timeout=15))
    daemon = query("daemon-status")
    save("daemon.json", daemon)
    cursor = directory / "cursor.json"
    native = args.cli.lower().endswith(".exe")
    cursor_argument = (subprocess.check_output(["wslpath", "-w", str(cursor)], text=True).strip()
                       if native else str(cursor))
    events = []
    page_number = 0
    while True:
        command = ["--endpoint", daemon["endpoint"], "machine", "events", "--limit", "256"]
        if cursor.exists():
            command += ["--cursor", cursor_argument]
        page = query(*command)
        save(f"page-{page_number:04}.json", page)
        if page["gap"]:
            raise RuntimeError("event history gap; cannot prove continuous ordering")
        for event in page["events"]:
            expected = events[-1]["cursor"]["sequence"] + 1 if events else 1
            if event["cursor"]["sequence"] != expected:
                raise RuntimeError("incomplete event prefix; cannot compute all active debits")
            events.append(event)
        save("cursor.json", page["cursor"])
        page_number += 1
        if not page["more"]:
            break
        if page_number > 4096:
            raise RuntimeError("event collection bound exceeded; retained prefix is incomplete")
    selected = set(args.job_id)
    selected_events = [e for e in events if e["owner"]["job_id"] in selected]
    if selected - {e["owner"]["job_id"] for e in selected_events}:
        raise RuntimeError("one of the selected Jobs has no coordinator events")
    active = {}
    timeline = []
    lower = min((e["cursor"]["sequence"] for e in selected_events), default=1)
    upper = max((e["cursor"]["sequence"] for e in selected_events), default=len(events))
    for event in events:
        grant = event["grant_id"]
        # Offers reserve their vector too; unacknowledged/uncertain Grants retain it.
        if event["state"] == "released":
            active.pop(grant, None)
        else:
            active[grant] = event
        sequence = event["cursor"]["sequence"]
        if lower <= sequence <= upper:
            cargo = {key: e["claims"]["scalars"].get("cargo_slots", 0)
                     for key, e in active.items() if e["claims"]["scalars"].get("cargo_slots", 0)}
            timeline.append({"sequence": sequence, "at": event["committed_unix_millis"],
                             "cargo_debit": sum(cargo.values()), "cargo_grants": cargo,
                             "changed_grant": grant, "state": event["state"]})
    save("selected-events.json", selected_events)
    save("debit-timeline.json", timeline)
    summary = {"event_count": len(events), "selected_jobs": sorted(selected),
               "maximum_cargo_debit_in_selected_window": max((t["cargo_debit"] for t in timeline), default=0),
               "continuous_from_sequence": 1, "last_cursor": events[-1]["cursor"] if events else None}
    save("summary.json", summary)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
