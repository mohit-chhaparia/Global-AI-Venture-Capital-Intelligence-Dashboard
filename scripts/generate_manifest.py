#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
MANIFEST_PATH = DATA_DIR / "manifest.json"
LAST_UPDATED_PATH = DATA_DIR / "last_updated.txt"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def detect_nation_name(payload: dict, fallback_name: str) -> str:
    deals = payload.get("deals", [])
    for deal in deals:
        nation = str(deal.get("Nation") or deal.get("Country") or "").strip()
        if nation:
            return nation
    return fallback_name


def build_manifest() -> dict:
    nations = []

    for json_path in sorted(DATA_DIR.glob("*.json"), key=lambda item: item.name.lower()):
        if json_path.name == MANIFEST_PATH.name:
            continue

        payload = load_json(json_path)
        deals = payload.get("deals", [])
        nation_name = detect_nation_name(payload, json_path.stem)

        nations.append(
            {
                "name": nation_name,
                "file": json_path.name,
                "path": f"data/{json_path.name}",
                "deal_count": len(deals),
                "last_updated": payload.get("last_updated"),
            }
        )

    nations.sort(key=lambda item: item["name"].lower())

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "last_updated": LAST_UPDATED_PATH.read_text(encoding="utf-8").strip()
        if LAST_UPDATED_PATH.exists()
        else None,
        "nations": nations,
    }

    return manifest


def main() -> None:
    manifest = build_manifest()
    MANIFEST_PATH.write_text(f"{json.dumps(manifest, indent=2)}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
