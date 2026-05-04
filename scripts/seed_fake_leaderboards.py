import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from azure.cosmos import CosmosClient


DEFAULT_CHALLENGE_ID = "challenge_2026_05_04"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_settings() -> dict[str, str]:
    settings_path = Path(__file__).resolve().parents[1] / "local.settings.json"
    with settings_path.open("r", encoding="utf-8-sig") as handle:
        settings = json.load(handle)
    return settings["Values"]


def leaderboard_doc(
    leaderboard_id: str,
    challenge_id: str,
    kind: str,
    period_label: str,
    rows: list[dict[str, Any]],
    generated_at: str,
    period_start: str | None = None,
    period_end: str | None = None,
) -> dict[str, Any]:
    doc = {
        "id": leaderboard_id,
        "leaderboardID": leaderboard_id,
        "type": "leaderboard",
        "challengeID": challenge_id,
        "kind": kind,
        "periodLabel": period_label,
        "generatedAt": generated_at,
        "rows": rows,
    }
    if period_start:
        doc["periodStart"] = period_start
    if period_end:
        doc["periodEnd"] = period_end
    return doc


def fake_leaderboards(challenge_id: str) -> list[dict[str, Any]]:
    return [
        leaderboard_doc(
            "lb_current_fake",
            challenge_id,
            "current",
            "Running tally",
            [
                {"rank": 1, "userID": "Jack", "displayName": "Jack", "score": 312.4},
                {"rank": 2, "userID": "Ash", "displayName": "Ash", "score": 298.9},
            ],
            utc_now(),
        ),
        leaderboard_doc(
            "lb_week_1_fake",
            challenge_id,
            "week",
            "Week 1",
            [
                {"rank": 1, "userID": "Ash", "displayName": "Ash", "score": 88.0},
                {"rank": 2, "userID": "Jack", "displayName": "Jack", "score": 74.5},
            ],
            "2026-05-11T00:05:00Z",
            "2026-05-04",
            "2026-05-10",
        ),
        leaderboard_doc(
            "lb_week_2_fake",
            challenge_id,
            "week",
            "Week 2",
            [
                {"rank": 1, "userID": "Jack", "displayName": "Jack", "score": 91.2},
                {"rank": 2, "userID": "Ash", "displayName": "Ash", "score": 83.6},
            ],
            "2026-05-18T00:05:00Z",
            "2026-05-11",
            "2026-05-17",
        ),
        leaderboard_doc(
            "lb_month_2026_05_fake",
            challenge_id,
            "month",
            "May 2026",
            [
                {"rank": 1, "userID": "Jack", "displayName": "Jack", "score": 183.7},
                {"rank": 2, "userID": "Ash", "displayName": "Ash", "score": 171.6},
            ],
            "2026-06-01T00:05:00Z",
            "2026-05-01",
            "2026-05-31",
        ),
        leaderboard_doc(
            "lb_final_fake",
            challenge_id,
            "final",
            "Final",
            [],
            "2026-06-02T00:05:00Z",
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed fake competition leaderboards into Cosmos DB.")
    parser.add_argument("--challenge-id", default=DEFAULT_CHALLENGE_ID)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    docs = fake_leaderboards(args.challenge_id)
    if args.dry_run:
        print(json.dumps(docs, indent=2))
        return

    settings = load_settings()
    client = CosmosClient(settings["COSMOS_ENDPOINT"], credential=settings["COSMOS_KEY"])
    database = client.get_database_client(settings["COSMOS_DATABASE"])
    container_name = settings.get("COSMOS_COMPETITION_CONTAINER", "fitness_competitions")
    container = database.get_container_client(container_name)

    for doc in docs:
        container.upsert_item(doc)
        print(f"upserted {doc['id']}")


if __name__ == "__main__":
    main()
