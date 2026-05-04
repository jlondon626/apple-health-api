import json

import azure.functions as func
import pytest

import function_app as app


class FakeCompetitionContainer:
    def __init__(self):
        self.items = {}

    def upsert_item(self, document):
        self.items[document["id"]] = dict(document)
        return dict(document)

    def query_items(self, query, parameters, **kwargs):
        params = {item["name"]: item["value"] for item in parameters}
        rows = list(self.items.values())

        if "@type" in params:
            rows = [row for row in rows if row.get("type") == params["@type"]]
        if "@userID" in params:
            rows = [row for row in rows if row.get("userID") == params["@userID"]]
        if "@challengeID" in params:
            rows = [row for row in rows if row.get("challengeID") == params["@challengeID"]]
        if "@status" in params:
            rows = [row for row in rows if row.get("status") == params["@status"]]
        if "@kind" in params:
            rows = [row for row in rows if row.get("kind") == params["@kind"]]
        if "@userIDs" in params:
            rows = [row for row in rows if row.get("userID") in params["@userIDs"]]
        if "@startDate" in params:
            date_field = "date" if "c.date" in query else "startDate"
            rows = [row for row in rows if row.get(date_field, "") >= params["@startDate"]]
        if "@endDate" in params:
            date_field = "date" if "c.date" in query else "endDate"
            rows = [row for row in rows if row.get(date_field, "") <= params["@endDate"]]

        if "ORDER BY c.startDate ASC" in query:
            rows.sort(key=lambda row: row.get("startDate", ""))
        if "ORDER BY c.publishedAt DESC" in query:
            rows.sort(key=lambda row: row.get("publishedAt", ""), reverse=True)
        if "TOP 1" in query:
            rows = rows[:1]

        return rows


class FakeHealthContainer:
    def __init__(self, items=None):
        self.items = {item["id"]: dict(item) for item in items or []}

    def upsert_item(self, document):
        self.items[document["id"]] = dict(document)
        return dict(document)

    def query_items(self, query, parameters, **kwargs):
        params = {item["name"]: item["value"] for item in parameters}
        rows = list(self.items.values())

        if "@userID" in params:
            rows = [
                row
                for row in rows
                if row.get("user_id") == params["@userID"] or row.get("userID") == params["@userID"]
            ]
        if "@type" in params:
            rows = [row for row in rows if row.get("type") == params["@type"]]
        if "@startDate" in params:
            rows = [row for row in rows if row.get("date", "") >= params["@startDate"]]
        if "@endDate" in params:
            rows = [row for row in rows if row.get("date", "") <= params["@endDate"]]

        return rows


@pytest.fixture()
def fake_container(monkeypatch):
    container = FakeCompetitionContainer()
    monkeypatch.setattr(app, "_competition_container", container)
    monkeypatch.delenv("HEALTH_API_TOKEN", raising=False)
    return container


def request(method, url, body=None, params=None, route_params=None, headers=None):
    encoded_body = b""
    if body is not None:
        encoded_body = json.dumps(body).encode()
    return func.HttpRequest(
        method=method,
        url=url,
        headers=headers or {"content-type": "application/json"},
        params=params or {},
        route_params=route_params or {},
        body=encoded_body,
    )


def response_json(response):
    return json.loads(response.get_body().decode())


def test_create_and_patch_user(fake_container):
    create_response = app.create_user(
        request(
            "POST",
            "/api/users",
            {
                "userID": "Jack",
                "displayName": "Jack",
                "timezone": "Europe/London",
                "goalWeightKg": 87,
                "weeklyCalorieTarget": 16800,
            },
        )
    )

    assert create_response.status_code == 201
    created = response_json(create_response)
    assert created["id"] == "user_jack"
    assert created["userID"] == "Jack"
    assert "_etag" not in created

    patch_response = app.patch_user(
        request(
            "PATCH",
            "/api/users/Jack",
            {"goalWeightKg": 85.5},
            route_params={"user_id": "Jack"},
        )
    )

    assert patch_response.status_code == 200
    patched = response_json(patch_response)
    assert patched["goalWeightKg"] == 85.5
    assert patched["createdAt"] == created["createdAt"]
    assert patched["updatedAt"] >= created["updatedAt"]


def test_current_or_upcoming_returns_active_before_upcoming(fake_container):
    fake_container.upsert_item(
        {
            "id": "challenge_later",
            "type": "challenge",
            "challengeID": "challenge_later",
            "name": "Later",
            "status": "upcoming",
            "startDate": "2026-06-01",
            "endDate": "2026-06-30",
            "participants": [],
        }
    )
    fake_container.upsert_item(
        {
            "id": "challenge_active",
            "type": "challenge",
            "challengeID": "challenge_active",
            "name": "Active",
            "status": "active",
            "startDate": "2026-05-01",
            "endDate": "2026-05-31",
            "participants": [],
        }
    )

    response = app.current_or_upcoming_challenge(
        request("GET", "/api/challenges/current-or-upcoming")
    )

    assert response.status_code == 200
    assert response_json(response)["challengeID"] == "challenge_active"


def test_participant_add_and_remove_updates_challenge(fake_container):
    fake_container.upsert_item(
        {
            "id": "user_jack",
            "type": "user",
            "userID": "Jack",
            "displayName": "Jack",
            "active": True,
        }
    )
    fake_container.upsert_item(
        {
            "id": "challenge_2026_05_04",
            "type": "challenge",
            "challengeID": "challenge_2026_05_04",
            "name": "League",
            "status": "upcoming",
            "startDate": "2026-05-10",
            "endDate": "2026-08-02",
            "participants": [],
        }
    )

    add_response = app.add_participant(
        request(
            "POST",
            "/api/challenges/challenge_2026_05_04/participants",
            {"userID": "Jack"},
            route_params={"challenge_id": "challenge_2026_05_04"},
        )
    )

    assert add_response.status_code == 201
    assert response_json(add_response)["participantId"] == "challenge_2026_05_04__Jack"
    assert fake_container.items["challenge_2026_05_04"]["participants"] == ["Jack"]

    remove_response = app.remove_participant(
        request(
            "DELETE",
            "/api/challenges/challenge_2026_05_04/participants/Jack",
            route_params={"challenge_id": "challenge_2026_05_04", "user_id": "Jack"},
        )
    )

    assert remove_response.status_code == 200
    assert response_json(remove_response)["active"] is False
    assert fake_container.items["challenge_2026_05_04"]["participants"] == []


def test_latest_leaderboard_read(fake_container):
    fake_container.upsert_item(
        {
            "id": "leaderboard_old",
            "type": "leaderboard",
            "challengeID": "challenge_2026_05_04",
            "kind": "week",
            "publishedAt": "2026-05-11T00:00:00Z",
            "rows": [{"userID": "Jack", "score": 8}],
        }
    )
    fake_container.upsert_item(
        {
            "id": "leaderboard_new",
            "type": "leaderboard",
            "challengeID": "challenge_2026_05_04",
            "kind": "week",
            "publishedAt": "2026-05-18T00:00:00Z",
            "rows": [{"userID": "Ash", "score": 10}],
        }
    )

    response = app.latest_leaderboard(
        request(
            "GET",
            "/api/challenges/challenge_2026_05_04/leaderboards/latest",
            params={"kind": "week"},
            route_params={"challenge_id": "challenge_2026_05_04"},
        )
    )

    assert response.status_code == 200
    body = response_json(response)
    assert body["id"] == "leaderboard_new"
    assert body["generatedAt"] == "2026-05-18T00:00:00Z"


def test_auth_enabled_rejects_missing_bearer_with_403(fake_container, monkeypatch):
    monkeypatch.setenv("HEALTH_API_TOKEN", "expected-token")

    response = app.list_users(request("GET", "/api/users"))

    assert response.status_code == 403
    assert response_json(response)["error"] == "Missing bearer token"


def test_health_export_upserts_stable_ids_and_missing_dates(monkeypatch):
    container = FakeHealthContainer(
        [
            {
                "id": "apple-health-data::Jack::2026-05-01",
                "type": "apple-health-data",
                "user_id": "Jack",
                "userID": "Jack",
                "date": "2026-05-01",
            }
        ]
    )
    monkeypatch.setattr(app, "_container", container)
    monkeypatch.delenv("HEALTH_API_TOKEN", raising=False)

    post_response = app.health_export(
        request(
            "POST",
            "/api/health-export",
            [
                {
                    "type": "apple-health-data",
                    "user_id": "Jack",
                    "date": "2026-05-04",
                    "active_energy_kcal": 642,
                    "exercise_minutes": 48,
                    "stand_hours": 11,
                }
            ],
        )
    )

    assert post_response.status_code == 200
    assert response_json(post_response)["ids"] == ["apple-health-data::Jack::2026-05-04"]
    assert "apple-health-data::Jack::2026-05-04" in container.items

    missing_response = app.health_export(
        request(
            "GET",
            "/api/health-export",
            params={
                "action": "missing-dates",
                "user_id": "Jack",
                "startDate": "2026-05-01",
                "endDate": "2026-05-03",
            },
        )
    )

    assert missing_response.status_code == 200
    assert response_json(missing_response)["missingDates"] == ["2026-05-02", "2026-05-03"]
