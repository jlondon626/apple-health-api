import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

_container = None
APPLE_HEALTH_DATA_TYPE = "apple-health-data"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def _json_response(body: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body),
        status_code=status_code,
        mimetype="application/json",
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ApiError(500, f"Missing required app setting: {name}")
    return value


def _get_container():
    global _container
    if _container is None:
        endpoint = _require_env("COSMOS_ENDPOINT")
        key = _require_env("COSMOS_KEY")
        database_name = _require_env("COSMOS_DATABASE")
        container_name = _require_env("COSMOS_CONTAINER")

        client = CosmosClient(endpoint, credential=key)
        database = client.get_database_client(database_name)
        _container = database.get_container_client(container_name)

    return _container


def _parse_local_date(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ApiError(400, f"{field_name} is required")

    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ApiError(400, f"{field_name} must use YYYY-MM-DD format") from exc

    return value


def _parse_number(value: Any, field_name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ApiError(400, f"{field_name} must be a number")

    if value < 0:
        raise ApiError(400, f"{field_name} must be 0 or greater")

    return value


def _validate_row(row: Any, index: int) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ApiError(400, f"Row {index} must be a JSON object")

    user_id = row.get("user_id")
    if not isinstance(user_id, str) or not user_id.strip():
        raise ApiError(400, f"Row {index} user_id is required")

    row_type = row.get("type", APPLE_HEALTH_DATA_TYPE)
    if row_type != APPLE_HEALTH_DATA_TYPE:
        raise ApiError(400, f"Row {index} type must be {APPLE_HEALTH_DATA_TYPE}")

    return {
        "type": APPLE_HEALTH_DATA_TYPE,
        "user_id": user_id.strip(),
        "date": _parse_local_date(row.get("date"), f"Row {index} date"),
        "active_energy_kcal": _parse_number(
            row.get("active_energy_kcal"), f"Row {index} active_energy_kcal"
        ),
        "exercise_minutes": _parse_number(
            row.get("exercise_minutes"), f"Row {index} exercise_minutes"
        ),
        "stand_hours": _parse_number(row.get("stand_hours"), f"Row {index} stand_hours"),
    }


def _validate_rows(payload: Any) -> list[dict[str, Any]]:
    rows = payload if isinstance(payload, list) else [payload]
    if not rows:
        raise ApiError(400, "Request body must contain at least one row")

    if len(rows) > 31:
        raise ApiError(400, "Batch must contain 31 rows or fewer")

    return [_validate_row(row, index) for index, row in enumerate(rows)]


def _check_bearer_token(req: func.HttpRequest) -> None:
    expected_token = os.environ.get("HEALTH_API_TOKEN")
    if not expected_token:
        return

    auth_header = req.headers.get("Authorization", "")
    prefix = "Bearer "
    if not auth_header.startswith(prefix):
        raise ApiError(401, "Missing bearer token")

    supplied_token = auth_header[len(prefix) :].strip()
    if supplied_token != expected_token:
        raise ApiError(403, "Invalid bearer token")


def _build_document(row: dict[str, Any]) -> dict[str, Any]:
    user_id = row["user_id"]
    local_date = row["date"]

    document = dict(row)
    document["userID"] = user_id
    document["id"] = f"{user_id}:{local_date}"
    document["receivedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return document


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end < start:
        raise ApiError(400, "endDate must be on or after startDate")

    days = (end - start).days
    if days > 370:
        raise ApiError(400, "Date range must be 370 days or fewer")

    return [(start + timedelta(days=offset)).isoformat() for offset in range(days + 1)]


def _get_missing_dates(req: func.HttpRequest) -> func.HttpResponse:
    action = req.params.get("action")
    if action != "missing-dates":
        raise ApiError(400, "Unsupported action")

    user_id = req.params.get("user_id", "").strip()
    if not user_id:
        raise ApiError(400, "user_id is required")

    start_date = _parse_local_date(req.params.get("startDate"), "startDate")
    end_date = _parse_local_date(req.params.get("endDate"), "endDate")
    requested_dates = _date_range(start_date, end_date)

    query = """
        SELECT c.date
        FROM c
        WHERE c.userID = @userID
          AND c.type = @type
          AND c.date >= @startDate
          AND c.date <= @endDate
    """
    parameters = [
        {"name": "@userID", "value": user_id},
        {"name": "@type", "value": APPLE_HEALTH_DATA_TYPE},
        {"name": "@startDate", "value": start_date},
        {"name": "@endDate", "value": end_date},
    ]

    container = _get_container()
    existing_dates = {
        item["date"]
        for item in container.query_items(
            query=query,
            parameters=parameters,
            partition_key=user_id,
        )
        if isinstance(item.get("date"), str)
    }
    missing_dates = [date for date in requested_dates if date not in existing_dates]

    logging.info(
        "Checked missing health export dates user_id=%s startDate=%s endDate=%s missing=%d",
        user_id,
        start_date,
        end_date,
        len(missing_dates),
    )

    return _json_response({"missingDates": missing_dates})


def _store_health_export(req: func.HttpRequest) -> func.HttpResponse:
    rows = _validate_rows(req.get_json())

    container = _get_container()
    documents = [_build_document(row) for row in rows]
    results = [container.upsert_item(document) for document in documents]

    logging.info(
        "Stored health export rows=%d users=%s",
        len(results),
        sorted({document["user_id"] for document in documents}),
    )

    return _json_response(
        {
            "ok": True,
            "count": len(results),
            "ids": [result["id"] for result in results],
        },
        status_code=200,
    )


@app.route(route="health-export", methods=["GET", "POST"])
def health_export(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        if req.method == "GET":
            return _get_missing_dates(req)
        if req.method == "POST":
            return _store_health_export(req)

        return _json_response({"ok": False, "error": "Method not allowed"}, status_code=405)
    except ValueError:
        return _json_response({"ok": False, "error": "Invalid JSON body"}, status_code=400)
    except ApiError as exc:
        logging.warning("Health export rejected: %s", exc.message)
        return _json_response({"ok": False, "error": exc.message}, status_code=exc.status_code)
    except exceptions.CosmosHttpResponseError:
        logging.exception("Cosmos DB write failed")
        return _json_response({"ok": False, "error": "Cosmos DB write failed"}, status_code=502)
    except Exception:
        logging.exception("Unhandled health export error")
        return _json_response({"ok": False, "error": "Internal server error"}, status_code=500)


@app.route(route="status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def status(req: func.HttpRequest) -> func.HttpResponse:
    return _json_response({"ok": True})
