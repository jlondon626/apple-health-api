import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

_container = None


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


def _parse_iso_datetime(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ApiError(400, f"{field_name} must be a non-empty ISO-8601 string")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, f"{field_name} must be a valid ISO-8601 datetime") from exc

    return value


def _parse_local_date(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ApiError(400, f"{field_name} is required")

    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ApiError(400, f"{field_name} must use YYYY-MM-DD format") from exc

    return value


def _validate_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ApiError(400, "Request body must be a JSON object")

    person_id = payload.get("personId")
    if not isinstance(person_id, str) or not person_id.strip():
        raise ApiError(400, "personId is required")

    _parse_local_date(payload.get("localDate"), "localDate")

    export_type = payload.get("exportType")
    if export_type not in {"complete-day", "today-so-far"}:
        raise ApiError(400, "exportType must be complete-day or today-so-far")

    window = payload.get("window")
    if not isinstance(window, dict):
        raise ApiError(400, "window is required")

    _parse_iso_datetime(window.get("start"), "window.start")
    _parse_iso_datetime(window.get("end"), "window.end")
    _parse_iso_datetime(payload.get("exportedAt"), "exportedAt")

    metrics = payload.get("metrics")
    if metrics is not None and not isinstance(metrics, dict):
        raise ApiError(400, "metrics must be an object")

    totals = payload.get("totals")
    if totals is not None and not isinstance(totals, dict):
        raise ApiError(400, "totals must be an object")

    return payload


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


def _build_document(payload: dict[str, Any]) -> dict[str, Any]:
    person_id = payload["personId"].strip()
    local_date = payload["localDate"]
    export_type = payload["exportType"]

    document = dict(payload)
    document["personId"] = person_id
    document["userID"] = person_id
    document["id"] = f"{person_id}:{local_date}:{export_type}"
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

    person_id = req.params.get("personId", "").strip()
    if not person_id:
        raise ApiError(400, "personId is required")

    start_date = _parse_local_date(req.params.get("startDate"), "startDate")
    end_date = _parse_local_date(req.params.get("endDate"), "endDate")
    requested_dates = _date_range(start_date, end_date)

    query = """
        SELECT c.localDate
        FROM c
        WHERE c.userID = @userID
          AND c.exportType = @exportType
          AND c.localDate >= @startDate
          AND c.localDate <= @endDate
    """
    parameters = [
        {"name": "@userID", "value": person_id},
        {"name": "@exportType", "value": "complete-day"},
        {"name": "@startDate", "value": start_date},
        {"name": "@endDate", "value": end_date},
    ]

    container = _get_container()
    existing_dates = {
        item["localDate"]
        for item in container.query_items(
            query=query,
            parameters=parameters,
            partition_key=person_id,
        )
        if isinstance(item.get("localDate"), str)
    }
    missing_dates = [date for date in requested_dates if date not in existing_dates]

    logging.info(
        "Checked missing health export dates personId=%s startDate=%s endDate=%s missing=%d",
        person_id,
        start_date,
        end_date,
        len(missing_dates),
    )

    return _json_response({"missingDates": missing_dates})


def _store_health_export(req: func.HttpRequest) -> func.HttpResponse:
    payload = _validate_payload(req.get_json())
    document = _build_document(payload)

    container = _get_container()
    result = container.upsert_item(document)

    logging.info(
        "Stored health export personId=%s localDate=%s exportType=%s",
        document["personId"],
        document["localDate"],
        document["exportType"],
    )

    return _json_response(
        {
            "ok": True,
            "id": result["id"],
            "personId": result["personId"],
            "localDate": result["localDate"],
            "exportType": result["exportType"],
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
