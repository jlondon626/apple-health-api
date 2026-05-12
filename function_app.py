import json
import logging
import os
import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

_container = None
_competition_container = None
APPLE_HEALTH_DATA_TYPE = "apple-health-data"
COMPETITION_CONTAINER_SETTING = "COSMOS_COMPETITION_CONTAINER"
DEFAULT_COMPETITION_CONTAINER = "fitness_competitions"
LEADERBOARD_TYPES_BY_KIND = {
    "week": "leaderboard_week",
    "month": "leaderboard_month",
    "final": "leaderboard_final",
}
PUBLISHED_LEADERBOARD_TYPES = set(LEADERBOARD_TYPES_BY_KIND.values())
LEADERBOARD_KINDS = {"week", "month", "final"}
LEADERBOARD_MESSAGE_TYPE = "leaderboard_ai_message"
DEFAULT_CHALLENGE_RULES = {
    "description": "Each week is scored out of 45 points across weight trend, calorie adherence, food logging, active calories, and weigh-ins. Higher scores rank better; the lowest score loses the relevant period.",
    "scoring": [
        "Weight trend is worth up to 10 points based on weekly percentage bodyweight change.",
        "Calorie adherence is worth up to 10 points based on average daily variance from target calories.",
        "Food logging is worth up to 10 points based on the number of days food is logged.",
        "Active calories are worth up to 10 points based on weekly active calories per kg of bodyweight.",
        "Weigh-ins are worth up to 5 points based on the number of days with a weigh-in.",
        "Weekly maximum score is 45 points.",
        "Leaderboard generation is handled by the scoring job; this API exposes the published results.",
    ],
    "tieBreaker": "Highest active calories per kg wins ties, then most food logging days, then most weigh-in days.",
    "healthDataWindow": "Complete local calendar days only. Weekly scores use the configured challenge week.",
    "sections": [
        {
            "title": "Weight trend",
            "points": [
                "Worth up to 10 points.",
                "Metric: weeklyWeightChangePct.",
                "Best band: -1.0% to -0.25% bodyweight change earns 10 points.",
                "Very aggressive loss below -1.25% earns 4 points.",
                "Maintaining or gaining weight earns fewer points.",
                "Requires at least 5 weigh-ins; below that, this category is capped at 3 points.",
            ],
        },
        {
            "title": "Calorie adherence",
            "points": [
                "Worth up to 10 points.",
                "Metric: averageDailyCalorieVariance.",
                "Within 100 kcal of target earns 10 points.",
                "Within 200 kcal earns 7 points.",
                "Within 300 kcal earns 4 points.",
                "More than 300 kcal away earns 0 points.",
                "Requires at least 3 logged calorie days; below that, this category is capped at 3 points.",
            ],
        },
        {
            "title": "Food logging",
            "points": [
                "Worth up to 10 points.",
                "Metric: daysWithFoodLogged.",
                "7 logged days earns 10 points.",
                "6 logged days earns 8 points.",
                "5 logged days earns 5 points.",
                "Fewer than 5 logged days earns 0 points.",
            ],
        },
        {
            "title": "Active calories",
            "points": [
                "Worth up to 10 points.",
                "Metric: weeklyActiveCaloriesPerKg.",
                "Calculated as totalWeeklyActiveCalories / averageBodyweightKg.",
                "45 or more kcal/kg earns 10 points.",
                "35 or more kcal/kg earns 8 points.",
                "25 or more kcal/kg earns 5 points.",
                "15 or more kcal/kg earns 2 points.",
                "Requires at least 5 active calorie days; below that, this category is capped at 3 points.",
            ],
        },
        {
            "title": "Weigh-ins",
            "points": [
                "Worth up to 5 points.",
                "Metric: daysWithWeighIn.",
                "5 or more weigh-in days earns 5 points.",
                "3 or more weigh-in days earns 3 points.",
                "1 or more weigh-in days earns 1 point.",
                "No weigh-ins earns 0 points.",
            ],
        },
    ],
    "minimumData": {
        "requiredDailyMetrics": [
            "weeklyWeightChangePct",
            "averageDailyCalorieVariance",
            "daysWithFoodLogged",
            "weeklyActiveCaloriesPerKg",
            "daysWithWeighIn",
        ],
        "missingMetricTreatment": "Missing data can reduce the category score or trigger the category-specific cap.",
        "missingDayTreatment": "Missing source data remains missing until synced; low data volume may cap weight trend, calorie adherence, and active calorie scores.",
    },
    "caps": {
        "description": "Category scores are capped when minimum data requirements are not met.",
        "weightTrend": "If fewer than 5 weigh-ins are available, weight trend is capped at 3 points.",
        "calorieAdherence": "If fewer than 3 calorie data points are available, calorie adherence is capped at 3 points.",
        "activeCalories": "If fewer than 5 active calorie days are available, active calories are capped at 3 points.",
    },
    "maxPoints": 45,
    "categories": {
        "weightTrend": {
            "label": "Weight trend",
            "maxPoints": 10,
            "metric": "weeklyWeightChangePct",
            "bands": [
                {"minPct": -999, "maxPct": -0.0125, "points": 4},
                {"minPct": -0.01, "maxPct": -0.0025, "points": 10},
                {"minPct": -0.0025, "maxPct": 0, "points": 6},
                {"minPct": 0, "maxPct": 0.005, "points": 3},
                {"minPct": 0.005, "maxPct": 999, "points": 0},
            ],
            "minDataPoints": 5,
            "dataPointType": "weighIns",
            "maxPointsIfBelowMinDataPoints": 3,
        },
        "calorieAdherence": {
            "label": "Calorie adherence",
            "maxPoints": 10,
            "metric": "averageDailyCalorieVariance",
            "bands": [
                {"maxAbsVariance": 100, "points": 10},
                {"maxAbsVariance": 200, "points": 7},
                {"maxAbsVariance": 300, "points": 4},
                {"maxAbsVariance": 99999, "points": 0},
            ],
            "minDataPoints": 3,
            "maxPointsIfBelowMinDataPoints": 3,
        },
        "foodLogging": {
            "label": "Food logging",
            "maxPoints": 10,
            "metric": "daysWithFoodLogged",
            "bands": [
                {"minDays": 7, "points": 10},
                {"minDays": 6, "points": 8},
                {"minDays": 5, "points": 5},
                {"minDays": 0, "points": 0},
            ],
        },
        "activeCalories": {
            "label": "Active calories",
            "maxPoints": 10,
            "metric": "weeklyActiveCaloriesPerKg",
            "definition": "totalWeeklyActiveCalories / averageBodyweightKg",
            "bands": [
                {"minValue": 45, "points": 10},
                {"minValue": 35, "points": 8},
                {"minValue": 25, "points": 5},
                {"minValue": 15, "points": 2},
                {"minValue": 0, "points": 0},
            ],
            "minDataPoints": 5,
            "dataPointType": "activeCalorieDays",
            "maxPointsIfBelowMinDataPoints": 3,
        },
        "weighIns": {
            "label": "Weigh-ins",
            "maxPoints": 5,
            "metric": "daysWithWeighIn",
            "bands": [
                {"minDays": 5, "points": 5},
                {"minDays": 3, "points": 3},
                {"minDays": 1, "points": 1},
                {"minDays": 0, "points": 0},
            ],
        },
    },
    "aggregation": {
        "method": "sum",
        "capAtMaxPoints": True,
    },
}
COSMOS_SYSTEM_FIELDS = {
    "_rid",
    "_self",
    "_etag",
    "_attachments",
    "_ts",
}
SECRET_FIELD_PATTERNS = (
    "password",
    "passwd",
    "token",
    "secret",
    "apikey",
    "api_key",
    "accesskey",
    "access_key",
    "clientsecret",
    "client_secret",
    "connectionstring",
    "connection_string",
    "functionappsetting",
    "function_app_setting",
    "functionsetting",
    "function_setting",
    "settingname",
    "setting_name",
    "azurewebjobs",
)
SAFE_NON_SECRET_FIELD_NAMES = {"fatsecret"}
SAFE_CREDENTIAL_REF_PATTERN = re.compile(r"^[a-z0-9_-]{1,32}$")


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


def _get_competition_container():
    global _competition_container
    if _competition_container is None:
        endpoint = _require_env("COSMOS_ENDPOINT")
        key = _require_env("COSMOS_KEY")
        database_name = _require_env("COSMOS_DATABASE")
        container_name = os.environ.get(COMPETITION_CONTAINER_SETTING, DEFAULT_COMPETITION_CONTAINER)

        client = CosmosClient(endpoint, credential=key)
        database = client.get_database_client(database_name)
        _competition_container = database.get_container_client(container_name)

    return _competition_container


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalise_id(value: str) -> str:
    normalised = "".join(char.lower() if char.isalnum() else "_" for char in value.strip())
    return "_".join(part for part in normalised.split("_") if part)


def _user_doc_id(user_id: str) -> str:
    return f"user_{_normalise_id(user_id)}"


def _credential_ref_from_user_id(user_id: str) -> str:
    return _normalise_id(user_id)


def _participant_doc_id(challenge_id: str, user_id: str) -> str:
    return f"{challenge_id}__{user_id.strip()}"


def _strip_cosmos_fields(document: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key not in COSMOS_SYSTEM_FIELDS}


def _strip_user_document(document: dict[str, Any]) -> dict[str, Any]:
    user = _strip_cosmos_fields(document)
    if "averageDailyCalorieTarget" not in user and user.get("weeklyCalorieTarget") is not None:
        user["averageDailyCalorieTarget"] = user["weeklyCalorieTarget"] / 7
    user.pop("weeklyCalorieTarget", None)
    return user


def _model_payload(model: BaseModel, exclude_unset: bool = False) -> dict[str, Any]:
    return model.model_dump(exclude_unset=exclude_unset)


def _validation_error_response(exc: ValidationError) -> func.HttpResponse:
    errors = [
        {
            "field": ".".join(str(part) for part in error["loc"]),
            "message": error["msg"],
        }
        for error in exc.errors()
    ]
    return _json_response({"ok": False, "error": "Validation failed", "details": errors}, 400)


def _looks_secret_field(field_name: str) -> bool:
    normalised = "".join(char.lower() for char in field_name if char.isalnum() or char == "_")
    if normalised in SAFE_NON_SECRET_FIELD_NAMES:
        return False
    return any(pattern in normalised for pattern in SECRET_FIELD_PATTERNS)


def _reject_secret_fields(value: Any, path: str = "body") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if _looks_secret_field(str(key)):
                raise ApiError(400, f"{path}.{key} must not contain secrets or secret references")
            _reject_secret_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secret_fields(child, f"{path}[{index}]")


def _get_json_body(req: func.HttpRequest) -> Any:
    payload = req.get_json()
    _reject_secret_fields(payload)
    return payload


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
        raise ApiError(403, "Missing bearer token")

    supplied_token = auth_header[len(prefix) :].strip()
    if supplied_token != expected_token:
        raise ApiError(403, "Invalid bearer token")


def _build_document(row: dict[str, Any]) -> dict[str, Any]:
    user_id = row["user_id"]
    local_date = row["date"]

    document = dict(row)
    document["userID"] = user_id
    document["id"] = f"{APPLE_HEALTH_DATA_TYPE}::{user_id}::{local_date}"
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


def _date_label(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%a")


def _to_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def _is_zero_or_missing_health_row(row: dict[str, Any]) -> bool:
    metric_names = ("active_energy_kcal", "exercise_minutes", "stand_hours")
    for metric_name in metric_names:
        value = row.get(metric_name)
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)) and value > 0:
            return False
    return True


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
    include_today = req.params.get("includeToday", "true").lower() not in {"false", "0", "no"}
    refresh_zero_days = req.params.get("refreshZeroDays", "true").lower() not in {"false", "0", "no"}
    refresh_existing = req.params.get("refreshExisting", "false").lower() in {"true", "1", "yes"}
    try:
        refresh_recent_days = int(req.params.get("refreshRecentDays", "7"))
    except ValueError as exc:
        raise ApiError(400, "refreshRecentDays must be a number") from exc
    if refresh_recent_days < 0 or refresh_recent_days > 31:
        raise ApiError(400, "refreshRecentDays must be between 0 and 31")
    today_date = (
        _parse_local_date(req.params.get("todayDate"), "todayDate")
        if req.params.get("todayDate")
        else datetime.now(timezone.utc).date().isoformat()
    )
    today = _to_date(today_date)
    recent_dates = {
        (today - timedelta(days=offset)).isoformat()
        for offset in range(refresh_recent_days)
    }

    query = """
        SELECT c.date, c.active_energy_kcal, c.exercise_minutes, c.stand_hours
        FROM c
        WHERE (c.user_id = @userID OR c.userID = @userID)
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
    existing_rows = [
        item
        for item in container.query_items(
            query=query,
            parameters=parameters,
            partition_key=user_id,
        )
        if isinstance(item.get("date"), str)
    ]
    existing_dates = {item["date"] for item in existing_rows}
    refreshable_dates = {
        item["date"] for item in existing_rows if refresh_zero_days and _is_zero_or_missing_health_row(item)
    }
    missing_dates = [
        date
        for date in requested_dates
        if refresh_existing
        or date not in existing_dates
        or date in refreshable_dates
        or date in recent_dates
        or (include_today and date == today_date)
    ]

    logging.info(
        "Checked missing health export dates user_id=%s startDate=%s endDate=%s includeToday=%s refreshZeroDays=%s refreshExisting=%s refreshRecentDays=%d missing=%d",
        user_id,
        start_date,
        end_date,
        include_today,
        refresh_zero_days,
        refresh_existing,
        refresh_recent_days,
        len(missing_dates),
    )

    return _json_response({"missingDates": missing_dates})


def _store_health_export(req: func.HttpRequest) -> func.HttpResponse:
    rows = _validate_rows(_get_json_body(req))

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


class SourceCredentialMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    credentialRef: str | None = None

    @field_validator("credentialRef")
    @classmethod
    def _safe_credential_ref(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = value.strip().lower()
        if not SAFE_CREDENTIAL_REF_PATTERN.fullmatch(cleaned) or _looks_secret_field(cleaned):
            raise ValueError("must be a short safe identifier using lowercase letters, numbers, _ or -")
        return cleaned


class AppleHealthSourceMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True


class SyncSources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    renpho: SourceCredentialMetadata = Field(default_factory=SourceCredentialMetadata)
    fatsecret: SourceCredentialMetadata = Field(default_factory=SourceCredentialMetadata)
    appleHealth: AppleHealthSourceMetadata = Field(default_factory=AppleHealthSourceMetadata)

    @field_validator("renpho", "fatsecret")
    @classmethod
    def _credential_required_when_enabled(
        cls, value: SourceCredentialMetadata
    ) -> SourceCredentialMetadata:
        if value.enabled and not value.credentialRef:
            raise ValueError("credentialRef is required when source is enabled")
        return value


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    userID: str = Field(min_length=1)
    displayName: str | None = None
    timezone: str = "Europe/London"
    goalWeightKg: float | None = None
    averageDailyCalorieTarget: float | None = None
    active: bool = True
    syncSources: SyncSources | None = None

    @field_validator("userID", "displayName", "timezone")
    @classmethod
    def _strip_text(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @field_validator("goalWeightKg", "averageDailyCalorieTarget")
    @classmethod
    def _positive_number(cls, value: float | None) -> float | None:
        if value is not None and value <= 0:
            raise ValueError("must be a positive number")
        return value


class UserPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    displayName: str | None = None
    timezone: str | None = None
    goalWeightKg: float | None = None
    averageDailyCalorieTarget: float | None = None
    active: bool | None = None
    syncSources: SyncSources | None = None

    @field_validator("displayName", "timezone")
    @classmethod
    def _strip_optional_text(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @field_validator("goalWeightKg", "averageDailyCalorieTarget")
    @classmethod
    def _positive_optional_number(cls, value: float | None) -> float | None:
        if value is not None and value <= 0:
            raise ValueError("must be a positive number")
        return value


class SyncSourcesPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    syncSources: SyncSources | None = None
    renpho: SourceCredentialMetadata | None = None
    fatsecret: SourceCredentialMetadata | None = None
    appleHealth: AppleHealthSourceMetadata | None = None


class ChallengeCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    challengeID: str | None = None
    name: str = Field(min_length=1)
    status: Literal["upcoming", "active", "completed"] = "upcoming"
    startDate: str
    endDate: str
    timezone: str = "Europe/London"
    weekStartsOn: Literal["SUNDAY", "MONDAY"] = "SUNDAY"
    participants: list[str] = Field(default_factory=list)
    rules: dict[str, Any] = Field(default_factory=lambda: dict(DEFAULT_CHALLENGE_RULES))
    forfeits: dict[str, Any] = Field(default_factory=dict)
    scoringVersion: str = "v1"

    @field_validator("challengeID", "name", "timezone", "scoringVersion")
    @classmethod
    def _strip_challenge_text(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @field_validator("startDate", "endDate")
    @classmethod
    def _valid_date(cls, value: str) -> str:
        return _parse_local_date(value, "date")

    @field_validator("participants")
    @classmethod
    def _clean_participants(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("participants must contain non-empty user IDs")
            user_id = item.strip()
            if user_id not in cleaned:
                cleaned.append(user_id)
        return cleaned


class ChallengePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    status: Literal["upcoming", "active", "completed"] | None = None
    startDate: str | None = None
    endDate: str | None = None
    timezone: str | None = None
    weekStartsOn: Literal["SUNDAY", "MONDAY"] | None = None
    participants: list[str] | None = None
    rules: dict[str, Any] | None = None
    forfeits: dict[str, Any] | None = None
    scoringVersion: str | None = None

    @field_validator("name", "timezone", "scoringVersion")
    @classmethod
    def _strip_patch_text(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) else value

    @field_validator("startDate", "endDate")
    @classmethod
    def _valid_optional_date(cls, value: str | None) -> str | None:
        return _parse_local_date(value, "date") if value is not None else value

    @field_validator("participants")
    @classmethod
    def _clean_optional_participants(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        return ChallengeCreate._clean_participants(value)


class ParticipantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    userID: str = Field(min_length=1)
    active: bool = True

    @field_validator("userID")
    @classmethod
    def _strip_user_id(cls, value: str) -> str:
        return value.strip()


class ParticipantPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    active: bool | None = None


def _handle_competition_error(exc: Exception) -> func.HttpResponse:
    if isinstance(exc, ValidationError):
        return _validation_error_response(exc)
    if isinstance(exc, ValueError):
        return _json_response({"ok": False, "error": "Invalid JSON body"}, 400)
    if isinstance(exc, ApiError):
        logging.warning("Competition API rejected request: %s", exc.message)
        return _json_response({"ok": False, "error": exc.message}, exc.status_code)
    if isinstance(exc, exceptions.CosmosHttpResponseError):
        logging.exception("Cosmos DB competition request failed")
        return _json_response({"ok": False, "error": "Cosmos DB request failed"}, 502)

    logging.exception("Unhandled competition API error")
    return _json_response({"ok": False, "error": "Internal server error"}, 500)


def _query_items(query: str, parameters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(
        _get_competition_container().query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True,
        )
    )


def _query_one(query: str, parameters: list[dict[str, Any]]) -> dict[str, Any] | None:
    items = _query_items(query, parameters)
    return items[0] if items else None


def _get_user_document(user_id: str) -> dict[str, Any] | None:
    return _query_one(
        "SELECT * FROM c WHERE c.type = @type AND c.userID = @userID",
        [{"name": "@type", "value": "user"}, {"name": "@userID", "value": user_id}],
    )


def _get_challenge_document(challenge_id: str) -> dict[str, Any] | None:
    return _query_one(
        "SELECT * FROM c WHERE c.type = @type AND c.challengeID = @challengeID",
        [
            {"name": "@type", "value": "challenge"},
            {"name": "@challengeID", "value": challenge_id},
        ],
    )


def _get_participant_document(challenge_id: str, user_id: str) -> dict[str, Any] | None:
    return _query_one(
        """
        SELECT * FROM c
        WHERE c.type = @type AND c.challengeID = @challengeID AND c.userID = @userID
        """,
        [
            {"name": "@type", "value": "challenge_participant"},
            {"name": "@challengeID", "value": challenge_id},
            {"name": "@userID", "value": user_id},
        ],
    )


def _normalise_challenge_rules(rules: Any) -> dict[str, Any]:
    if not isinstance(rules, dict):
        return deepcopy(DEFAULT_CHALLENGE_RULES)

    normalised = {
        **deepcopy(DEFAULT_CHALLENGE_RULES),
        **rules,
    }
    if not isinstance(normalised.get("scoring"), list) or not normalised["scoring"]:
        normalised["scoring"] = list(DEFAULT_CHALLENGE_RULES["scoring"])
    return normalised


def _enrich_challenge(document: dict[str, Any]) -> dict[str, Any]:
    challenge = _strip_cosmos_fields(document)
    challenge["rules"] = _normalise_challenge_rules(challenge.get("rules"))
    if "forfeits" in challenge and "forfeitDetails" not in challenge:
        challenge["forfeitDetails"] = challenge["forfeits"]
    user_ids = challenge.get("participants", [])
    if not isinstance(user_ids, list) or not user_ids:
        challenge["participantProfiles"] = []
        return challenge

    profiles = _query_items(
        "SELECT * FROM c WHERE c.type = @type AND ARRAY_CONTAINS(@userIDs, c.userID)",
        [{"name": "@type", "value": "user"}, {"name": "@userIDs", "value": user_ids}],
    )
    by_user = {profile["userID"]: _strip_user_document(profile) for profile in profiles}
    challenge["participantProfiles"] = [
        by_user[user_id] for user_id in user_ids if user_id in by_user
    ]
    return challenge


def _ensure_date_order(start_date: str, end_date: str) -> None:
    if datetime.strptime(end_date, "%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d"):
        raise ApiError(400, "endDate must be on or after startDate")


def _create_or_update_user(payload: UserCreate) -> dict[str, Any]:
    now = _utc_now()
    existing = _get_user_document(payload.userID)
    body = _model_payload(payload)
    if body.get("syncSources") is None:
        credential_ref = _credential_ref_from_user_id(payload.userID)
        body["syncSources"] = _model_payload(
            SyncSources(
                renpho=SourceCredentialMetadata(enabled=True, credentialRef=credential_ref),
                fatsecret=SourceCredentialMetadata(enabled=True, credentialRef=credential_ref),
                appleHealth=AppleHealthSourceMetadata(enabled=True),
            )
        )
    user = {
        **(existing or {}),
        **body,
        "id": _user_doc_id(payload.userID),
        "type": "user",
        "userID": payload.userID,
        "displayName": payload.displayName or payload.userID,
        "createdAt": (existing or {}).get("createdAt", now),
        "updatedAt": now,
    }
    user.pop("weeklyCalorieTarget", None)
    return _strip_user_document(_get_competition_container().upsert_item(user))


def _merge_sync_sources(existing: dict[str, Any], patch: SyncSourcesPatch) -> dict[str, Any]:
    current = existing.get("syncSources") or {}
    if patch.syncSources is not None:
        return _model_payload(patch.syncSources)

    merged = {
        **_model_payload(SyncSources()),
        **current,
    }
    updates = _model_payload(patch, exclude_unset=True)
    updates.pop("syncSources", None)
    for source_name, source_value in updates.items():
        if source_value is not None:
            merged[source_name] = source_value

    return _model_payload(SyncSources.model_validate(merged))


def _create_challenge(payload: ChallengeCreate) -> dict[str, Any]:
    _ensure_date_order(payload.startDate, payload.endDate)
    challenge_id = payload.challengeID or f"challenge_{payload.startDate.replace('-', '_')}"
    now = _utc_now()
    challenge = {
        **_model_payload(payload),
        "id": challenge_id,
        "type": "challenge",
        "challengeID": challenge_id,
        "createdAt": now,
        "updatedAt": now,
    }
    return _enrich_challenge(_get_competition_container().upsert_item(challenge))


def _route_param(req: func.HttpRequest, name: str) -> str:
    value = req.route_params.get(name) if req.route_params else None
    if not value:
        raise ApiError(400, f"{name} is required")
    return value


@app.route(route="users", methods=["POST"])
def create_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        return _json_response(_create_or_update_user(UserCreate.model_validate(_get_json_body(req))), 201)
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="users", methods=["GET"])
def list_users(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        active = req.params.get("active")
        if active is None:
            query = "SELECT * FROM c WHERE c.type = @type ORDER BY c.displayName"
            parameters = [{"name": "@type", "value": "user"}]
        else:
            query = "SELECT * FROM c WHERE c.type = @type AND c.active = @active ORDER BY c.displayName"
            parameters = [
                {"name": "@type", "value": "user"},
                {"name": "@active", "value": active.lower() == "true"},
            ]
        return _json_response({"users": [_strip_user_document(item) for item in _query_items(query, parameters)]})
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="users/{user_id}", methods=["GET"])
def get_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        user = _get_user_document(_route_param(req, "user_id"))
        if not user:
            raise ApiError(404, "User not found")
        return _json_response(_strip_user_document(user))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="users/{user_id}", methods=["PATCH"])
def patch_user(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        user_id = _route_param(req, "user_id")
        existing = _get_user_document(user_id)
        if not existing:
            raise ApiError(404, "User not found")
        patch = UserPatch.model_validate(_get_json_body(req))
        updated = {
            **existing,
            **_model_payload(patch, exclude_unset=True),
            "updatedAt": _utc_now(),
        }
        updated.pop("weeklyCalorieTarget", None)
        return _json_response(_strip_user_document(_get_competition_container().upsert_item(updated)))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="users/{user_id}/sync-sources", methods=["PATCH"])
def patch_user_sync_sources(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        user_id = _route_param(req, "user_id")
        existing = _get_user_document(user_id)
        if not existing:
            raise ApiError(404, "User not found")
        patch = SyncSourcesPatch.model_validate(_get_json_body(req))
        updated = {
            **existing,
            "syncSources": _merge_sync_sources(existing, patch),
            "updatedAt": _utc_now(),
        }
        return _json_response(_strip_user_document(_get_competition_container().upsert_item(updated)))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="users/{user_id}/profile-check", methods=["GET"])
def user_profile_check(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        user_id = _route_param(req, "user_id")
        user = _get_user_document(user_id)
        if not user:
            raise ApiError(404, "User not found")
        normalised_user = _strip_user_document(user)
        required = ["goalWeightKg", "averageDailyCalorieTarget", "timezone"]
        missing = [field for field in required if normalised_user.get(field) in (None, "")]
        return _json_response({"userID": user_id, "ready": not missing, "missingFields": missing})
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="app/bootstrap", methods=["GET"])
def app_bootstrap(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        user_id = req.params.get("user_id", "").strip()
        if not user_id:
            raise ApiError(400, "user_id is required")
        user = _get_user_document(user_id)
        challenge = _current_or_upcoming_challenge()
        return _json_response(
            {
                "user": _strip_user_document(user) if user else None,
                "challenge": _enrich_challenge(challenge) if challenge else None,
            }
        )
    except Exception as exc:
        return _handle_competition_error(exc)


def _current_or_upcoming_challenge() -> dict[str, Any] | None:
    active = _query_one(
        """
        SELECT * FROM c
        WHERE c.type = @type AND c.status = @status
        ORDER BY c.startDate ASC
        """,
        [{"name": "@type", "value": "challenge"}, {"name": "@status", "value": "active"}],
    )
    if active:
        return active
    return _query_one(
        """
        SELECT * FROM c
        WHERE c.type = @type AND c.status = @status
        ORDER BY c.startDate ASC
        """,
        [{"name": "@type", "value": "challenge"}, {"name": "@status", "value": "upcoming"}],
    )


@app.route(route="challenges/current-or-upcoming", methods=["GET"])
def current_or_upcoming_challenge(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge = _current_or_upcoming_challenge()
        if not challenge:
            raise ApiError(404, "No active or upcoming challenge found")
        return _json_response(_enrich_challenge(challenge))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges", methods=["POST"])
def create_challenge(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        return _json_response(_create_challenge(ChallengeCreate.model_validate(_get_json_body(req))), 201)
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}", methods=["GET"])
def get_challenge(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge = _get_challenge_document(_route_param(req, "challenge_id"))
        if not challenge:
            raise ApiError(404, "Challenge not found")
        return _json_response(_enrich_challenge(challenge))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}", methods=["PATCH"])
def patch_challenge(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        existing = _get_challenge_document(challenge_id)
        if not existing:
            raise ApiError(404, "Challenge not found")
        patch = ChallengePatch.model_validate(_get_json_body(req))
        updated = {**existing, **_model_payload(patch, exclude_unset=True), "updatedAt": _utc_now()}
        _ensure_date_order(updated["startDate"], updated["endDate"])
        return _json_response(_enrich_challenge(_get_competition_container().upsert_item(updated)))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/settings", methods=["GET"])
def challenge_settings(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge = _get_challenge_document(_route_param(req, "challenge_id"))
        if not challenge:
            raise ApiError(404, "Challenge not found")
        return _json_response(
            {
                "challengeID": challenge["challengeID"],
                "rules": _normalise_challenge_rules(challenge.get("rules")),
                "forfeits": challenge.get("forfeits", {}),
                "forfeitDetails": challenge.get("forfeits", {}),
                "scoringVersion": challenge.get("scoringVersion"),
                "weekStartsOn": challenge.get("weekStartsOn"),
                "timezone": challenge.get("timezone"),
            }
        )
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/participants", methods=["GET"])
def list_participants(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        participants = _query_items(
            """
            SELECT * FROM c
            WHERE c.type = @type AND c.challengeID = @challengeID
            ORDER BY c.userID
            """,
            [
                {"name": "@type", "value": "challenge_participant"},
                {"name": "@challengeID", "value": challenge_id},
            ],
        )
        return _json_response({"participants": [_strip_cosmos_fields(item) for item in participants]})
    except Exception as exc:
        return _handle_competition_error(exc)


def _sync_challenge_participant(challenge: dict[str, Any], user_id: str, include: bool) -> dict[str, Any]:
    participants = challenge.get("participants", [])
    if not isinstance(participants, list):
        participants = []
    if include and user_id not in participants:
        participants.append(user_id)
    if not include:
        participants = [item for item in participants if item != user_id]
    challenge["participants"] = participants
    challenge["updatedAt"] = _utc_now()
    return _get_competition_container().upsert_item(challenge)


@app.route(route="challenges/{challenge_id}/participants", methods=["POST"])
def add_participant(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        challenge = _get_challenge_document(challenge_id)
        if not challenge:
            raise ApiError(404, "Challenge not found")
        payload = ParticipantCreate.model_validate(_get_json_body(req))
        user = _get_user_document(payload.userID)
        if not user:
            raise ApiError(404, "User not found")
        now = _utc_now()
        existing = _get_participant_document(challenge_id, payload.userID)
        participant = {
            **(existing or {}),
            "id": _participant_doc_id(challenge_id, payload.userID),
            "type": "challenge_participant",
            "challengeID": challenge_id,
            "participantId": _participant_doc_id(challenge_id, payload.userID),
            "userID": payload.userID,
            "active": payload.active,
            "joinedAt": (existing or {}).get("joinedAt", now),
            "updatedAt": now,
        }
        result = _get_competition_container().upsert_item(participant)
        _sync_challenge_participant(challenge, payload.userID, include=True)
        return _json_response(_strip_cosmos_fields(result), 201)
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/participants/{user_id}", methods=["PATCH"])
def patch_participant(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        user_id = _route_param(req, "user_id")
        participant = _get_participant_document(challenge_id, user_id)
        if not participant:
            raise ApiError(404, "Participant not found")
        patch = ParticipantPatch.model_validate(_get_json_body(req))
        updated = {**participant, **_model_payload(patch, exclude_unset=True), "updatedAt": _utc_now()}
        return _json_response(_strip_cosmos_fields(_get_competition_container().upsert_item(updated)))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/participants/{user_id}", methods=["DELETE"])
def remove_participant(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        user_id = _route_param(req, "user_id")
        challenge = _get_challenge_document(challenge_id)
        participant = _get_participant_document(challenge_id, user_id)
        if not participant:
            raise ApiError(404, "Participant not found")
        participant["active"] = False
        participant["updatedAt"] = _utc_now()
        result = _get_competition_container().upsert_item(participant)
        if challenge:
            _sync_challenge_participant(challenge, user_id, include=False)
        return _json_response(_strip_cosmos_fields(result))
    except Exception as exc:
        return _handle_competition_error(exc)


def _leaderboard_query(
    challenge_id: str, kind: str | None = None, latest: bool = False
) -> list[dict[str, Any]]:
    allowed_kinds = {"week", "month", "final", "current"}
    if kind is not None and kind not in allowed_kinds:
        raise ApiError(400, "kind must be week, month, final, or current")

    query_kind = kind
    items = _query_published_leaderboards(challenge_id, query_kind)
    messages = _query_leaderboard_messages(challenge_id, query_kind)
    enriched = _attach_leaderboard_messages(items, messages)
    if latest:
        if enriched:
            return enriched[:1]
        return []
    return enriched


def _query_published_leaderboards(challenge_id: str, kind: str | None = None) -> list[dict[str, Any]]:
    items = _query_items(
        """
        SELECT * FROM c
        WHERE c.challengeID = @challengeID OR c.challengeId = @challengeID
        """,
        [{"name": "@challengeID", "value": challenge_id}],
    )

    filtered = []
    for item in items:
        if not _is_leaderboard_document(item):
            continue
        item_kind = _leaderboard_kind(item)
        if kind is None and item_kind not in LEADERBOARD_KINDS:
            continue
        if kind is not None and item_kind != kind:
            continue
        status = item.get("status")
        if status is not None and status != "published":
            continue
        filtered.append(item)

    filtered.sort(key=_leaderboard_sort_key, reverse=True)
    return filtered


def _query_leaderboard_messages(
    challenge_id: str | None = None,
    kind: str | None = None,
    leaderboard_id: str | None = None,
    ai_message_id: str | None = None,
) -> list[dict[str, Any]]:
    parameters = [{"name": "@type", "value": LEADERBOARD_MESSAGE_TYPE}]
    filters = ["c.type = @type"]
    if ai_message_id is not None:
        filters.append("c.id = @id")
        parameters.append({"name": "@id", "value": ai_message_id})
    if challenge_id is not None:
        filters.append("(c.challengeID = @challengeID OR c.challengeId = @challengeID)")
        parameters.append({"name": "@challengeID", "value": challenge_id})
    if kind is not None:
        filters.append("c.leaderboardKind = @kind")
        parameters.append({"name": "@kind", "value": kind})
    if leaderboard_id is not None:
        filters.append("c.leaderboardId = @leaderboardId")
        parameters.append({"name": "@leaderboardId", "value": leaderboard_id})

    items = _query_items(
        f"SELECT * FROM c WHERE {' AND '.join(filters)}",
        parameters,
    )
    filtered = [_strip_ai_message(item) for item in items if _is_allowed_ai_message(item, kind)]
    filtered.sort(key=_leaderboard_sort_key, reverse=True)
    return filtered


def _is_leaderboard_document(document: dict[str, Any]) -> bool:
    doc_type = document.get("type")
    return doc_type == "leaderboard" or doc_type in PUBLISHED_LEADERBOARD_TYPES


def _leaderboard_kind(document: dict[str, Any]) -> str | None:
    kind = document.get("leaderboardKind") or document.get("kind")
    if kind:
        return kind
    for candidate_kind, doc_type in LEADERBOARD_TYPES_BY_KIND.items():
        if document.get("type") == doc_type:
            return candidate_kind
    return None


def _leaderboard_sort_key(document: dict[str, Any]) -> str:
    return (
        document.get("periodEndDate")
        or document.get("periodEnd")
        or document.get("generatedAt")
        or document.get("publishedAt")
        or document.get("periodStartDate")
        or document.get("periodStart")
        or ""
    )


def _is_allowed_ai_message(document: dict[str, Any], kind: str | None = None) -> bool:
    if kind is not None and document.get("leaderboardKind") != kind:
        return False
    channel = document.get("channel")
    if channel is not None and channel != "app":
        return False
    status = document.get("status")
    return status in (None, "generated", "published")


def _strip_ai_message(document: dict[str, Any]) -> dict[str, Any]:
    message = _strip_cosmos_fields(document)
    if "challengeID" not in message and "challengeId" in message:
        message["challengeID"] = message["challengeId"]
    return message


def _attach_leaderboard_messages(
    leaderboards: list[dict[str, Any]], messages: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    by_leaderboard_id = {
        message.get("leaderboardId"): message
        for message in messages
        if message.get("leaderboardId")
    }
    by_message_id = {message.get("id"): message for message in messages if message.get("id")}

    enriched = []
    for item in leaderboards:
        leaderboard = _strip_leaderboard(item)
        message = by_message_id.get(leaderboard.get("aiMessageId")) or by_leaderboard_id.get(
            leaderboard.get("id")
        ) or by_leaderboard_id.get(leaderboard.get("leaderboardID"))
        if not message:
            continue
        leaderboard["aiMessageId"] = message["id"]
        leaderboard["message"] = message.get("message", "")
        leaderboard["aiMessage"] = message
        enriched.append(leaderboard)
    return enriched


def _strip_leaderboard(document: dict[str, Any]) -> dict[str, Any]:
    leaderboard = _strip_cosmos_fields(document)
    if "leaderboardID" not in leaderboard and "id" in leaderboard:
        leaderboard["leaderboardID"] = leaderboard["id"]
    if "challengeID" not in leaderboard and "challengeId" in leaderboard:
        leaderboard["challengeID"] = leaderboard["challengeId"]
    if "leaderboardKind" not in leaderboard:
        kind = _leaderboard_kind(leaderboard)
        if kind:
            leaderboard["leaderboardKind"] = kind
    if "kind" not in leaderboard and leaderboard.get("leaderboardKind"):
        leaderboard["kind"] = leaderboard["leaderboardKind"]
    if "generatedAt" not in leaderboard and "publishedAt" in leaderboard:
        leaderboard["generatedAt"] = leaderboard["publishedAt"]
    if "rows" not in leaderboard and "entries" in leaderboard:
        leaderboard["rows"] = leaderboard["entries"]
    if "rows" not in leaderboard:
        leaderboard["rows"] = []
    return leaderboard


def _numeric_field(document: dict[str, Any], field_names: tuple[str, ...]) -> float | None:
    for field_name in field_names:
        value = document.get(field_name)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _raw_user_id(document: dict[str, Any]) -> str | None:
    value = document.get("userID") or document.get("user_id")
    return value if isinstance(value, str) and value else None


def _raw_date(document: dict[str, Any]) -> str | None:
    value = document.get("date") or document.get("localDate")
    return value if isinstance(value, str) else None


def _stats_period_bounds(challenge: dict[str, Any], period: str, today_date: str) -> tuple[str, str]:
    challenge_start = _to_date(challenge["startDate"])
    challenge_end = _to_date(challenge["endDate"])
    today = _to_date(today_date)
    effective_end = challenge_end if challenge.get("status") == "completed" else min(today, challenge_end)

    if period == "challenge":
        start = challenge_start
        end = effective_end
    elif period == "month":
        month_start = today.replace(day=1)
        start = max(challenge_start, month_start)
        end = effective_end
    elif period == "week":
        days_since_sunday = (today.weekday() + 1) % 7
        week_start = today - timedelta(days=days_since_sunday)
        start = max(challenge_start, week_start)
        end = effective_end
    else:
        raise ApiError(400, "period must be week, month, or challenge")

    if end < start:
        end = start
    return start.isoformat(), end.isoformat()


def _stats_buckets(start_date: str, end_date: str, period: str) -> list[dict[str, str]]:
    dates = _date_range(start_date, end_date)
    if period == "week":
        return [{"label": _date_label(date), "start": date, "end": date} for date in dates]

    buckets = []
    start = _to_date(start_date)
    end = _to_date(end_date)
    cursor = start
    index = 1
    while cursor <= end:
        bucket_end = min(cursor + timedelta(days=6), end)
        buckets.append(
            {
                "label": f"W{index}",
                "start": cursor.isoformat(),
                "end": bucket_end.isoformat(),
            }
        )
        cursor = bucket_end + timedelta(days=1)
        index += 1
    return buckets


def _query_raw_stats_rows(user_ids: list[str], start_date: str, end_date: str) -> list[dict[str, Any]]:
    if not user_ids:
        return []
    return list(
        _get_container().query_items(
            query="""
            SELECT * FROM c
            WHERE (ARRAY_CONTAINS(@userIDs, c.userID) OR ARRAY_CONTAINS(@userIDs, c.user_id))
              AND c.date >= @startDate
              AND c.date <= @endDate
            """,
            parameters=[
                {"name": "@userIDs", "value": user_ids},
                {"name": "@startDate", "value": start_date},
                {"name": "@endDate", "value": end_date},
            ],
            enable_cross_partition_query=True,
        )
    )


def _participant_profiles(user_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not user_ids:
        return {}
    profiles = _query_items(
        "SELECT * FROM c WHERE c.type = @type AND ARRAY_CONTAINS(@userIDs, c.userID)",
        [{"name": "@type", "value": "user"}, {"name": "@userIDs", "value": user_ids}],
    )
    return {profile["userID"]: _strip_user_document(profile) for profile in profiles}


def _stats_value_row(label: str, participants: list[str]) -> dict[str, Any]:
    return {"label": label, **{participant: 0 for participant in participants}}


def _average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0


def _round_stat(value: float) -> int | float:
    rounded = round(value, 2)
    return int(rounded) if rounded == int(rounded) else rounded


def _build_stats_response(challenge: dict[str, Any], period: str, today_date: str) -> dict[str, Any]:
    user_ids = [item for item in challenge.get("participants", []) if isinstance(item, str)]
    profiles = _participant_profiles(user_ids)
    display_by_user = {
        user_id: profiles.get(user_id, {}).get("displayName") or user_id for user_id in user_ids
    }
    participants = [display_by_user[user_id] for user_id in user_ids]
    calorie_targets = {
        user_id: profiles.get(user_id, {}).get("averageDailyCalorieTarget") or 0 for user_id in user_ids
    }
    start_date, end_date = _stats_period_bounds(challenge, period, today_date)
    buckets = _stats_buckets(start_date, end_date, period)
    raw_rows = _query_raw_stats_rows(user_ids, start_date, end_date)

    by_user_date: dict[str, dict[str, dict[str, Any]]] = {
        user_id: {date: {"active": 0.0, "calories": [], "weights": []} for date in _date_range(start_date, end_date)}
        for user_id in user_ids
    }

    for row in raw_rows:
        user_id = _raw_user_id(row)
        date = _raw_date(row)
        if user_id not in by_user_date or date not in by_user_date[user_id]:
            continue
        active = _numeric_field(
            row,
            (
                "active_energy_kcal",
                "activeEnergyKcal",
                "activeCalories",
                "active_calories",
                "active_kcal",
            ),
        )
        calories = _numeric_field(
            row,
            (
                "calories",
                "caloriesKcal",
                "calorie_kcal",
                "foodCaloriesKcal",
                "totalCalories",
                "consumedCalories",
            ),
        )
        weight = _numeric_field(
            row,
            ("weightKg", "weight_kg", "bodyWeightKg", "body_weight_kg", "weight"),
        )
        if active is not None:
            by_user_date[user_id][date]["active"] += active
        if calories is not None:
            by_user_date[user_id][date]["calories"].append(calories)
        if weight is not None:
            by_user_date[user_id][date]["weights"].append(weight)

    weight_change = []
    calorie_adherence = []
    active_calories = []
    food_logging_days = {display_by_user[user_id]: 0 for user_id in user_ids}
    weigh_in_days = {display_by_user[user_id]: 0 for user_id in user_ids}

    first_weights: dict[str, float | None] = {}
    for user_id in user_ids:
        first_weights[user_id] = None
        for date in _date_range(start_date, end_date):
            weights = by_user_date[user_id][date]["weights"]
            if weights:
                first_weights[user_id] = weights[-1]
                break

    for bucket in buckets:
        weight_row = _stats_value_row(bucket["label"], participants)
        calorie_row = _stats_value_row(bucket["label"], participants)
        active_row = _stats_value_row(bucket["label"], participants)
        bucket_dates = _date_range(bucket["start"], bucket["end"])

        for user_id in user_ids:
            display_name = display_by_user[user_id]
            bucket_active = 0.0
            calorie_variances = []
            latest_weight = None
            for date in bucket_dates:
                day = by_user_date[user_id][date]
                bucket_active += day["active"]
                if day["calories"]:
                    calories = sum(day["calories"])
                    target = calorie_targets[user_id]
                    calorie_variances.append(calories - target if target else 0)
                    food_logging_days[display_name] += 1
                if day["weights"]:
                    latest_weight = day["weights"][-1]
                    weigh_in_days[display_name] += 1

            baseline = first_weights[user_id]
            if baseline and latest_weight:
                weight_row[display_name] = _round_stat(((latest_weight - baseline) / baseline) * 100)
            calorie_row[display_name] = _round_stat(_average(calorie_variances))
            active_row[display_name] = _round_stat(bucket_active)

        weight_change.append(weight_row)
        calorie_adherence.append(calorie_row)
        active_calories.append(active_row)

    return {
        "period": period,
        "participants": participants,
        "weightChangePct": weight_change,
        "calorieAdherence": calorie_adherence,
        "foodLoggingDays": food_logging_days,
        "activeCalories": active_calories,
        "weighInDays": weigh_in_days,
    }


def _empty_leaderboard(challenge_id: str, kind: str) -> dict[str, Any]:
    return {
        "leaderboardID": f"empty_{challenge_id}_{kind}",
        "challengeID": challenge_id,
        "kind": "current" if kind == "week" else kind,
        "periodLabel": "Running tally" if kind == "week" else kind.title(),
        "generatedAt": _utc_now(),
        "rows": [],
    }


@app.route(route="challenges/{challenge_id}/leaderboards/latest", methods=["GET"])
def latest_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        kind = req.params.get("kind", "week")
        items = _leaderboard_query(challenge_id, kind, latest=True)
        if not items:
            return _json_response(_empty_leaderboard(challenge_id, kind))
        return _json_response(_strip_leaderboard(items[0]))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/leaderboard-messages", methods=["GET"])
def list_leaderboard_messages(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        kind = req.params.get("kind")
        if kind is not None and kind not in LEADERBOARD_KINDS:
            raise ApiError(400, "kind must be week, month, or final")
        return _json_response({"messages": _query_leaderboard_messages(challenge_id, kind)})
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="leaderboard-messages/{ai_message_id}", methods=["GET"])
def get_leaderboard_message(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        ai_message_id = _route_param(req, "ai_message_id")
        messages = _query_leaderboard_messages(ai_message_id=ai_message_id)
        if not messages:
            raise ApiError(404, "Leaderboard message not found")
        return _json_response(messages[0])
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/leaderboards/{leaderboard_id}/message", methods=["GET"])
def get_leaderboard_message_for_leaderboard(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        leaderboard_id = _route_param(req, "leaderboard_id")
        messages = _query_leaderboard_messages(challenge_id=challenge_id, leaderboard_id=leaderboard_id)
        if not messages:
            raise ApiError(404, "Leaderboard message not found")
        return _json_response(messages[0])
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/leaderboards", methods=["GET"])
def list_leaderboards(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        kind = req.params.get("kind")
        return _json_response(
            {
                "leaderboards": [
                    _strip_leaderboard(item)
                    for item in _leaderboard_query(challenge_id, kind, latest=False)
                ]
            }
        )
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/scores", methods=["GET"])
def list_scores(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        user_id = req.params.get("userID")
        if user_id:
            query = """
                SELECT * FROM c
                WHERE c.type = @type AND c.challengeID = @challengeID AND c.userID = @userID
                ORDER BY c.date DESC
            """
            parameters = [
                {"name": "@type", "value": "score"},
                {"name": "@challengeID", "value": challenge_id},
                {"name": "@userID", "value": user_id},
            ]
        else:
            query = """
                SELECT * FROM c
                WHERE c.type = @type AND c.challengeID = @challengeID
                ORDER BY c.date DESC
            """
            parameters = [
                {"name": "@type", "value": "score"},
                {"name": "@challengeID", "value": challenge_id},
            ]
        return _json_response({"scores": [_strip_cosmos_fields(item) for item in _query_items(query, parameters)]})
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="challenges/{challenge_id}/stats", methods=["GET"])
def challenge_stats(req: func.HttpRequest) -> func.HttpResponse:
    try:
        _check_bearer_token(req)
        challenge_id = _route_param(req, "challenge_id")
        period = req.params.get("period", "week")
        if period not in {"week", "month", "challenge"}:
            raise ApiError(400, "period must be week, month, or challenge")
        today_date = (
            _parse_local_date(req.params.get("todayDate"), "todayDate")
            if req.params.get("todayDate")
            else datetime.now(timezone.utc).date().isoformat()
        )
        challenge = _get_challenge_document(challenge_id)
        if not challenge:
            raise ApiError(404, "Challenge not found")
        return _json_response(_build_stats_response(challenge, period, today_date))
    except Exception as exc:
        return _handle_competition_error(exc)


@app.route(route="status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def status(req: func.HttpRequest) -> func.HttpResponse:
    return _json_response({"ok": True})
