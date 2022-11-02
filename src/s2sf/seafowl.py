import json
import logging
import math
from datetime import datetime
from numbers import Number
from typing import Any, Optional

import requests

SEAFOWL_SCHEMA = "socrata"
SEAFOWL_TABLE = "dataset_history"

logger = logging.getLogger(__name__)


def emit_value(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, float):
        if math.isnan(value):
            return "NULL"
        return f"{value:.20f}"

    if isinstance(value, Number) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    quoted = str(value).replace("'", "''")
    return f"'{quoted}'"


def query_seafowl(endpoint: str, sql: str, access_token: Optional[str] = None) -> Any:
    headers = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    response = requests.post(f"{endpoint}/q", json={"query": sql}, headers=headers)

    if not response.ok:
        logger.error(response.text)
    response.raise_for_status()
    if response.text:
        return [json.loads(t) for t in response.text.strip().split("\n")]
    return None
