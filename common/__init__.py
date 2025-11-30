import json
import os
from decimal import Decimal
from typing import Any

PROJECT_NAME = os.environ["PROJECT_NAME"]
STAGE = os.environ["STAGE"]


def resource_name(basename: str) -> str:
    return f"{PROJECT_NAME}-{STAGE}-{basename}"


def response(status_code: int, body: Any):
    raw_body = None

    if body != None:
        if type(body) == str:
            raw_body = body
        else:
            raw_body = to_json(body)

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "body": raw_body,
    }


def json_default(obj: dict):
    if isinstance(obj, Decimal):
        return float(obj)

    raise TypeError


def to_json(obj: dict):
    return json.dumps(obj, default=json_default)
