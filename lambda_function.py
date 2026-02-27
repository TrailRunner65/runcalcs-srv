import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

TIP_CATEGORIES = [
    "running equipment",
    "health",
    "training",
    "rest",
    "recovery from injury",
    "nutrition",
    "mental wellbeing",
    "weight loss",
    "racing",
    "club running",
    "Parkruns",
]

ALLOWED_ORIGINS = [
    "https://runcalcs.com",
    "https://www.runcalcs.com",
    "https://develop.d39l2wzc9rmkuy.amplifyapp.com",
]


@dataclass
class RunningTip:
    category: str
    tip: str
    model: str
    generated_at: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "category": self.category,
            "tip": self.tip,
            "model": self.model,
            "generated_at": self.generated_at,
        }


def _iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from _iter_strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _iter_strings(nested)


def _load_openai_key(secrets_client: Any, secret_name: str) -> str:
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString", "")
    if not secret_string:
        raise ValueError(f"Secret '{secret_name}' is empty")

    try:
        parsed = json.loads(secret_string)
    except json.JSONDecodeError:
        return secret_string.strip()

    for key_name in (
        "OPENAI_API_KEY",
        "openai_api_key",
        "api_key",
        "key",
        "ChatGPTKey",
        "chatgptkey",
        "chatgpt_key",
    ):
        value = parsed.get(key_name)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for candidate in _iter_strings(parsed):
        cleaned = candidate.strip()
        if cleaned.startswith("sk-"):
            return cleaned

    raise ValueError(
        f"Secret '{secret_name}' JSON does not contain an OpenAI API key. "
        "Expected a plain key value or one of: OPENAI_API_KEY, openai_api_key, api_key, key, ChatGPTKey"
    )


def _choose_category(event: Dict[str, Any]) -> str:
    requested = event.get("category")
    if isinstance(requested, str) and requested in TIP_CATEGORIES:
        return requested
    return random.choice(TIP_CATEGORIES)


def _request_openai_tip(api_key: str, category: str, model: str, timeout_seconds: int) -> str:
    prompt = (
        "You are a practical running coach. "
        f"Provide one concise tip of the day for the category '{category}'. "
        "Keep it to 1-2 sentences and make it actionable."
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You provide safe, clear running advice."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.7,
    }

    request = Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as exc:
        logger.error("OpenAI request failed: %s", exc)
        raise

    choices = body.get("choices", [])
    if not choices:
        raise ValueError("OpenAI response contained no choices")

    message = choices[0].get("message", {})
    content = message.get("content", "")
    tip = content.strip()
    if not tip:
        raise ValueError("OpenAI response did not contain tip text")

    return tip


def _ensure_bucket(s3_client: Any, bucket: str, region_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

        params: Dict[str, Any] = {"Bucket": bucket}
        if region_name != "us-east-1":
            params["CreateBucketConfiguration"] = {"LocationConstraint": region_name}
        s3_client.create_bucket(**params)


def _configure_bucket_cors(s3_client: Any, bucket: str) -> None:
    s3_client.put_bucket_cors(
        Bucket=bucket,
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedMethods": ["GET"],
                    "AllowedOrigins": ALLOWED_ORIGINS,
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3600,
                }
            ]
        },
    )


def _build_dated_key(key_prefix: str, run_at: datetime) -> str:
    cleaned = key_prefix.strip().strip("/") or "running-tips/tip"
    return f"{cleaned}-{run_at.strftime('%Y-%m-%d')}.json"


def _store_tip(s3_client: Any, bucket: str, key: str, tip: RunningTip) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(tip.to_dict(), indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event: Optional[Dict[str, Any]], context: Any) -> Dict[str, Any]:
    import boto3

    payload = event or {}
    region_name = os.getenv("AWS_REGION", "ap-southeast-2")
    secret_name = os.getenv("OPENAI_SECRET_NAME", "ChatGPTKey")
    key_prefix = os.getenv("TIPS_KEY_PREFIX", "running-tips/tip")
    bucket = os.environ["TIPS_BUCKET"]
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    timeout_seconds = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))

    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    s3_client = boto3.client("s3", region_name=region_name)

    api_key = _load_openai_key(secrets_client, secret_name)
    category = _choose_category(payload)
    run_at = datetime.now(timezone.utc)

    tip_text = _request_openai_tip(api_key, category, model, timeout_seconds)
    tip = RunningTip(
        category=category,
        tip=tip_text,
        model=model,
        generated_at=run_at.isoformat(),
    )

    _ensure_bucket(s3_client, bucket, region_name)
    _configure_bucket_cors(s3_client, bucket)
    key = _build_dated_key(key_prefix, run_at)
    _store_tip(s3_client, bucket, key, tip)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "bucket": bucket,
                "key": key,
                "category": category,
                "model": model,
            }
        ),
    }
