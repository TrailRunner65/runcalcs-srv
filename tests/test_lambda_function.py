import json
from datetime import datetime, timezone
from types import SimpleNamespace

import lambda_function
from lambda_function import (
    ALLOWED_ORIGINS,
    TIP_CATEGORIES,
    RunningTip,
    _build_dated_key,
    _choose_category,
    _configure_bucket_cors,
    _ensure_bucket,
    _load_openai_key,
)


class FakeBody:
    def __init__(self, payload: dict):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_load_openai_key_from_plain_secret_value():
    class FakeSecrets:
        def get_secret_value(self, SecretId):
            assert SecretId == "ChatGPTKey"
            return {"SecretString": "sk-test-key"}

    key = _load_openai_key(FakeSecrets(), "ChatGPTKey", "ap-southeast-2")
    assert key == "sk-test-key"


def test_load_openai_key_from_json_secret_value():
    class FakeSecrets:
        def get_secret_value(self, SecretId):
            return {"SecretString": '{"OPENAI_API_KEY": "sk-json"}'}

    key = _load_openai_key(FakeSecrets(), "ChatGPTKey", "ap-southeast-2")
    assert key == "sk-json"


def test_choose_category_uses_event_value_when_valid():
    assert _choose_category({"category": "nutrition"}) == "nutrition"


def test_choose_category_falls_back_to_known_list():
    category = _choose_category({"category": "invalid"})
    assert category in TIP_CATEGORIES


def test_ensure_bucket_creates_when_missing():
    class MissingBucketError(Exception):
        def __init__(self):
            self.response = {"Error": {"Code": "404"}}

    class FakeS3:
        def __init__(self):
            self.created = None

        def head_bucket(self, Bucket):
            raise MissingBucketError()

        def create_bucket(self, **kwargs):
            self.created = kwargs

    fake_s3 = FakeS3()
    _ensure_bucket(fake_s3, "tips-bucket", "ap-southeast-2")

    assert fake_s3.created == {
        "Bucket": "tips-bucket",
        "CreateBucketConfiguration": {"LocationConstraint": "ap-southeast-2"},
    }


def test_configure_bucket_cors_sets_expected_origins():
    class FakeS3:
        def __init__(self):
            self.cors = None

        def put_bucket_cors(self, **kwargs):
            self.cors = kwargs

    fake_s3 = FakeS3()
    _configure_bucket_cors(fake_s3, "tips-bucket")

    rule = fake_s3.cors["CORSConfiguration"]["CORSRules"][0]
    assert rule["AllowedOrigins"] == ALLOWED_ORIGINS


def test_build_dated_key_has_date_suffix():
    key = _build_dated_key("running-tips/tip", datetime(2026, 2, 27, tzinfo=timezone.utc))
    assert key == "running-tips/tip-2026-02-27.json"


def test_lambda_handler_generates_and_stores_tip(monkeypatch):
    class FakeSecrets:
        def get_secret_value(self, SecretId):
            return {"SecretString": "sk-test-key"}

    class FakeS3:
        def __init__(self):
            self.put_calls = []
            self.cors_calls = []

        def head_bucket(self, Bucket):
            return {}

        def put_bucket_cors(self, **kwargs):
            self.cors_calls.append(kwargs)

        def put_object(self, **kwargs):
            self.put_calls.append(kwargs)

    fake_s3 = FakeS3()

    monkeypatch.setenv("TIPS_BUCKET", "tips-bucket")
    monkeypatch.setenv("TIPS_KEY_PREFIX", "running-tips/tip")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o-mini")
    monkeypatch.setattr(lambda_function, "datetime", SimpleNamespace(now=lambda tz: datetime(2026, 2, 27, tzinfo=timezone.utc)))
    monkeypatch.setattr(lambda_function, "_request_openai_tip", lambda *args, **kwargs: "Hydrate after your easy run.")

    import sys

    def fake_client(service, region_name=None):
        if service == "secretsmanager":
            return FakeSecrets()
        if service == "s3":
            return fake_s3
        raise AssertionError(service)

    sys.modules["boto3"] = SimpleNamespace(client=fake_client)

    result = lambda_function.lambda_handler({"category": "rest"}, None)
    body = json.loads(result["body"])

    assert body["bucket"] == "tips-bucket"
    assert body["key"] == "running-tips/tip-2026-02-27.json"
    assert body["category"] == "rest"

    stored_payload = json.loads(fake_s3.put_calls[0]["Body"].decode("utf-8"))
    assert stored_payload == RunningTip(
        category="rest",
        tip="Hydrate after your easy run.",
        model="gpt-4o-mini",
        generated_at="2026-02-27T00:00:00+00:00",
    ).to_dict()
