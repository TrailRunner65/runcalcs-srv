from datetime import date

from lambda_function import Race, _dedupe_and_filter, _extract_entry_requirements, _parse_jsonld


def test_parse_jsonld_extracts_marathon_event():
    html = '''
    <html><head>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Event",
        "name": "City Spring Marathon",
        "startDate": "2030-04-12",
        "description": "Lottery entry with registration fee applies",
        "location": {
          "@type": "Place",
          "name": "Main Square",
          "address": {
            "addressLocality": "Portland",
            "addressRegion": "OR",
            "addressCountry": "US"
          }
        }
      }
      </script>
    </head></html>
    '''

    races = _parse_jsonld(html, "https://example.com")

    assert len(races) == 1
    assert races[0].name == "City Spring Marathon"
    assert races[0].date == "2030-04-12"
    assert "lottery" in races[0].entry_requirements


def test_dedupe_and_filter_removes_duplicates_and_past_dates():
    races = [
        Race(
            name="City Marathon",
            date="2020-01-01",
            location="Boston, US",
            description="Old race",
            entry_requirements="Not specified",
            source_url="https://a.example",
        ),
        Race(
            name="City Marathon",
            date="2030-01-01",
            location="Boston, US",
            description="Upcoming race",
            entry_requirements="Not specified",
            source_url="https://a.example",
        ),
        Race(
            name=" City  Marathon ",
            date="2030-01-01",
            location=" Boston, US ",
            description="Duplicate upcoming race",
            entry_requirements="Not specified",
            source_url="https://b.example",
        ),
    ]

    filtered = _dedupe_and_filter(races, today=date(2024, 1, 1))

    assert len(filtered) == 1
    assert filtered[0].date == "2030-01-01"


def test_extract_entry_requirements():
    text = "Participants must meet a qualification time and pay an entry fee."
    requirements = _extract_entry_requirements(text)

    assert "qualification standard" in requirements
    assert "entry fee" in requirements


def test_lambda_handler_defaults_bucket_name(monkeypatch):
    import sys
    import types
    import lambda_function as lf

    captured = {}

    fake_boto3 = types.SimpleNamespace(client=lambda service: object())
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.delenv("RACES_BUCKET", raising=False)
    monkeypatch.setenv("RACES_KEY", "races/marathons.json")

    monkeypatch.setattr(lf, "load_existing_races", lambda *args, **kwargs: [])
    monkeypatch.setattr(lf, "crawl_sources", lambda *args, **kwargs: [])

    def fake_store(_s3, bucket, _key, _races):
        captured["bucket"] = bucket

    monkeypatch.setattr(lf, "store_races", fake_store)

    result = lf.lambda_handler({}, None)

    assert result["statusCode"] == 200
    assert captured["bucket"] == "runcalcs"
