from datetime import date

from lambda_function import (
    Race,
    _dedupe_and_filter,
    _extract_entry_requirements,
    _parse_jsonld,
    _parse_fallback_races,
    curated_major_marathons,
)


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
    assert races[0].date_start == "2030-04-12"
    assert "lottery" in races[0].entry_requirements


def test_dedupe_and_filter_removes_duplicates_and_past_dates():
    races = [
        Race(
            name="City Marathon",
            date_start="2020-01-01",
            date_end=None,
            city="Boston",
            region=None,
            country="US",
            lat=None,
            lng=None,
            distance_km=42.195,
            website_url="https://a.example",
            source="Example",
            source_event_id=None,
            description="Old race",
            entry_requirements="Not specified",
            last_seen_at="2024-01-01T00:00:00+00:00",
            last_verified_at=None,
            status="scheduled",
        ),
        Race(
            name="City Marathon",
            date_start="2030-01-01",
            date_end=None,
            city="Boston",
            region=None,
            country="US",
            lat=None,
            lng=None,
            distance_km=42.195,
            website_url="https://a.example",
            source="Example",
            source_event_id=None,
            description="Upcoming race",
            entry_requirements="Not specified",
            last_seen_at="2024-01-01T00:00:00+00:00",
            last_verified_at=None,
            status="scheduled",
        ),
        Race(
            name=" City  Marathon ",
            date_start="2030-01-02",
            date_end=None,
            city="Boston",
            region=None,
            country="US",
            lat=None,
            lng=None,
            distance_km=42.195,
            website_url="https://a.example",
            source="Example",
            source_event_id=None,
            description="Duplicate upcoming race",
            entry_requirements="Not specified",
            last_seen_at="2024-01-01T00:00:00+00:00",
            last_verified_at=None,
            status="scheduled",
        ),
    ]

    filtered = _dedupe_and_filter(races, today=date(2024, 1, 1))

    assert len(filtered) == 1
    assert filtered[0].date_start == "2030-01-01"


def test_extract_entry_requirements():
    text = "Participants must meet a qualification time and pay an entry fee."
    requirements = _extract_entry_requirements(text)

    assert "qualification standard" in requirements
    assert "entry fee" in requirements


def test_parse_fallback_races_extracts_marathon_and_date():
    html = """
    <div>2029-05-22 Spring City Marathon</div>
    <div>Jun 3, 2031 - Lakeside Marathon Weekend</div>
    """
    races = _parse_fallback_races(html, "https://example.com")

    assert len(races) == 2
    assert races[0].date_start == "2029-05-22"
    assert "marathon" in races[0].name.lower()
    assert races[1].date_start == "2031-06-03"


def test_parse_fallback_races_skips_half_marathon():
    html = """
    <div>2030-06-10 Spring Half Marathon</div>
    <div>2030-06-11 Spring Marathon</div>
    """
    races = _parse_fallback_races(html, "https://example.com")

    assert len(races) == 1
    assert races[0].name == "Spring Marathon"


def test_location_extraction_from_garbage():
    from lambda_function import _extract_location_from_name_garbage
    
    # Case 1: Simple
    c1, n1 = _extract_location_from_name_garbage('marathon","location":"Cairo, Egypt"')
    assert c1 == "Cairo"
    assert n1 == "Egypt"
    
    # Case 2: Escaped
    c2, n2 = _extract_location_from_name_garbage(r'marathon\",\"location\":\"Paris, France\"')
    assert c2 == "Paris"
    assert n2 == "France"
    
    # Case 3: No location
    c3, n3 = _extract_location_from_name_garbage('marathon","other":"value"')
    assert c3 is None
    assert n3 is None


def test_clean_race_name_strips_json_fragments():
    html = """
    <div>2026-02-06 marathon\\\",\\\"location\\\":\\\"Cairo, Egypt\\\",\\\"photos\\\":[</div>
    """
    races = _parse_fallback_races(html, "https://example.com")

    assert races == []


def test_parse_fallback_races_ignores_jsonish_names():
    html = """
    <div>2026-02-06 marathon","hasStravaItinerary":false</div>
    """
    races = _parse_fallback_races(html, "https://example.com")

    assert races == []


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


def test_curated_major_marathons_includes_six_majors():
    majors = curated_major_marathons()
    names = {race.name for race in majors}

    assert "Tokyo Marathon" in names
    assert "Boston Marathon" in names
    assert "London Marathon" in names
    assert "Berlin Marathon" in names
    assert "Chicago Marathon" in names
    assert "New York City Marathon" in names
