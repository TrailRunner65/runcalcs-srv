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
