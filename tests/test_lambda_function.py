from datetime import date

from lambda_function import (
    Race,
    _dedupe_and_filter,
    _extract_entry_requirements,
    _parse_jsonld,
    _parse_aims_calendar,
    _parse_world_marathon_majors,
    _should_visit_link,
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


def test_parse_world_marathon_majors_extracts_location_fields():
    html = """
    <script>
    window.__data = {
      "name": "Example City Marathon",
      "date_start": "2031-09-14",
      "city": "Example City",
      "country": "Exampleland",
      "url": "https://www.worldmarathonmajors.com/example"
    };
    </script>
    """

    races = _parse_world_marathon_majors(html, "https://www.worldmarathonmajors.com")

    assert len(races) == 1
    assert races[0].date == "2031-09-14"
    assert races[0].location == "Example City, Exampleland"


def test_parse_aims_calendar_extracts_table_rows():
    html = """
    <table>
      <tr><th>Date</th><th>Race</th><th>City</th><th>Country</th></tr>
      <tr>
        <td>14 Sep 2032</td>
        <td><a href="https://example.com">Example Marathon</a></td>
        <td>Example City</td>
        <td>Exampleland</td>
      </tr>
    </table>
    """

    races = _parse_aims_calendar(html, "https://aims-worldrunning.org/calendar.html")

    assert len(races) == 1
    assert races[0].name == "Example Marathon"
    assert races[0].date == "2032-09-14"
    assert races[0].location == "Example City, Exampleland"


def test_should_visit_link_skips_blocked_paths():
    assert not _should_visit_link("example.com", "https://example.com/result.uri")
    assert not _should_visit_link("example.com", "https://example.com/archived/majors-v1")
    assert not _should_visit_link("example.com", "https://example.com/assets/guide.pdf")
