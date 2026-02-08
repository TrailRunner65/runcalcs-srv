import json
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DEFAULT_SEED_URLS = [
    "https://www.ahotu.com/calendar/running/marathon",
    "https://www.runningintheusa.com/classic/list/marathon/upcoming",
    "https://marathons.ahotu.com/calendar/marathon",
    "https://aims-worldrunning.org/calendar.html",
    "https://www.worldmarathonmajors.com",
]


@dataclass
class Race:
    name: str
    date: str
    location: str
    description: str
    entry_requirements: str
    source_url: str

    def to_dict(self) -> Dict[str, str]:
        return self.__dict__.copy()


def _extract_location(location_obj: Any) -> str:
    if isinstance(location_obj, str):
        return location_obj.strip()
    if not isinstance(location_obj, dict):
        return "Unknown"

    address = location_obj.get("address", {})
    parts = [location_obj.get("name")]
    if isinstance(address, str):
        parts.append(address)
    elif isinstance(address, dict):
        parts.extend(
            [
                address.get("addressLocality"),
                address.get("addressRegion"),
                address.get("addressCountry"),
            ]
        )
    cleaned = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return ", ".join(cleaned) if cleaned else "Unknown"


def _extract_entry_requirements(text: str) -> str:
    if not text:
        return "Not specified"
    lowered = text.lower()
    checks = {
        "lottery": r"\blottery\b",
        "qualification standard": r"\bqualif(?:y|ication|ier)\b",
        "membership required": r"\bmember(ship)? required\b",
        "minimum age": r"\b(minimum|min) age\b",
        "entry fee": r"\b(entry fee|registration fee|cost)\b",
        "medical certificate": r"\bmedical certificate\b",
    }
    labels = [label for label, pattern in checks.items() if re.search(pattern, lowered)]
    return ", ".join(labels) if labels else "Not specified"


def _walk_jsonld(payload: Any) -> Iterable[Any]:
    if isinstance(payload, list):
        for item in payload:
            yield from _walk_jsonld(item)
    elif isinstance(payload, dict):
        if "@graph" in payload:
            yield from _walk_jsonld(payload["@graph"])
        yield payload


def _is_event(item: Dict[str, Any]) -> bool:
    value = item.get("@type")
    if isinstance(value, list):
        return any(v in {"Event", "SportsEvent"} for v in value)
    return value in {"Event", "SportsEvent"}


def _normalize_date(raw: Any) -> Optional[str]:
    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip().replace("Z", "+00:00")
    try:
        if len(value) == 10:
            return date.fromisoformat(value).isoformat()
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError:
        match = re.match(r"^(\d{4}-\d{2}-\d{2})", value)
        if match:
            return match.group(1)
    for pattern in ("%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def _extract_jsonld_blobs(html: str) -> List[str]:
    pattern = re.compile(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )
    return [unescape(blob.strip()) for blob in pattern.findall(html)]


def _extract_links(html: str, page_url: str) -> List[str]:
    links = []
    for match in re.findall(r'<a[^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        links.append(urljoin(page_url, unescape(match)).split("#", 1)[0])
    return links


def _strip_html(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(cleaned)).strip()


def _extract_json_string_value(payload: str, key: str) -> Optional[str]:
    pattern = rf'"{re.escape(key)}"\s*:\s*"((?:\\\\.|[^"\\\\])*)"'
    match = re.search(pattern, payload)
    if not match:
        return None
    try:
        return json.loads(f'"{match.group(1)}"')
    except json.JSONDecodeError:
        return match.group(1)


def _extract_tag_attribute(tag: str, attr: str) -> Optional[str]:
    match = re.search(rf'{re.escape(attr)}=["\']([^"\']+)["\']', tag, re.IGNORECASE)
    if match:
        return unescape(match.group(1)).strip()
    return None


def _parse_aims_calendar(doc: str, page_url: str) -> List[Race]:
    if "aims-worldrunning.org" not in urlparse(page_url).netloc:
        return []

    races: List[Race] = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", doc, re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.IGNORECASE | re.DOTALL)
        if len(cells) < 3:
            continue
        values = [_strip_html(cell) for cell in cells]
        start_date = _normalize_date(values[0])
        if not start_date:
            continue
        name = values[1]
        if "marathon" not in name.lower():
            continue
        city = values[2] if len(values) >= 4 else None
        country = values[3] if len(values) >= 4 else (values[2] if len(values) == 3 else None)
        location_parts = [part for part in (city, country) if part]
        location = ", ".join(location_parts) if location_parts else "Unknown"
        races.append(
            Race(
                name=name,
                date=start_date,
                location=location,
                description="AIMS World Running calendar listing",
                entry_requirements="Not specified",
                source_url=page_url,
            )
        )

    if races:
        return races

    for tag in re.findall(r"<[^>]+data-date=[\"'][^\"']+[\"'][^>]*>", doc, re.IGNORECASE):
        date_value = _extract_tag_attribute(tag, "data-date")
        start_date = _normalize_date(date_value) if date_value else None
        name = _extract_tag_attribute(tag, "data-title") or _extract_tag_attribute(tag, "data-name")
        if not start_date or not name or "marathon" not in name.lower():
            continue
        city = _extract_tag_attribute(tag, "data-city")
        country = _extract_tag_attribute(tag, "data-country")
        location_parts = [part for part in (city, country) if part]
        location = ", ".join(location_parts) if location_parts else "Unknown"
        races.append(
            Race(
                name=name,
                date=start_date,
                location=location,
                description="AIMS World Running calendar listing",
                entry_requirements="Not specified",
                source_url=page_url,
            )
        )

    return races


def _parse_world_marathon_majors(doc: str, page_url: str) -> List[Race]:
    if "worldmarathonmajors.com" not in urlparse(page_url).netloc:
        return []

    races: List[Race] = []
    blocks = re.findall(r"\{[^{}]{0,1000}date_start[^{}]{0,1000}\}", doc, re.IGNORECASE | re.DOTALL)
    for block in blocks:
        name = (
            _extract_json_string_value(block, "name")
            or _extract_json_string_value(block, "title")
            or _extract_json_string_value(block, "race_name")
        )
        if not name or "marathon" not in name.lower():
            continue

        date_start = _extract_json_string_value(block, "date_start")
        start_date = _normalize_date(date_start)
        if not start_date:
            continue

        city = _extract_json_string_value(block, "city")
        country = _extract_json_string_value(block, "country")
        location_parts = [part for part in (city, country) if part]
        location = ", ".join(location_parts) if location_parts else "Unknown"
        source_url = _extract_json_string_value(block, "url") or page_url

        races.append(
            Race(
                name=name,
                date=start_date,
                location=location,
                description="World Marathon Majors listing",
                entry_requirements="Not specified",
                source_url=source_url,
            )
        )

    return races


def _parse_jsonld(doc: str, page_url: str) -> List[Race]:
    races: List[Race] = []
    for blob in _extract_jsonld_blobs(doc):
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            continue

        for item in _walk_jsonld(data):
            if not isinstance(item, dict) or not _is_event(item):
                continue
            name = str(item.get("name") or "").strip()
            if not name or "marathon" not in name.lower():
                continue
            start_date = _normalize_date(item.get("startDate"))
            if not start_date:
                continue
            description = str(item.get("description") or "").strip()
            races.append(
                Race(
                    name=name,
                    date=start_date,
                    location=_extract_location(item.get("location")),
                    description=description,
                    entry_requirements=_extract_entry_requirements(description),
                    source_url=str(item.get("url") or page_url),
                )
            )
    return races


def _race_key(race: Race) -> Tuple[str, str, str]:
    norm = lambda x: re.sub(r"\s+", " ", x.lower()).strip()
    return norm(race.name), race.date, norm(race.location)


def _dedupe_and_filter(races: Iterable[Race], today: date) -> List[Race]:
    unique: Dict[Tuple[str, str, str], Race] = {}
    for race in races:
        try:
            race_day = date.fromisoformat(race.date)
        except ValueError:
            continue
        if race_day < today:
            continue
        unique.setdefault(_race_key(race), race)
    return sorted(unique.values(), key=lambda r: (r.date, r.name.lower()))


def _should_visit_link(base_domain: str, href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc and parsed.netloc != base_domain:
        return False
    path = parsed.path.lower()
    if path.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".zip", ".ics", ".uri")):
        return False
    if "archived/majors-v1" in path:
        return False
    lower = href.lower()
    return any(token in lower for token in ("marathon", "race", "calendar"))


def _fetch_url(url: str, timeout_seconds: int) -> Optional[str]:
    request = Request(url, headers={"User-Agent": "marathon-race-crawler/1.0"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return None
            return response.read().decode("utf-8", errors="ignore")
    except HTTPError as exc:
        if exc.code == 403:
            fallback = Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            try:
                with urlopen(fallback, timeout=timeout_seconds) as response:
                    content_type = response.headers.get("Content-Type", "")
                    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                        return None
                    return response.read().decode("utf-8", errors="ignore")
            except (HTTPError, URLError, TimeoutError) as retry_exc:
                logger.warning("Failed fetching %s after retry: %s", url, retry_exc)
                return None
        logger.warning("Failed fetching %s: %s", url, exc)
        return None
    except (URLError, TimeoutError) as exc:
        logger.warning("Failed fetching %s: %s", url, exc)
        return None


def crawl_sources(seed_urls: List[str], max_pages: int = 80, timeout_seconds: int = 15) -> List[Race]:
    queue: deque[str] = deque(seed_urls)
    visited: Set[str] = set()
    races: List[Race] = []

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        html = _fetch_url(url, timeout_seconds)
        if not html:
            continue

        races.extend(_parse_jsonld(html, url))
        races.extend(_parse_aims_calendar(html, url))
        races.extend(_parse_world_marathon_majors(html, url))

        domain = urlparse(url).netloc
        for link in _extract_links(html, url):
            if link not in visited and _should_visit_link(domain, link):
                queue.append(link)

    return races


def load_existing_races(s3_client: Any, bucket: str, key: str) -> List[Race]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code in {"NoSuchKey", "404", "NoSuchBucket"} or "NoSuchKey" in str(exc):
            return []
        raise
    payload = json.loads(obj["Body"].read().decode("utf-8"))
    return [Race(**item) for item in payload.get("races", []) if isinstance(item, dict)]


def store_races(s3_client: Any, bucket: str, key: str, races: List[Race]) -> None:
    body = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(races),
        "races": [r.to_dict() for r in races],
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    import boto3

    bucket = os.environ["RACES_BUCKET"]
    key = os.getenv("RACES_KEY", "races/marathons.json")
    max_pages = int(os.getenv("MAX_PAGES", "80"))
    seed_urls = [
        u.strip() for u in os.getenv("SEED_URLS", ",".join(DEFAULT_SEED_URLS)).split(",") if u.strip()
    ]

    s3_client = boto3.client("s3")
    existing = load_existing_races(s3_client, bucket, key)
    discovered = crawl_sources(seed_urls=seed_urls, max_pages=max_pages)
    filtered = _dedupe_and_filter(existing + discovered, today=datetime.now(timezone.utc).date())
    store_races(s3_client, bucket, key, filtered)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "stored": len(filtered),
                "discovered": len(discovered),
                "existing": len(existing),
                "bucket": bucket,
                "key": key,
            }
        ),
    }
