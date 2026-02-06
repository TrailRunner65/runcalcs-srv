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

DEFAULT_BUCKET_NAME = "runcalcs"

DEFAULT_SEED_URLS = [
    "https://aims-worldrunning.org/calendar/",
    "https://www.ahotu.com/calendar/running/marathon",
    "https://www.runningintheusa.com/classic/list/marathon/upcoming",
    "https://marathons.ahotu.com/calendar/marathon",
    "https://www.worldmarathonmajors.com/races",
    "https://www.baa.org/",
    "https://www.nyrr.org/tcsnycmarathon",
    "https://www.chicagomarathon.com/",
    "https://www.bmw-berlin-marathon.com/en/",
    "https://www.londonmarathon.co.uk/",
    "https://www.tokyo-marathon.org/en/",
]

MAJOR_MARATHONS = [
    {"name": "Tokyo Marathon", "website_url": "https://www.tokyo-marathon.org/en/"},
    {"name": "Boston Marathon", "website_url": "https://www.baa.org/"},
    {"name": "London Marathon", "website_url": "https://www.londonmarathon.co.uk/"},
    {"name": "Berlin Marathon", "website_url": "https://www.bmw-berlin-marathon.com/en/"},
    {"name": "Chicago Marathon", "website_url": "https://www.chicagomarathon.com/"},
    {"name": "New York City Marathon", "website_url": "https://www.nyrr.org/tcsnycmarathon"},
]


@dataclass
class Race:
    name: str
    date_start: Optional[str]
    date_end: Optional[str]
    city: Optional[str]
    region: Optional[str]
    country: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    distance_km: float
    website_url: str
    source: str
    source_event_id: Optional[str]
    description: str
    entry_requirements: str
    last_seen_at: str
    last_verified_at: Optional[str]
    status: str

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


def _extract_location_fields(location_obj: Any) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[float]]:
    if isinstance(location_obj, str):
        return location_obj.strip(), None, None, None, None
    if not isinstance(location_obj, dict):
        return None, None, None, None, None

    address = location_obj.get("address", {})
    city = region = country = None
    if isinstance(address, dict):
        city = address.get("addressLocality")
        region = address.get("addressRegion")
        country = address.get("addressCountry")
    elif isinstance(address, str):
        city = address.strip()

    geo = location_obj.get("geo") if isinstance(location_obj, dict) else None
    lat = lng = None
    if isinstance(geo, dict):
        lat = geo.get("latitude")
        lng = geo.get("longitude")

    return _clean_optional(city), _clean_optional(region), _clean_optional(country), _to_float(lat), _to_float(lng)


def _clean_optional(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                continue
        return None


def _parse_fallback_races(html: str, page_url: str) -> List[Race]:
    races: List[Race] = []
    cleaned_html = _strip_script_style(html)
    text_only = _strip_tags(cleaned_html)
    patterns = [
        re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<name>[^<]{0,120}marathon[^<]{0,120})", re.IGNORECASE),
        re.compile(
            r"(?P<date>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4})\s+(?P<name>[^<]{0,120}marathon[^<]{0,120})",
            re.IGNORECASE,
        ),
    ]

    now = datetime.now(timezone.utc).isoformat()
    source = _source_from_url(page_url)

    for pattern in patterns:
        for line in text_only.splitlines():
            if not line.strip():
                continue
            for match in pattern.finditer(line):
                if "half marathon" in match.group(0).lower():
                    continue
                raw_date = match.group("date")
                name = _clean_race_name(match.group("name"))
                if not name or not _is_full_marathon_name(name):
                    continue
                if _looks_like_json_fragment(name):
                    continue
                if not _is_reasonable_name(name):
                    continue
                normalized_date = _normalize_date(raw_date)
                if not normalized_date:
                    continue

                raw_name = match.group("name")
                garbage_city, garbage_country = _extract_location_from_name_garbage(raw_name)
                name_city = name_country = None
                name = _clean_race_name(raw_name)
                if source == "Ahotu":
                    name, name_city, name_country = _split_location_from_name(name)

                races.append(
                    Race(
                        name=name,
                        date_start=normalized_date,
                        date_end=None,
                        city=garbage_city or name_city,
                        region=None,
                        country=garbage_country or name_country,
                        lat=None,
                        lng=None,
                        distance_km=42.195,
                        website_url=page_url,
                        source=source,
                        source_event_id=None,
                        description="",
                        entry_requirements="Not specified",
                        last_seen_at=now,
                        last_verified_at=None,
                        status="unknown",
                    )
                )

    return races


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


def _parse_jsonld(doc: str, page_url: str) -> List[Race]:
    races: List[Race] = []
    now = datetime.now(timezone.utc).isoformat()
    source = _source_from_url(page_url)
    for blob in _extract_jsonld_blobs(doc):
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            continue

        for item in _walk_jsonld(data):
            if not isinstance(item, dict) or not _is_event(item):
                continue
            name = _clean_race_name(str(item.get("name") or ""))
            if not name or not _is_full_marathon_name(name):
                continue
            start_date = _normalize_date(item.get("startDate"))
            end_date = _normalize_date(item.get("endDate"))
            if not start_date and not end_date:
                continue
            description = str(item.get("description") or "").strip()
            city, region, country, lat, lng = _extract_location_fields(item.get("location"))
            status = _normalize_status(item.get("eventStatus"))
            website_url = str(item.get("url") or page_url)
            source_event_id = _extract_source_event_id(item)
            if source == "Ahotu":
                name, name_city, name_country = _split_location_from_name(name)
                if not city and name_city:
                    city = name_city
                if not country and name_country:
                    country = name_country
            races.append(
                Race(
                    name=name,
                    date_start=start_date or end_date,
                    date_end=end_date,
                    city=city,
                    region=region,
                    country=country,
                    lat=lat,
                    lng=lng,
                    distance_km=42.195,
                    website_url=website_url,
                    source=source,
                    source_event_id=source_event_id,
                    description=description,
                    entry_requirements=_extract_entry_requirements(description),
                    last_seen_at=now,
                    last_verified_at=now,
                    status=status,
                )
            )
    return races


def _race_key(race: Race) -> Tuple[str, Optional[str], Optional[str], Optional[str], str]:
    norm = lambda x: re.sub(r"\s+", " ", x.lower()).strip() if isinstance(x, str) else None
    return (
        norm(race.name),
        norm(race.city),
        norm(race.country),
        _domain_from_url(race.website_url),
        race.date_start,
    )


def _dedupe_and_filter(races: Iterable[Race], today: date) -> List[Race]:
    unique: List[Race] = []

    for race in races:
        race_day = _safe_date(race.date_start)
        if race_day and race_day < today:
            continue

        matched = False
        for existing in unique:
            if _is_same_race(existing, race):
                matched = True
                existing.last_seen_at = race.last_seen_at
                if race.last_verified_at:
                    existing.last_verified_at = race.last_verified_at
                if not existing.website_url and race.website_url:
                    existing.website_url = race.website_url
                break
        if not matched:
            unique.append(race)

    return sorted(unique, key=lambda r: (r.date_start or "9999-12-31", r.name.lower()))


def _safe_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_same_race(left: Race, right: Race) -> bool:
    name_left = _normalize_text(left.name)
    name_right = _normalize_text(right.name)
    if not name_left or not name_right or name_left != name_right:
        return False

    city_left = _normalize_text(left.city)
    city_right = _normalize_text(right.city)
    country_left = _normalize_text(left.country)
    country_right = _normalize_text(right.country)
    if city_left and city_right and city_left != city_right:
        return False
    if country_left and country_right and country_left != country_right:
        return False

    domain_left = _domain_from_url(left.website_url)
    domain_right = _domain_from_url(right.website_url)
    if domain_left and domain_right and domain_left != domain_right:
        return False

    if left.date_start and right.date_start:
        return _dates_within_one_day(left.date_start, right.date_start)

    return True


def _normalize_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"\s+", " ", value.strip().lower())


def _dates_within_one_day(left: Optional[str], right: Optional[str]) -> bool:
    left_date = _safe_date(left)
    right_date = _safe_date(right)
    if not left_date or not right_date:
        return False
    return abs((left_date - right_date).days) <= 1


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _source_from_url(url: str) -> str:
    domain = _domain_from_url(url)
    if "aims-worldrunning.org" in domain:
        return "AIMS"
    if "ahotu.com" in domain:
        return "Ahotu"
    if "worldmarathonmajors.com" in domain:
        return "World Marathon Majors"
    if "runningintheusa.com" in domain:
        return "Running in the USA"
    return domain or "unknown"


def _extract_source_event_id(item: Dict[str, Any]) -> Optional[str]:
    for key in ("@id", "identifier"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            inner = value.get("@id") or value.get("value")
            if isinstance(inner, str) and inner.strip():
                return inner.strip()
    return None


def _normalize_status(raw: Any) -> str:
    if isinstance(raw, str):
        lowered = raw.lower()
        if "cancel" in lowered:
            return "cancelled"
        if "postpon" in lowered:
            return "unknown"
        if "scheduled" in lowered:
            return "scheduled"
    return "unknown"


def _strip_script_style(html: str) -> str:
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def _strip_tags(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html)


def _clean_race_name(raw: str) -> str:
    cleaned = unescape(raw)
    cleaned = re.sub(r"[\n\r\t]+", " ", cleaned)
    cleaned = re.sub(r"\\u[0-9a-fA-F]{4}", " ", cleaned)
    for token in ("{", "}", "\":", "\\\""):
        if token in cleaned:
            cleaned = cleaned.split(token, 1)[0]
    
    for token in ("\\\",", "\","):
        if token in cleaned:
            cleaned = cleaned.split(token, 1)[0]
    cleaned = cleaned.replace("\\", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" \"'")
    if len(cleaned) > 120:
        cleaned = cleaned[:120].strip()
    return cleaned


def _extract_location_from_name_garbage(name: str) -> Tuple[Optional[str], Optional[str]]:
    # pattern captures value after location": or location\":
    # it handles potential quotes around key and value
    # e.g. location":"City, Country" or location\":\"City, Country\"
    match = re.search(r"location\\?\"\\?:\s*\\?\"([^\"]+)\"", name, re.IGNORECASE)
    if not match:
        return None, None
    
    value = match.group(1).strip()
    if value.endswith("\\"):
        value = value[:-1]
    if not value:
        return None, None
        
    parts = [p.strip() for p in value.split(",")]
    city = parts[0] if parts else None
    country = parts[-1] if len(parts) > 1 else None
    
    return city, country


def _split_location_from_name(name: str) -> Tuple[str, Optional[str], Optional[str]]:
    if not name:
        return name, None, None

    candidate = None
    paren_match = re.search(r"\(([^()]+)\)\s*$", name)
    if paren_match:
        candidate = paren_match.group(1).strip()
        name = name[: paren_match.start()].strip(" -–—|")

    if not candidate:
        for sep in (" - ", " – ", " — ", " | "):
            if sep in name:
                head, tail = name.rsplit(sep, 1)
                if "," in tail and "marathon" not in tail.lower():
                    candidate = tail.strip()
                    name = head.strip()
                    break

    if not candidate and "," in name:
        parts = [p.strip() for p in name.split(",") if p.strip()]
        if len(parts) >= 3 and "marathon" in parts[0].lower():
            candidate = ", ".join(parts[-2:])
            name = ", ".join(parts[:-2]).strip()

    if not candidate:
        return name, None, None

    location_parts = [p.strip() for p in candidate.split(",") if p.strip()]
    if len(location_parts) < 2:
        return name, None, None

    city = location_parts[0]
    country = location_parts[-1]
    return name, city, country


def _is_full_marathon_name(name: str) -> bool:
    lowered = name.lower()
    if "marathon" not in lowered:
        return False
    if lowered.strip() == "marathon":
        return False
    return "half marathon" not in lowered and "half-marathon" not in lowered


def _looks_like_json_fragment(value: str) -> bool:
    if not value:
        return False
    lowered = value.lower()
    return any(token in lowered for token in ("\\\"", "\":", "{", "}", "hasstrava", "haswm", "photos", "location\":"))


def _is_reasonable_name(name: str) -> bool:
    if not name:
        return False
    if any(char in name for char in ('{', '}', ':', '"', '\\')):
        return False
    letters = sum(ch.isalpha() for ch in name)
    return letters >= 5


def _should_visit_link(base_domain: str, href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc and parsed.netloc != base_domain:
        return False
    lower = href.lower()
    return any(token in lower for token in ("marathon", "race", "calendar"))


def _fetch_url(url: str, timeout_seconds: int) -> Optional[str]:
    request = Request(url, headers={"User-Agent": "marathon-race-crawler/1.0"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "")
            if content_type and "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                logger.info("Skipping non-HTML content from %s (%s)", url, content_type)
            return response.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError, TimeoutError) as exc:
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

        page_races = _parse_jsonld(html, url)
        if not page_races:
            page_races = _parse_fallback_races(html, url)
        races.extend(page_races)

        domain = urlparse(url).netloc
        for link in _extract_links(html, url):
            if link not in visited and _should_visit_link(domain, link):
                queue.append(link)

    return races


def curated_major_marathons() -> List[Race]:
    now = datetime.now(timezone.utc).isoformat()
    curated = []
    for item in MAJOR_MARATHONS:
        curated.append(
            Race(
                name=item["name"],
                date_start=None,
                date_end=None,
                city=None,
                region=None,
                country=None,
                lat=None,
                lng=None,
                distance_km=42.195,
                website_url=item["website_url"],
                source="World Marathon Majors",
                source_event_id=None,
                description="",
                entry_requirements="Not specified",
                last_seen_at=now,
                last_verified_at=None,
                status="unknown",
            )
        )
    return curated


def load_existing_races(s3_client: Any, bucket: str, key: str) -> List[Race]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code in {"NoSuchKey", "404", "NoSuchBucket"} or "NoSuchKey" in str(exc):
            return []
        raise
    payload = json.loads(obj["Body"].read().decode("utf-8"))
    races = []
    for item in payload.get("races", []):
        if not isinstance(item, dict):
            continue
        races.append(_race_from_payload(item))
    return races


def _race_from_payload(item: Dict[str, Any]) -> Race:
    if "date_start" in item:
        return Race(**item)

    now = datetime.now(timezone.utc).isoformat()
    name = item.get("name", "")
    date_start = item.get("date")
    location = item.get("location") or ""
    city, region, country = None, None, None
    if isinstance(location, str):
        parts = [p.strip() for p in location.split(",") if p.strip()]
        if parts:
            city = parts[0]
        if len(parts) > 1:
            country = parts[-1]
    return Race(
        name=name,
        date_start=date_start,
        date_end=None,
        city=city,
        region=region,
        country=country,
        lat=None,
        lng=None,
        distance_km=42.195,
        website_url=item.get("source_url") or "",
        source=_source_from_url(item.get("source_url") or ""),
        source_event_id=None,
        description=item.get("description") or "",
        entry_requirements=item.get("entry_requirements") or "Not specified",
        last_seen_at=now,
        last_verified_at=None,
        status="unknown",
    )


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

    bucket = os.getenv("RACES_BUCKET", DEFAULT_BUCKET_NAME)
    key = os.getenv("RACES_KEY", "races/marathons.json")
    max_pages = int(os.getenv("MAX_PAGES", "80"))
    seed_urls = [
        u.strip() for u in os.getenv("SEED_URLS", ",".join(DEFAULT_SEED_URLS)).split(",") if u.strip()
    ]

    s3_client = boto3.client("s3")
    existing = load_existing_races(s3_client, bucket, key)
    discovered = crawl_sources(seed_urls=seed_urls, max_pages=max_pages)
    curated = curated_major_marathons()
    filtered = _dedupe_and_filter(existing + discovered + curated, today=datetime.now(timezone.utc).date())
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
