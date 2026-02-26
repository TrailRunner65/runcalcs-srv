import json
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DEFAULT_SEED_URLS = [
    "https://www.letsrun.com",
    "https://www.letsrun.com/news",
    "https://www.runnersworld.com",
    "https://www.runnersworld.com/running",
]


@dataclass
class Article:
    title: str
    summary: str
    source_url: str

    def to_dict(self) -> Dict[str, str]:
        return self.__dict__.copy()


def _walk_jsonld(payload: Any) -> Iterable[Any]:
    if isinstance(payload, list):
        for item in payload:
            yield from _walk_jsonld(item)
    elif isinstance(payload, dict):
        if "@graph" in payload:
            yield from _walk_jsonld(payload["@graph"])
        yield payload


def _is_article(item: Dict[str, Any]) -> bool:
    value = item.get("@type")
    article_types = {"Article", "NewsArticle", "BlogPosting", "Report"}
    if isinstance(value, list):
        return any(v in article_types for v in value)
    return value in article_types


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


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    collapsed = re.sub(r"\s+", " ", value).strip()
    return collapsed


def _to_summary(value: str, max_len: int = 220) -> str:
    cleaned = _clean_text(value)
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[: max_len - 1].rstrip() + "…"


def _first_non_empty(values: Iterable[Any]) -> str:
    for value in values:
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return ""


def _extract_url(item: Dict[str, Any], page_url: str) -> str:
    raw = item.get("url")
    if isinstance(raw, str) and raw.strip():
        return urljoin(page_url, raw.strip())
    main = item.get("mainEntityOfPage")
    if isinstance(main, dict):
        main_id = main.get("@id")
        if isinstance(main_id, str) and main_id.strip():
            return urljoin(page_url, main_id.strip())
    return page_url


def _parse_jsonld_articles(doc: str, page_url: str) -> List[Article]:
    articles: List[Article] = []
    for blob in _extract_jsonld_blobs(doc):
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            continue

        for item in _walk_jsonld(data):
            if not isinstance(item, dict) or not _is_article(item):
                continue

            title = _first_non_empty([item.get("headline"), item.get("name")])
            summary = _first_non_empty([item.get("description"), item.get("articleBody")])
            if not title:
                continue

            source_url = _extract_url(item, page_url)
            articles.append(Article(title=title, summary=_to_summary(summary), source_url=source_url))
    return articles


def _parse_html_articles(doc: str, page_url: str) -> List[Article]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", doc, re.IGNORECASE | re.DOTALL)
    meta_description_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        doc,
        re.IGNORECASE | re.DOTALL,
    )

    title = _clean_text(unescape(title_match.group(1))) if title_match else ""
    summary = _clean_text(unescape(meta_description_match.group(1))) if meta_description_match else ""

    if not title:
        return []

    return [Article(title=title, summary=_to_summary(summary), source_url=page_url)]


def _article_key(article: Article) -> Tuple[str, str]:
    norm = lambda x: re.sub(r"\s+", " ", x.lower()).strip()
    return norm(article.title), article.source_url.strip().lower()


def _dedupe_articles(articles: Iterable[Article]) -> List[Article]:
    unique: Dict[Tuple[str, str], Article] = {}
    for article in articles:
        if not article.title or not article.source_url:
            continue
        unique.setdefault(_article_key(article), article)
    return sorted(unique.values(), key=lambda a: a.title.lower())


def _is_allowed_source(url: str) -> bool:
    domain = urlparse(url).netloc.lower()
    return any(d in domain for d in ("letsrun.com", "runnersworld.com", "runnersword.com"))


def _should_visit_link(base_domain: str, href: str) -> bool:
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc and parsed.netloc != base_domain:
        return False
    lower = href.lower()
    return any(token in lower for token in ("news", "article", "running", "training", "/202"))


def _fetch_url(url: str, timeout_seconds: int) -> Optional[str]:
    request = Request(url, headers={"User-Agent": "running-article-crawler/1.0"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return None
            return response.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError, TimeoutError) as exc:
        logger.warning("Failed fetching %s: %s", url, exc)
        return None


def crawl_sources(seed_urls: List[str], max_pages: int = 80, timeout_seconds: int = 15) -> List[Article]:
    queue: deque[str] = deque(seed_urls)
    visited: Set[str] = set()
    articles: List[Article] = []

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        if url in visited or not _is_allowed_source(url):
            continue
        visited.add(url)

        html = _fetch_url(url, timeout_seconds)
        if not html:
            continue

        articles.extend(_parse_jsonld_articles(html, url))
        articles.extend(_parse_html_articles(html, url))

        domain = urlparse(url).netloc
        for link in _extract_links(html, url):
            if link not in visited and _is_allowed_source(link) and _should_visit_link(domain, link):
                queue.append(link)

    return articles


def load_existing_articles(s3_client: Any, bucket: str, key: str) -> List[Article]:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code in {"NoSuchKey", "404", "NoSuchBucket"} or "NoSuchKey" in str(exc):
            return []
        raise
    payload = json.loads(obj["Body"].read().decode("utf-8"))
    return [Article(**item) for item in payload.get("articles", []) if isinstance(item, dict)]


def store_articles(s3_client: Any, bucket: str, key: str, articles: List[Article]) -> None:
    body = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(articles),
        "articles": [a.to_dict() for a in articles],
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
    key = os.getenv("RACES_KEY", "running/articles.json")
    max_pages = int(os.getenv("MAX_PAGES", "80"))
    seed_urls = [
        u.strip() for u in os.getenv("SEED_URLS", ",".join(DEFAULT_SEED_URLS)).split(",") if u.strip()
    ]

    s3_client = boto3.client("s3")
    existing = load_existing_articles(s3_client, bucket, key)
    discovered = crawl_sources(seed_urls=seed_urls, max_pages=max_pages)
    merged = _dedupe_articles(existing + discovered)
    store_articles(s3_client, bucket, key, merged)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "stored": len(merged),
                "discovered": len(discovered),
                "existing": len(existing),
                "bucket": bucket,
                "key": key,
            }
        ),
    }
