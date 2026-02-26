from lambda_function import (
    Article,
    DEFAULT_SEED_URLS,
    _dedupe_articles,
    _is_allowed_article_url,
    _is_allowed_source,
    _parse_html_articles,
    _parse_jsonld_articles,
    _should_visit_link,
)


def test_parse_jsonld_extracts_running_article():
    html = '''
    <html><head>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": "How to Improve Your 10K Time",
        "description": "Start with consistent weekly mileage and structured intervals.",
        "url": "https://www.letsrun.com/news/2029/10/how-to-improve-10k-time"
      }
      </script>
    </head></html>
    '''

    articles = _parse_jsonld_articles(html, "https://www.letsrun.com/news")

    assert len(articles) == 1
    assert articles[0].title == "How to Improve Your 10K Time"
    assert articles[0].summary.startswith("Start with consistent weekly mileage")
    assert articles[0].source_url.endswith("how-to-improve-10k-time")


def test_dedupe_articles_by_title_and_source_url():
    articles = [
        Article(
            title="Race Day Nutrition Guide",
            summary="A",
            source_url="https://www.runnersworld.com/running/a12345/race-day-nutrition-guide/",
        ),
        Article(
            title=" Race Day   Nutrition Guide ",
            summary="B",
            source_url="https://www.runnersworld.com/running/a12345/race-day-nutrition-guide/",
        ),
    ]

    filtered = _dedupe_articles(articles)

    assert len(filtered) == 1
    assert filtered[0].title == "Race Day Nutrition Guide"


def test_parse_html_fallback_from_title_and_meta_description():
    html = '''
    <html>
      <head>
        <title>Best Recovery Runs for Marathoners</title>
        <meta name="description" content="Easy efforts done consistently can speed recovery." />
      </head>
    </html>
    '''

    articles = _parse_html_articles(html, "https://www.runnersworld.com/running/a1111/recovery-runs")

    assert len(articles) == 1
    assert articles[0].title == "Best Recovery Runs for Marathoners"
    assert "speed recovery" in articles[0].summary


def test_is_allowed_source_accepts_requested_domains():
    assert _is_allowed_source("https://www.letsrun.com/news")
    assert _is_allowed_source("https://www.runnersworld.com/running")
    assert _is_allowed_source("https://runnersword.com")
    assert not _is_allowed_source("https://example.com")


def test_default_seed_urls_are_article_or_news_sections():
    paths = [url.split("/", 3)[-1] if "/" in url[8:] else "" for url in DEFAULT_SEED_URLS]
    assert all(path.strip("/") for path in paths)


def test_is_allowed_source_accepts_added_running_news_domains():
    assert _is_allowed_source("https://www.irunfar.com/news/ultra-training-update")
    assert _is_allowed_source("https://www.trailrunnermag.com/category/training/")
    assert _is_allowed_source("https://runningmagazine.ca/the-scene/")


def test_runnersworld_links_are_limited_to_news_path():
    assert _should_visit_link("www.runnersworld.com", "https://www.runnersworld.com/news/a12345/story/")
    assert not _should_visit_link("www.runnersworld.com", "https://www.runnersworld.com/running/a12345/story/")


def test_default_seed_urls_include_only_runnersworld_news():
    runnersworld_seeds = [u for u in DEFAULT_SEED_URLS if "runnersworld.com" in u]
    assert runnersworld_seeds == ["https://www.runnersworld.com/news/"]


def test_runnersworld_article_urls_are_limited_to_news():
    assert _is_allowed_article_url("https://www.runnersworld.com/news/a12345/story/")
    assert not _is_allowed_article_url("https://www.runnersworld.com/training/a12345/story/")
    assert not _is_allowed_article_url("https://www.runnersworld.com/auth/login")


def test_non_runnersworld_article_urls_still_allowed_from_other_feeds():
    assert _is_allowed_article_url("https://www.letsrun.com/news/2025/10/example/")
    assert _is_allowed_article_url("https://www.irunfar.com/news/ultra-update")
