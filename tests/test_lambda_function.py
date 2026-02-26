from lambda_function import (
    Article,
    DEFAULT_SEED_URLS,
    _dedupe_articles,
    _is_allowed_source,
    _parse_html_articles,
    _parse_jsonld_articles,
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
