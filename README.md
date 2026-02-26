# runcalcs-srv

AWS SAM project that deploys a daily Lambda to discover running news/articles, deduplicate them, and persist the merged dataset to S3.

## Requirements
- Python `3.13.3`
- AWS SAM CLI
- AWS credentials configured for deployment

## What it does
- Runs once per day at midnight UTC via EventBridge schedule (`cron(0 0 * * ? *)`).
- Crawls running **article/news section pages** (not main site homepages).
- Extracts article metadata from `application/ld+json` Article markup.
- Falls back to the page `<title>` and meta description when JSON-LD is unavailable.
- Captures:
  - `title`
  - `summary` (short snippet from the start of article text/description)
  - `source_url` (link to original article)
- Merges with previously stored data in S3.
- Deduplicates articles by normalized `title + source_url`.
- Writes refreshed JSON file back to S3.

## Default article/news sources
- `https://www.letsrun.com/news/`
- `https://www.runnersworld.com/running/`
- `https://www.runnersworld.com/training/`
- `https://www.irunfar.com/news`
- `https://www.trailrunnermag.com/category/training/`
- `https://runningmagazine.ca/the-scene/`

> Tip: keep `SEED_URLS` focused on section/category/article listing pages rather than domain root homepages.

## Files
- `lambda_function.py` - Lambda implementation.
- `template.yaml` - SAM template (Lambda, S3 bucket, EventBridge schedule).
- `requirements.txt` - Python dependencies.
- `tests/test_lambda_function.py` - Unit tests for parsing and dedupe behavior.

## Deploy
```bash
sam build && sam deploy --stack-name running-article-crawler --capabilities CAPABILITY_IAM --resolve-s3
```

## Lambda environment variables
- `RACES_BUCKET` (required)
- `RACES_KEY` (default: `running/articles.json`)
- `MAX_PAGES` (default: `80`)
- `SEED_URLS` (comma-separated list; defaults are in code)
