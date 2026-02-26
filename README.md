# runcalcs-srv

AWS SAM project that deploys a daily Lambda to discover running news/articles, deduplicate them, and persist the merged dataset to S3.

## What it does
- Runs once per day at midnight UTC via EventBridge schedule (`cron(0 0 * * ? *)`).
- Crawls a set of running article pages from:
  - `LetsRun.com`
  - `runnersworld.com`
- Extracts article metadata from `application/ld+json` Article markup.
- Falls back to the page `<title>` and meta description when JSON-LD is unavailable.
- Captures:
  - `title`
  - `summary` (short snippet from the start of article text/description)
  - `source_url` (link to original article)
- Merges with previously stored data in S3.
- Deduplicates articles by normalized `title + source_url`.
- Writes refreshed JSON file back to S3.

## Files
- `lambda_function.py` - Lambda implementation.
- `template.yaml` - SAM template (Lambda, S3 bucket, EventBridge schedule).
- `requirements.txt` - Python dependencies.
- `tests/test_lambda_function.py` - Unit tests for parsing and dedupe behavior.

## Deploy
```bash
sam build
sam deploy --guided
```

## Lambda environment variables
- `RACES_BUCKET` (required)
- `RACES_KEY` (default: `running/articles.json`)
- `MAX_PAGES` (default: `80`)
- `SEED_URLS` (comma-separated list; defaults are in code)
