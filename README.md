# runcalcs-srv

AWS SAM project that deploys a daily Lambda to discover upcoming marathon races, deduplicate them, and persist the merged dataset to S3.

## What it does
- Runs once per day at midnight UTC via EventBridge schedule (`cron(0 0 * * ? *)`).
- Crawls a set of seed marathon race calendar pages.
- Extracts race metadata from `application/ld+json` Event markup.
- Captures:
  - `name`
  - `date`
  - `location`
  - `description`
  - `entry_requirements` (best-effort extraction from text)
  - `source_url`
- Merges with previously stored data in S3.
- Deduplicates races by normalized `name + date + location`.
- Removes races whose date is in the past.
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

## Run with SAM CLI
```bash
# build artifacts
sam build

# run Lambda locally once with a sample event
sam local invoke MarathonRaceCrawlerFunction --event events/event.json

# alternatively run locally as an API (for manual testing)
sam local start-lambda
```

## Lambda environment variables
- `RACES_BUCKET` (required)
- `RACES_KEY` (default: `races/marathons.json`)
- `MAX_PAGES` (default: `80`)
- `SEED_URLS` (comma-separated list; defaults are in code)
