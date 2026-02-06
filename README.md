# runcalcs-srv

AWS SAM project that deploys a daily Lambda to discover upcoming marathon races, deduplicate them, and persist the merged dataset to S3.

## What it does
- Uses the S3 bucket name `runcalcs` by default (no bucket name is required in the Lambda event). Ensure the bucket exists before deployment.
- Runs once per day at midnight UTC via EventBridge schedule (`cron(0 0 * * ? *)`).
- Crawls a set of seed marathon race calendar pages (including AIMS and World Marathon Majors pages) plus major marathon sites.
- Ensures the six World Marathon Majors are always present via a curated baseline list.
- Extracts race metadata from `application/ld+json` Event markup.
- Falls back to HTML text parsing when JSON-LD is missing.
- Captures structured race details (see Data model below).
- Merges with previously stored data in S3.
- Deduplicates races by normalized `name + date + location`.
- Removes races whose date is in the past.
- Writes refreshed JSON file back to S3.

## Data model
Each race entry includes:
- `name`
- `date_start` (and optional `date_end`)
- `city`, `region`, `country`
- `lat`, `lng` (optional)
- `distance_km` (defaults to 42.195)
- `website_url`
- `source` (AIMS / Ahotu / etc.)
- `source_event_id` (when available)
- `last_seen_at`, `last_verified_at`
- `status` (`scheduled` / `cancelled` / `unknown`)

## De-duping strategy
Races are deduped by matching:
- normalized name
- date within ±1 day
- city and country (when available)
- website domain

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

## Run on AWS
```bash
# build and deploy to AWS (first deployment can use --guided)
sam build
sam deploy --stack-name marathon-race-crawler --capabilities CAPABILITY_IAM

# invoke the deployed Lambda in AWS
aws lambda invoke \
  --function-name marathon-race-crawler \
  --payload '{}' \
  response.json

# tail Lambda logs in CloudWatch
sam logs -n MarathonRaceCrawlerFunction --stack-name marathon-race-crawler --tail
```

## Lambda environment variables
- `RACES_BUCKET` (optional override; defaults to `runcalcs`)
- `RACES_KEY` (default: `races/marathons.json`)
- `MAX_PAGES` (default: `80`)
- `SEED_URLS` (comma-separated list; defaults are in code)
