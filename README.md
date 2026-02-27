# runcalcs-srv

AWS SAM project that deploys a daily Lambda to request a running tip of the day from OpenAI and store the result in S3.

## Requirements
- Python `3.13.3`
- AWS SAM CLI
- AWS credentials configured for deployment

## What it does
- Runs once per day at midnight UTC via EventBridge schedule (`cron(0 0 * * ? *)`).
- Fetches OpenAI API key from AWS Secrets Manager:
  - Secret name: `ChatGPTKey`
  - Region: `ap-southeast-2`
- Chooses a running tip category from:
  - running equipment
  - health
  - training
  - rest
  - recovery from injury
  - nutrition
  - mental wellbeing
  - weight loss
  - racing
  - club running
  - Parkruns
- Calls OpenAI Chat Completions to generate one concise tip.
- Ensures the target S3 bucket exists (creates it when missing).
- Configures bucket CORS access for:
  - `https://runcalcs.com`
  - `https://www.runcalcs.com`
  - `https://develop.d39l2wzc9rmkuy.amplifyapp.com/`
- Stores each run to a new dated JSON object in S3 (`<prefix>-YYYY-MM-DD.json`).

## Files
- `lambda_function.py` - Lambda implementation.
- `template.yaml` - SAM template (Lambda, S3 bucket, EventBridge schedule, IAM permissions).
- `requirements.txt` - Python dependencies.
- `tests/test_lambda_function.py` - Unit tests.

## Deploy
```bash
sam build && sam deploy --stack-name running-tip-of-day --capabilities CAPABILITY_IAM --resolve-s3
```

## Lambda environment variables
- `TIPS_BUCKET` (required)
- `TIPS_KEY_PREFIX` (default: `running-tips/tip`; output key format: `<prefix>-YYYY-MM-DD.json`)
- `OPENAI_SECRET_NAME` (default: `ChatGPTKey`)
- `OPENAI_MODEL` (default: `gpt-4o-mini`)
- `OPENAI_TIMEOUT_SECONDS` (default: `30`)
