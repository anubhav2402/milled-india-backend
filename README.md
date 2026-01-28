# Milled India – Gmail Ingestion Starter

## What this does
- Reads promotional emails from Gmail
- Uses Gmail API (read-only)
- Pulls emails from label: PROMO_INGEST
- Prints subject, sender, date, HTML presence

## Setup
1. Create a Google Cloud project
2. Enable Gmail API
3. Download OAuth credentials as credentials.json
4. Place credentials.json in this folder

## Run
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python ingest.py
```

First run will open a browser for Google consent.

## Deploying on Render (recommended)

### Gmail auth (no browser)

For server-side ingestion you should use env-based OAuth (refresh token), not `token.pickle`.

1) Create an OAuth client (Desktop app) in Google Cloud Console and enable Gmail API.
2) Download the OAuth credentials JSON locally (often named `credentials.json`).
3) Generate a refresh token locally:

```bash
python get_refresh_token.py --credentials credentials.json
```

4) Add these **Environment Variables** on Render (both the Web Service and Cron Job):
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN`

### De-duplication note (important)

On servers, local files may not persist across runs. By default, ingestion does **not** use
`processed_ids.txt` and relies on the database unique `gmail_id` to avoid duplicates.

If you want the old local-file behavior (mainly for local debugging), set:
- `USE_PROCESSED_FILE=true`
