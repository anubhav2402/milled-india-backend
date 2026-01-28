"""
One-time helper to generate a Gmail API refresh token for server-side ingestion.

Steps:
1) Create OAuth client "Desktop app" in Google Cloud Console (Gmail API enabled).
2) Download credentials JSON (or copy client_id/client_secret).
3) Run:
   python get_refresh_token.py --credentials credentials.json
4) It will open a browser for consent and print a refresh token.

Then set these env vars on Render (web service + cron job):
- GOOGLE_CLIENT_ID
- GOOGLE_CLIENT_SECRET
- GOOGLE_REFRESH_TOKEN
"""

import argparse
import json

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials",
        required=True,
        help="Path to OAuth client credentials JSON (downloaded from Google Cloud)",
    )
    args = parser.parse_args()

    with open(args.credentials, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support both "installed" and "web" formats just in case.
    if "installed" in data:
        client_config = {"installed": data["installed"]}
    elif "web" in data:
        client_config = {"installed": data["web"]}
    else:
        raise SystemExit("Unrecognized credentials.json format (expected 'installed' or 'web').")

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    if not creds.refresh_token:
        raise SystemExit(
            "No refresh token was returned. Re-run and ensure prompt='consent' and access_type='offline'."
        )

    print("\n=== Copy these values into Render env vars ===")
    print("GOOGLE_CLIENT_ID:", client_config["installed"]["client_id"])
    print("GOOGLE_CLIENT_SECRET:", client_config["installed"]["client_secret"])
    print("GOOGLE_REFRESH_TOKEN:", creds.refresh_token)


if __name__ == "__main__":
    main()

