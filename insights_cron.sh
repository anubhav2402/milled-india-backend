#!/bin/bash
# Insights Engine — weekly computation
# Run every Sunday at 23:00 UTC (after all weekend emails are collected)
#
# Cron entry:
#   0 23 * * 0  /Users/macbook/milled_india/insights_cron.sh >> /Users/macbook/milled_india/insights_cron.log 2>&1

cd /Users/macbook/milled_india

venv/bin/python -c "
from datetime import datetime
from backend.db import SessionLocal
from backend.insights import compute_all

db = SessionLocal()
try:
    results = compute_all(db)
    print(f'[{datetime.utcnow().isoformat()}] Insights computed: {results}')
finally:
    db.close()
"
