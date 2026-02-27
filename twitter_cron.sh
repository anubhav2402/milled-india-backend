#!/bin/bash
# Twitter tweet draft generation cron
# Usage:
#   twitter_cron.sh daily         - Generate daily digest draft
#   twitter_cron.sh weekly        - Generate weekly roundup + subject line insight
#   twitter_cron.sh spotlight     - Generate brand spotlight draft
#
# Cron schedule:
#   0 8 * * *     /path/to/twitter_cron.sh daily
#   0 8 * * 1     /path/to/twitter_cron.sh weekly
#   0 8 * * 2,4,6 /path/to/twitter_cron.sh spotlight

cd /Users/macbook/milled_india

TYPE=${1:-daily}

case "$TYPE" in
  daily)
    venv/bin/python -c "
from backend.db import SessionLocal
from backend.twitter import generate_tweet_content
from backend.models import TweetQueue
db = SessionLocal()
content = generate_tweet_content('daily_digest', db)
tweet = TweetQueue(content=content, tweet_type='daily_digest', status='draft')
db.add(tweet)
db.commit()
print(f'Generated daily digest draft: {content[:80]}...')
db.close()
"
    ;;
  weekly)
    venv/bin/python -c "
from backend.db import SessionLocal
from backend.twitter import generate_tweet_content
from backend.models import TweetQueue
db = SessionLocal()
for t in ['weekly_digest', 'subject_line_insight']:
    content = generate_tweet_content(t, db)
    tweet = TweetQueue(content=content, tweet_type=t, status='draft')
    db.add(tweet)
    db.commit()
    print(f'Generated {t} draft: {content[:80]}...')
db.close()
"
    ;;
  spotlight)
    venv/bin/python -c "
from backend.db import SessionLocal
from backend.twitter import generate_tweet_content
from backend.models import TweetQueue
db = SessionLocal()
content = generate_tweet_content('brand_spotlight', db)
tweet = TweetQueue(content=content, tweet_type='brand_spotlight', status='draft')
db.add(tweet)
db.commit()
print(f'Generated brand spotlight draft: {content[:80]}...')
db.close()
"
    ;;
  *)
    echo "Usage: $0 {daily|weekly|spotlight}"
    exit 1
    ;;
esac
