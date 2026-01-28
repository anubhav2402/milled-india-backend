import os
from datetime import datetime

EMAILS_DIR = "emails"
OUTPUT_FILE = "index.html"


def collect_emails():
    items = []

    for brand in os.listdir(EMAILS_DIR):
        brand_path = os.path.join(EMAILS_DIR, brand)
        if not os.path.isdir(brand_path):
            continue

        for file in os.listdir(brand_path):
            if not file.endswith(".html"):
                continue

            try:
                date_part, subject_part = file.split("_", 1)
                date = datetime.strptime(date_part, "%Y-%m-%d")
                subject = subject_part.replace(".html", "").replace("-", " ").title()
            except Exception:
                date = datetime.min
                subject = file.replace(".html", "")

            items.append({
                "brand": brand.title(),
                "date": date,
                "date_str": date.strftime("%d %b %Y"),
                "subject": subject,
                "path": f"{brand}/{file}"
            })

    # newest first
    items.sort(key=lambda x: x["date"], reverse=True)
    return items


def build_html(items):
    rows = []

    for item in items:
        rows.append(f"""
        <div class="row">
            <div class="brand">{item['brand']}</div>
            <div class="subject">
                <a href="{EMAILS_DIR}/{item['path']}" target="_blank">
                    {item['subject']}
                </a>
            </div>
            <div class="date">{item['date_str']}</div>
        </div>
        """)

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Indian Brand Email Feed</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            background: #fafafa;
            margin: 0;
            padding: 20px;
        }}
        h1 {{
            margin-bottom: 20px;
        }}
        .row {{
            display: grid;
            grid-template-columns: 140px 1fr 120px;
            gap: 16px;
            padding: 12px 16px;
            background: #fff;
            border-bottom: 1px solid #eee;
            align-items: center;
        }}
        .row:hover {{
            background: #f5f7ff;
        }}
        .brand {{
            font-weight: 600;
            text-transform: capitalize;
        }}
        .subject a {{
            color: #1a0dab;
            text-decoration: none;
        }}
        .subject a:hover {{
            text-decoration: underline;
        }}
        .date {{
            color: #666;
            font-size: 14px;
            text-align: right;
        }}
    </style>
</head>
<body>
    <h1>📬 Indian Brand Email Feed</h1>
    <p>{len(items)} campaigns captured</p>
    {"".join(rows)}
</body>
</html>
"""


def main():
    items = collect_emails()
    html = build_html(items)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ index.html generated with {len(items)} emails")


if __name__ == "__main__":
    main()
