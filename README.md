## Milled India – Backend & Ingestion

### 1. Local Python setup (optional for testing)

- Create a virtualenv and install deps:

```bash
cd milled_india
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Gmail credentials

You have two options:

#### Option A (recommended for Render): refresh token via env vars (no browser)

1) Create an OAuth client in Google Cloud Console (Desktop app) with Gmail API enabled.
2) Download the OAuth client JSON (often called `credentials.json`) locally.
3) Generate a refresh token locally:

```bash
python get_refresh_token.py --credentials credentials.json
```

4) Add these env vars on Render (both the **web service** and the **cron job**):
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN`

After that, ingestion can run headlessly on Render (no `token.pickle`, no browser).

#### Option B (local only): browser OAuth + token.pickle

- Place your Gmail API `credentials.json` in the project root (next to `engine.py`).
- First run of ingestion will open a browser for OAuth and create `token.pickle`.

### 3. Run ingestion (populate SQLite)

```bash
python ingest_gmail.py
```

This will:
- Fetch recent emails from the `Search Engine` label.
- Store them into `emails.db` via the `Email` SQLAlchemy model.

### 4. Run FastAPI backend

```bash
uvicorn backend.main:app --reload
```

Endpoints:
- `GET /health` – simple health check.
- `GET /emails` – list emails, supports `brand`, `type`, `q`, `skip`, `limit`.
- `GET /emails/{id}` – get single email metadata.

## Deploying the API to Render

1. Push this repo to GitHub.
2. On Render:
   - Click "New +" → "Blueprint" and select this repo.
   - Render will detect `render.yaml` and create:
     - A **Postgres database** (`milled-india-db`).
     - A **web service** (`milled-india-api`) running FastAPI.
     - A **cron job** (`milled-india-ingest`) that runs `python ingest_gmail.py` every 30 minutes.
3. Add any additional env vars you need (e.g. Gmail credentials or config).

Once deployed, you’ll have a public API URL like:
- `https://milled-india-api.onrender.com/health`
- `https://milled-india-api.onrender.com/emails`

Use that base URL as `NEXT_PUBLIC_API_BASE_URL` for the frontend.

## Next.js Frontend (separate app, e.g. on Vercel)

Create a Next.js app in a sibling `frontend/` directory:

```bash
cd ..
npx create-next-app@latest frontend
cd frontend
```

Set an environment variable in `frontend/.env.local`:

```bash
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000   # or your Render URL in production
```

Then implement pages that call the backend, e.g.:

### `app/page.tsx` – home feed

```tsx
// app/page.tsx
type Email = {
  id: number;
  subject: string;
  brand?: string;
  preview?: string;
};

async function fetchEmails(q?: string, brand?: string): Promise<Email[]> {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (brand) params.set("brand", brand);

  const base = process.env.NEXT_PUBLIC_API_BASE_URL!;
  const res = await fetch(`${base}/emails?${params.toString()}`, {
    cache: "no-store",
  });
  if (!res.ok) throw new Error("Failed to fetch emails");
  return res.json();
}

export default async function Home({
  searchParams,
}: {
  searchParams?: { q?: string; brand?: string };
}) {
  const q = searchParams?.q;
  const brand = searchParams?.brand;
  const emails = await fetchEmails(q, brand);

  return (
    <main className="max-w-5xl mx-auto p-6">
      <h1 className="text-3xl font-bold mb-4">Milled India</h1>
      {/* You can add search + brand filters here */}
      <ul className="space-y-3">
        {emails.map((e) => (
          <li key={e.id} className="border rounded p-3 hover:bg-gray-50">
            <a href={`/email/${e.id}`} className="block">
              <div className="flex justify-between items-center">
                <span className="font-semibold">{e.subject}</span>
                {e.brand && (
                  <span className="text-xs uppercase tracking-wide text-gray-500">
                    {e.brand}
                  </span>
                )}
              </div>
              {e.preview && (
                <p className="text-sm text-gray-600 mt-1 line-clamp-2">
                  {e.preview}
                </p>
              )}
            </a>
          </li>
        ))}
      </ul>
    </main>
  );
}
```

### `app/email/[id]/page.tsx` – email detail

```tsx
// app/email/[id]/page.tsx
type Email = {
  id: number;
  subject: string;
  brand?: string;
  received_at: string;
  html: string;
};

async function fetchEmail(id: string): Promise<Email> {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL!;
  const res = await fetch(`${base}/emails/${id}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch email");
  return res.json();
}

export default async function EmailPage({
  params,
}: {
  params: { id: string };
}) {
  const email = await fetchEmail(params.id);

  return (
    <main className="max-w-4xl mx-auto p-6">
      <a href="/" className="text-sm text-blue-600">
        ← Back to feed
      </a>
      <h1 className="text-2xl font-bold mt-3 mb-1">{email.subject}</h1>
      <div className="text-sm text-gray-500 mb-4">
        {email.brand && <span className="mr-2">{email.brand}</span>}
        <span>{new Date(email.received_at).toLocaleString()}</span>
      </div>
      <article
        className="border rounded p-4 bg-white"
        dangerouslySetInnerHTML={{ __html: email.html }}
      />
    </main>
  );
}
```

With backend running on `127.0.0.1:8000` and `NEXT_PUBLIC_API_BASE_URL` set, `npm run dev` in `frontend/` will give you a basic milled-style UI backed by your Gmail ingestion.

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
