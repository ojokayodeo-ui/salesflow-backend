# SalesFlow AI — Backend

FastAPI backend powering the SalesFlow AI pipeline:

```
Instantly.ai positive reply
  → /api/webhook/instantly
  → ICP generation (Claude)
  → Apollo filter config
  → Delivery  email (Microsoft Outlook via Graph API)
```

---

## Quick Start

```bash
# 1. Clone and enter the directory
cd salesflow-backend

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env with your credentials (see setup guide below)

# 5. Run the server
uvicorn app.main:app --reload --port 8000
```

API docs available at: http://localhost:8000/docs

---

## Microsoft Outlook Setup (Azure App Registration)

This is the only non-trivial setup step. Follow these instructions exactly.

### Step 1 — Create an App Registration

1. Go to https://portal.azure.com
2. Search for **Azure Active Directory** → **App registrations**
3. Click **New registration**
4. Name: `SalesFlow AI`
5. Supported account types: **Accounts in this organizational directory only**
6. Click **Register**

Copy the **Application (client) ID** and **Directory (tenant) ID** — you'll need both.

### Step 2 — Add API Permissions

1. In your new app registration, go to **API permissions**
2. Click **Add a permission** → **Microsoft Graph**
3. Choose **Application permissions** (not Delegated)
4. Search for and add: `Mail.Send`
5. Click **Grant admin consent** (requires admin rights on your tenant)

### Step 3 — Create a Client Secret

1. Go to **Certificates & secrets** → **New client secret**
2. Set an expiry (24 months recommended)
3. Copy the **Value** immediately — it won't show again

### Step 4 — Update .env

```env
MS_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
MS_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
MS_CLIENT_SECRET=your~secret~value~here
MS_SENDER_EMAIL=kayode@yourdomain.com
```

`MS_SENDER_EMAIL` must be a real mailbox in your Microsoft 365 tenant.

### Step 5 — Test the Connection

```bash
curl http://localhost:8000/api/email/test-connection
# → {"success": true, "token_preview": "eyJ0eXAiO..."}
```

---

## Instantly.ai Webhook Setup

1. In Instantly, go to **Settings → Webhooks**
2. Add a new webhook:
   - **URL**: `https://your-domain.com/api/webhook/instantly`
   - **Event**: Reply (select "Positive replies" if available)
   - **Secret**: Any string — copy it to `INSTANTLY_WEBHOOK_SECRET` in `.env`
3. Save

When a prospect replies positively, Instantly fires the webhook and your
pipeline runs automatically in the background.

---

## API Reference

### POST /api/email/send
Send a delivery email with optional CSV attachment.

```json
{
  "to_email": "sarah@prospect.co.uk",
  "to_name": "Sarah Mitchell",
  "from_name": "Kayode · SalesFlow",
  "subject": "Your 100 leads — Nexus Health Analytics",
  "body": "Hi Sarah, your leads are attached...",
  "attach_csv": true,
  "csv_data": "Name,Email,Company\nJames Thornton,...",
  "csv_filename": "leads_100.csv"
}
```

### POST /api/pipeline/run
Manually trigger the full pipeline for a prospect.

```json
{
  "prospect": {
    "name": "Sarah Mitchell",
    "email": "sarah@nexushealth.co.uk",
    "company": "Nexus Health Analytics",
    "domain": "nexushealthanalytics.co.uk"
  },
  "auto_send": false
}
```

### POST /api/webhook/instantly
Receives Instantly.ai positive reply events (background processing).

### GET /api/email/test-connection
Smoke-test your Microsoft Graph credentials.

---

## Deployment (Railway / Render / Fly.io)

```bash
# Procfile (for Railway / Render)
web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

Set all `.env` values as environment variables in your hosting dashboard.
Do NOT commit your `.env` file to git.

---

## Connecting the Frontend

In the SalesFlow AI app, update the Send button to call your backend:

```javascript
// Replace the simulated sendEmail() with:
const result = await fetch('https://your-backend.railway.app/api/email/send', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    to_email: document.getElementById('to-email').value,
    to_name: document.getElementById('to-name').value,
    from_name: document.getElementById('from-name').value,
    subject: document.getElementById('email-subject').value,
    body: emailBody,
    attach_csv: true,
    csv_data: generateCSVString(),   // your lead list as CSV text
  })
});
const data = await result.json();
```

---

## Project Structure

```
salesflow-backend/
├── app/
│   ├── main.py              # FastAPI app, CORS, router registration
│   ├── config.py            # Settings (reads from .env)
│   ├── models/
│   │   └── schemas.py       # Pydantic request/response models
│   ├── routers/
│   │   ├── email.py         # POST /api/email/send
│   │   ├── webhook.py       # POST /api/webhook/instantly
│   │   └── pipeline.py      # POST /api/pipeline/run
│   └── services/
│       ├── outlook.py       # Microsoft Graph API integration
│       ├── icp.py           # Claude ICP generation + Apollo filter mapping
│       └── composer.py      # Email body templates + AI personalisation
├── requirements.txt
├── .env.example
└── README.md
```
