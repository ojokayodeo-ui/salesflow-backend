import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import email, webhook, pipeline, crm, calendly, analytics, nurture, mail, agent
from app.services.database import init_db, ensure_extra_tables
from app.services.scheduler import run_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await ensure_extra_tables()
    # Start the email scheduler as a background task
    scheduler_task = asyncio.create_task(run_scheduler())
    yield
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="PALM Backend",
    version="3.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(email.router,    prefix="/api/email",    tags=["Email"])
app.include_router(webhook.router,  prefix="/api/webhook",  tags=["Webhooks"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["Pipeline"])
app.include_router(crm.router,      prefix="/api/crm",      tags=["CRM"])
app.include_router(calendly.router,  prefix="/api/calendly",  tags=["Calendly"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(nurture.router,   prefix="/api/nurture",   tags=["Nurture"])
app.include_router(mail.router,      prefix="/api/mail",      tags=["Mail"])
app.include_router(agent.router,     prefix="/api/agent",     tags=["Agent"])


@app.get("/")
def root():
    return {"status": "PALM backend running", "version": "3.3.0"}


@app.get("/debug/apollo")
async def debug_apollo():
    """Test Apollo search directly and return the full response."""
    import os, httpx
    key = os.environ.get("APOLLO_API_KEY", "")
    if not key:
        return {"error": "APOLLO_API_KEY not set"}

    payload = {
        "per_page": 5,
        "page": 1,
        "person_titles[]": ["Managing Director", "CEO", "Founder"],
        "person_locations[]": ["United Kingdom"],
        "contact_email_status[]": ["verified"],
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.apollo.io/api/v1/mixed_people/api_search",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": key,
                    "Cache-Control": "no-cache",
                },
            )
        return {
            "status_code": resp.status_code,
            "payload_sent": payload,
            "response": resp.json(),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/pipeline/{email}")
async def debug_pipeline(email: str):
    """Show full pipeline status for a specific email — for debugging."""
    from app.services import database as db
    deal = await db.get_deal_by_email(email)
    if not deal:
        return {"found": False, "email": email}
    return {
        "found":      True,
        "deal_id":    deal["id"],
        "stage":      deal["stage"],
        "name":       deal["name"],
        "company":    deal["company"],
        "job_title":  deal.get("job_title"),
        "location":   deal.get("location"),
        "industry":   deal.get("industry"),
        "icp_built":  bool(deal.get("icp")),
        "icp_segments": len(deal["icp"]["segments"]) if deal.get("icp") and deal["icp"].get("segments") else 0,
        "sentiment":  deal.get("sentiment"),
        "history":    deal.get("history", []),
        "last_activity": deal.get("last_activity"),
    }


@app.get("/debug/env")
def debug_env():
    anthropic  = os.environ.get("ANTHROPIC_API_KEY", "")
    apollo     = os.environ.get("APOLLO_API_KEY", "")
    ms_tenant  = os.environ.get("MS_TENANT_ID", "")
    ms_client  = os.environ.get("MS_CLIENT_ID", "")
    ms_sender  = os.environ.get("MS_SENDER_EMAIL", "")
    instantly  = os.environ.get("INSTANTLY_API_KEY", "")
    apify      = os.environ.get("APIFY_API_TOKEN", "")
    return {
        "anthropic_set":    bool(anthropic),
        "anthropic_prefix": anthropic[:15] if anthropic else "NOT SET",
        "apollo_set":       bool(apollo),
        "apollo_prefix":    apollo[:10] if apollo else "NOT SET",
        "instantly_set":    bool(instantly),
        "instantly_prefix": instantly[:10] if instantly else "NOT SET",
        "apify_set":        bool(apify),
        "apify_prefix":     apify[:10] if apify else "NOT SET",
        "ms_tenant_set":    bool(ms_tenant),
        "ms_client_set":    bool(ms_client),
        "ms_sender":        ms_sender or "NOT SET",
        "review_mode":      os.environ.get("REVIEW_MODE", "not set"),
        "from_name":        os.environ.get("DEFAULT_FROM_NAME", "not set"),
        "app_url":          os.environ.get("APP_URL", "NOT SET"),
    }
