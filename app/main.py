import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import email, webhook, pipeline, crm
from app.services.database import init_db
from app.services.scheduler import run_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
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
    version="3.1.0",
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


@app.get("/")
def root():
    return {"status": "PALM backend running", "version": "3.1.0"}


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


@app.get("/debug/env")
def debug_env():
    anthropic = os.environ.get("ANTHROPIC_API_KEY", "")
    apollo    = os.environ.get("APOLLO_API_KEY", "")
    return {
        "anthropic_set":    bool(anthropic),
        "anthropic_prefix": anthropic[:15] if anthropic else "NOT SET",
        "apollo_set":       bool(apollo),
        "apollo_prefix":    apollo[:10] if apollo else "NOT SET",
        "review_mode":      os.environ.get("REVIEW_MODE", "not set"),
        "from_name":        os.environ.get("DEFAULT_FROM_NAME", "not set"),
    }
