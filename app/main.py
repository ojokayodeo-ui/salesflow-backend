import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import email, webhook, pipeline, crm
from app.services.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="PALM Backend",
    version="3.0.0",
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
    return {"status": "PALM backend running", "version": "3.0.0"}


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
