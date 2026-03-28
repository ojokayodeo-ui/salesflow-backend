from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import email, webhook, pipeline, crm
from app.services.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs once at startup — creates DB tables if they don't exist
    await init_db()
    yield


app = FastAPI(
    title="SalesFlow AI Backend",
    description="Pipeline: Instantly.ai → ICP → Apollo → Email Delivery → CRM",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock this to your Netlify URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(email.router,    prefix="/api/email",    tags=["Email Delivery"])
app.include_router(webhook.router,  prefix="/api/webhook",  tags=["Webhooks"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["Pipeline"])
app.include_router(crm.router,      prefix="/api/crm",      tags=["CRM"])


@app.get("/")
def root():
    return {
        "status": "SalesFlow AI backend running",
        "version": "2.0.0",
        "docs": "/docs",
        "endpoints": {
            "webhook":  "/api/webhook/instantly",
            "crm":      "/api/crm/deals",
            "email":    "/api/email/send",
            "pipeline": "/api/pipeline/run",
        }
    }
