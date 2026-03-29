from pydantic_settings import BaseSettings
from pydantic import field_validator
import os


class Settings(BaseSettings):
    ms_tenant_id:           str = ""
    ms_client_id:           str = ""
    ms_client_secret:       str = ""
    ms_sender_email:        str = ""
    anthropic_api_key:      str = ""
    apollo_api_key:         str = ""
    instantly_webhook_secret: str = ""
    review_mode:            bool = False
    auto_send_email:        bool = True
    default_from_name:      str  = "Kayode · PALM"
    default_email_template: str  = "warm"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


# Read directly from environment as fallback
# This ensures Railway env vars are always picked up
def get_settings() -> Settings:
    s = Settings()
    # Override with direct os.environ reads in case pydantic misses them
    if not s.anthropic_api_key:
        s.anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not s.apollo_api_key:
        s.apollo_api_key = os.environ.get("APOLLO_API_KEY", "")
    if not s.ms_tenant_id:
        s.ms_tenant_id = os.environ.get("MS_TENANT_ID", "")
    if not s.ms_client_id:
        s.ms_client_id = os.environ.get("MS_CLIENT_ID", "")
    if not s.ms_client_secret:
        s.ms_client_secret = os.environ.get("MS_CLIENT_SECRET", "")
    if not s.ms_sender_email:
        s.ms_sender_email = os.environ.get("MS_SENDER_EMAIL", "")
    return s


settings = get_settings()
