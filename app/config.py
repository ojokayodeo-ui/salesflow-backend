import os

class Settings:
    def __init__(self):
        self.anthropic_api_key        = os.environ.get("ANTHROPIC_API_KEY", "")
        self.apollo_api_key           = os.environ.get("APOLLO_API_KEY", "")
        self.ms_tenant_id             = os.environ.get("MS_TENANT_ID", "")
        self.ms_client_id             = os.environ.get("MS_CLIENT_ID", "")
        self.ms_client_secret         = os.environ.get("MS_CLIENT_SECRET", "")
        self.ms_sender_email          = os.environ.get("MS_SENDER_EMAIL", "")
        self.instantly_webhook_secret = os.environ.get("INSTANTLY_WEBHOOK_SECRET", "")
        self.instantly_api_key        = os.environ.get("INSTANTLY_API_KEY", "")
        self.apify_api_token          = os.environ.get("APIFY_API_TOKEN", "")
        self.review_mode              = os.environ.get("REVIEW_MODE", "false").lower() == "true"
        self.auto_send_email          = os.environ.get("AUTO_SEND_EMAIL", "true").lower() == "true"
        self.default_from_name        = os.environ.get("DEFAULT_FROM_NAME", "Kayode · PALM")
        self.default_email_template   = os.environ.get("DEFAULT_EMAIL_TEMPLATE", "warm")
        self.backend_url              = os.environ.get("BACKEND_URL", "").rstrip("/")

settings = Settings()
