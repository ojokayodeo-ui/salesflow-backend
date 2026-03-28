from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ms_tenant_id:     str = ""
    ms_client_id:     str = ""
    ms_client_secret: str = ""
    ms_sender_email:  str = ""
    anthropic_api_key: str = ""
    apollo_api_key: str = ""
    instantly_webhook_secret: str = ""
    review_mode:            bool = False
    auto_send_email:        bool = True
    default_from_name:      str  = "Kayode · SalesFlow"
    default_email_template: str  = "warm"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
