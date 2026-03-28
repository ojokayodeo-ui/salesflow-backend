from pydantic import BaseModel, EmailStr
from typing import Optional, List


class EmailRequest(BaseModel):
    """Payload for sending a delivery email via Outlook."""
    to_email: str
    to_name: str
    from_name: str
    subject: str
    body: str
    attach_csv: bool = True
    csv_data: Optional[str] = None        # Base64-encoded CSV string
    csv_filename: str = "leads_100.csv"


class EmailResponse(BaseModel):
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


class InstantlyWebhookPayload(BaseModel):
    """Shape of the positive-reply event fired by Instantly.ai."""
    event: str
    timestamp: str
    campaign: Optional[str] = None
    prospect: "ProspectData"
    reply_body: Optional[str] = None


class ProspectData(BaseModel):
    name: str
    email: str
    company: str
    domain: Optional[str] = None


class ICPData(BaseModel):
    industry: str
    sub_niche: str
    company_size: str
    hq_country: str
    target_titles: List[str]
    pain_point: str
    keywords: List[str]
    apollo_employee_min: int
    apollo_employee_max: int
    company_age_years: str
    buying_signal: str


class PipelineRunRequest(BaseModel):
    prospect: ProspectData
    auto_send: bool = False   # If True, fire email without manual confirmation


class PipelineRunResponse(BaseModel):
    success: bool
    icp: Optional[ICPData] = None
    apollo_filters: Optional[dict] = None
    email_status: Optional[str] = None
    error: Optional[str] = None


InstantlyWebhookPayload.model_rebuild()
