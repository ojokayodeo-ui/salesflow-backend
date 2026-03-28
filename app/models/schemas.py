from pydantic import BaseModel, field_validator
from typing import Optional, List, Any


class EmailRequest(BaseModel):
    """Payload for sending a delivery email via Outlook."""
    to_email: str
    to_name: str
    from_name: str
    subject: str
    body: str
    attach_csv: bool = True
    csv_data: Optional[str] = None
    csv_filename: str = "leads_100.csv"


class EmailResponse(BaseModel):
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


class InstantlyWebhookPayload(BaseModel):
    """
    Matches Instantly.ai's actual webhook payload format.
    Instantly sends a flat JSON object with these fields.
    """
    # Event identification
    event_type: Optional[str] = None       # "lead_interested"
    event: Optional[str] = None            # fallback field name

    # Timestamps
    timestamp: Optional[str] = None

    # Prospect personal info (Instantly sends these separately)
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    lead_email: Optional[str] = None       # alternate email field

    # Prospect job info
    jobTitle: Optional[str] = None
    jobLevel: Optional[str] = None
    department: Optional[str] = None
    linkedIn: Optional[str] = None
    location: Optional[str] = None

    # Company info
    companyName: Optional[str] = None
    companyDomain: Optional[str] = None
    companyWebsite: Optional[str] = None
    companyHeadCount: Optional[str] = None
    companyDescription: Optional[str] = None
    industry: Optional[str] = None
    subIndustry: Optional[str] = None

    # Campaign info
    campaign: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None

    # Reply content
    reply_text: Optional[str] = None
    reply_text_snippet: Optional[str] = None
    reply_subject: Optional[str] = None

    # Other fields Instantly sends
    summary: Optional[str] = None
    headline: Optional[str] = None
    step: Optional[int] = None

    # Allow any extra fields Instantly might send
    model_config = {"extra": "allow"}

    def get_prospect_name(self) -> str:
        """Build full name from firstName + lastName."""
        first = self.firstName or ""
        last = self.lastName or ""
        full = f"{first} {last}".strip()
        return full or "Unknown"

    def get_prospect_email(self) -> str:
        """Get prospect email — Instantly uses both 'email' and 'lead_email'."""
        return self.email or self.lead_email or ""

    def get_company(self) -> str:
        return self.companyName or ""

    def get_domain(self) -> str:
        domain = self.companyDomain or self.companyWebsite or ""
        # Strip http:// https:// www.
        domain = domain.replace("https://", "").replace("http://", "").replace("www.", "")
        return domain.rstrip("/")

    def get_event(self) -> str:
        """Normalise event type to our internal format."""
        ev = self.event_type or self.event or ""
        if ev in ("lead_interested", "reply.positive", "reply", "email_reply"):
            return "reply.positive"
        return ev

    def get_reply(self) -> str:
        return self.reply_text or self.reply_text_snippet or ""

    def to_prospect_data(self) -> "ProspectData":
        return ProspectData(
            name    = self.get_prospect_name(),
            email   = self.get_prospect_email(),
            company = self.get_company(),
            domain  = self.get_domain(),
        )


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
    auto_send: bool = False


class PipelineRunResponse(BaseModel):
    success: bool
    icp: Optional[ICPData] = None
    apollo_filters: Optional[dict] = None
    email_status: Optional[str] = None
    error: Optional[str] = None
