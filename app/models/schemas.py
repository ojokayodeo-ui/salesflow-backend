from pydantic import BaseModel
from typing import Optional, List, Any


class EmailRequest(BaseModel):
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
    """Matches Instantly.ai v2 webhook payload — handles both snake_case and camelCase fields."""
    # Event metadata
    event_type:         Optional[str] = None
    event:              Optional[str] = None
    timestamp:          Optional[str] = None

    # Prospect identity — snake_case (Instantly v2)
    first_name:         Optional[str] = None
    last_name:          Optional[str] = None
    email:              Optional[str] = None
    lead_email:         Optional[str] = None
    job_title:          Optional[str] = None
    job_level:          Optional[str] = None
    department:         Optional[str] = None
    linkedin_url:       Optional[str] = None
    phone:              Optional[str] = None
    city:               Optional[str] = None
    country:            Optional[str] = None
    state:              Optional[str] = None
    location:           Optional[str] = None
    headline:           Optional[str] = None
    summary:            Optional[str] = None

    # Prospect identity — camelCase (Instantly v1 / legacy)
    firstName:          Optional[str] = None
    lastName:           Optional[str] = None
    jobTitle:           Optional[str] = None
    jobLevel:           Optional[str] = None
    linkedIn:           Optional[str] = None

    # Company — snake_case (Instantly v2)
    company_name:       Optional[str] = None
    company_domain:     Optional[str] = None
    company_website:    Optional[str] = None
    company_linkedin_url: Optional[str] = None
    employee_count:     Optional[str] = None
    industry:           Optional[str] = None
    sub_industry:       Optional[str] = None
    company_description: Optional[str] = None
    website:            Optional[str] = None

    # Company — camelCase (Instantly v1 / legacy)
    companyName:        Optional[str] = None
    companyDomain:      Optional[str] = None
    companyWebsite:     Optional[str] = None
    companyHeadCount:   Optional[str] = None
    companyDescription: Optional[str] = None
    subIndustry:        Optional[str] = None

    # Campaign & reply
    campaign:           Optional[str] = None
    campaign_id:        Optional[str] = None
    campaign_name:      Optional[str] = None
    reply_text:         Optional[str] = None
    reply_text_snippet: Optional[str] = None
    reply_subject:      Optional[str] = None
    step:               Optional[int] = None

    model_config = {"extra": "allow"}

    def get_prospect_name(self) -> str:
        first = self.first_name or self.firstName or ""
        last  = self.last_name  or self.lastName  or ""
        return f"{first} {last}".strip() or "Unknown"

    def get_prospect_email(self) -> str:
        return self.email or self.lead_email or ""

    def get_company(self) -> str:
        return self.company_name or self.companyName or ""

    def get_domain(self) -> str:
        domain = self.company_domain or self.companyDomain or self.company_website or self.companyWebsite or ""
        domain = domain.replace("https://", "").replace("http://", "").replace("www.", "")
        return domain.rstrip("/")

    def get_website(self) -> str:
        w = self.company_website or self.companyWebsite or self.website or ""
        if w and not w.startswith("http"):
            w = "https://" + w
        return w

    def get_event(self) -> str:
        ev = self.event_type or self.event or ""
        if ev in ("lead_interested", "reply.positive", "reply", "email_reply"):
            return "reply.positive"
        return ev

    def get_reply(self) -> str:
        return self.reply_text or self.reply_text_snippet or ""

    def to_prospect_data(self) -> "ProspectData":
        # Build location from city/country/state if flat location field is empty
        location = self.location or ""
        if not location:
            parts = [p for p in [self.city, self.state, self.country] if p]
            location = ", ".join(parts)

        return ProspectData(
            name        = self.get_prospect_name(),
            email       = self.get_prospect_email(),
            company     = self.get_company(),
            domain      = self.get_domain(),
            website     = self.get_website(),
            job_title   = self.job_title   or self.jobTitle   or "",
            job_level   = self.job_level   or self.jobLevel   or "",
            linkedin    = self.linkedin_url or self.linkedIn   or "",
            location    = location,
            headcount   = self.employee_count or self.companyHeadCount or "",
            industry    = self.industry    or "",
            sub_industry= self.sub_industry or self.subIndustry or "",
            description = self.company_description or self.companyDescription or "",
            headline    = self.headline    or "",
            department  = self.department  or "",
        )


class ProspectData(BaseModel):
    name:         str
    email:        str
    company:      str
    domain:       Optional[str] = None
    website:      Optional[str] = None
    job_title:    Optional[str] = None
    job_level:    Optional[str] = None
    linkedin:     Optional[str] = None
    location:     Optional[str] = None
    headcount:    Optional[str] = None
    industry:     Optional[str] = None
    sub_industry: Optional[str] = None
    description:  Optional[str] = None
    headline:     Optional[str] = None
    department:   Optional[str] = None


class ICPData(BaseModel):
    industry:            str
    sub_niche:           str
    company_size:        str
    hq_country:          str
    target_titles:       List[str]
    pain_point:          str
    keywords:            List[str]
    apollo_employee_min: int
    apollo_employee_max: int
    company_age_years:   str
    buying_signal:       str
    # Optional enriched fields
    revenue_range:       Optional[str] = None
    secondary_titles:    Optional[List[str]] = None
    technologies:        Optional[List[str]] = None
    growth_signals:      Optional[List[str]] = None
    cold_email_hook:     Optional[str] = None
    value_proposition:   Optional[str] = None
    ideal_list_size:     Optional[int] = 100
    segmentation:        Optional[List[str]] = None


class OutlookAccountCreate(BaseModel):
    tenant_id:     str
    client_id:     str
    client_secret: str
    sender_email:  str
    display_name:  str = ""
    is_default:    bool = False


class OutlookAccountUpdate(BaseModel):
    tenant_id:     Optional[str] = None
    client_id:     Optional[str] = None
    client_secret: Optional[str] = None
    sender_email:  Optional[str] = None
    display_name:  Optional[str] = None
    is_default:    Optional[bool] = None


class OutlookAccountResponse(BaseModel):
    id:           str
    tenant_id:    str
    client_id:    str
    sender_email: str
    display_name: str
    is_default:   bool
    created_at:   str
    updated_at:   str


class PipelineRunRequest(BaseModel):
    prospect:  ProspectData
    auto_send: bool = False


class PipelineRunResponse(BaseModel):
    success:        bool
    icp:            Optional[ICPData] = None
    apollo_filters: Optional[dict] = None
    email_status:   Optional[str] = None
    error:          Optional[str] = None
