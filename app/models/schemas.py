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
    """Matches Instantly.ai's actual flat webhook payload format."""
    event_type:         Optional[str] = None
    event:              Optional[str] = None
    timestamp:          Optional[str] = None
    firstName:          Optional[str] = None
    lastName:           Optional[str] = None
    email:              Optional[str] = None
    lead_email:         Optional[str] = None
    jobTitle:           Optional[str] = None
    jobLevel:           Optional[str] = None
    department:         Optional[str] = None
    linkedIn:           Optional[str] = None
    location:           Optional[str] = None
    companyName:        Optional[str] = None
    companyDomain:      Optional[str] = None
    companyWebsite:     Optional[str] = None
    companyHeadCount:   Optional[str] = None
    companyDescription: Optional[str] = None
    industry:           Optional[str] = None
    subIndustry:        Optional[str] = None
    summary:            Optional[str] = None
    headline:           Optional[str] = None
    campaign:           Optional[str] = None
    campaign_id:        Optional[str] = None
    campaign_name:      Optional[str] = None
    reply_text:         Optional[str] = None
    reply_text_snippet: Optional[str] = None
    reply_subject:      Optional[str] = None
    step:               Optional[int] = None

    model_config = {"extra": "allow"}

    def get_prospect_name(self) -> str:
        first = self.firstName or ""
        last  = self.lastName or ""
        return f"{first} {last}".strip() or "Unknown"

    def get_prospect_email(self) -> str:
        return self.email or self.lead_email or ""

    def get_company(self) -> str:
        return self.companyName or ""

    def get_domain(self) -> str:
        domain = self.companyDomain or self.companyWebsite or ""
        domain = domain.replace("https://", "").replace("http://", "").replace("www.", "")
        return domain.rstrip("/")

    def get_website(self) -> str:
        w = self.companyWebsite or ""
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
        return ProspectData(
            name        = self.get_prospect_name(),
            email       = self.get_prospect_email(),
            company     = self.get_company(),
            domain      = self.get_domain(),
            website     = self.get_website(),
            job_title   = self.jobTitle or "",
            linkedin    = self.linkedIn or "",
            location    = self.location or "",
            headcount   = self.companyHeadCount or "",
            industry    = self.industry or "",
            sub_industry= self.subIndustry or "",
            description = self.companyDescription or "",
            headline    = self.headline or "",
        )


class ProspectData(BaseModel):
    name:         str
    email:        str
    company:      str
    domain:       Optional[str] = None
    website:      Optional[str] = None
    job_title:    Optional[str] = None
    linkedin:     Optional[str] = None
    location:     Optional[str] = None
    headcount:    Optional[str] = None
    industry:     Optional[str] = None
    sub_industry: Optional[str] = None
    description:  Optional[str] = None
    headline:     Optional[str] = None


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


class PipelineRunRequest(BaseModel):
    prospect:  ProspectData
    auto_send: bool = False


class PipelineRunResponse(BaseModel):
    success:        bool
    icp:            Optional[ICPData] = None
    apollo_filters: Optional[dict] = None
    email_status:   Optional[str] = None
    error:          Optional[str] = None
