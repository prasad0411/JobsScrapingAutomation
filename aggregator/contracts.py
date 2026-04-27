"""
Data Contracts — typed schemas for pipeline data flow.

Ensures consistent field names and types across all 18 modules.
Fails fast with clear errors instead of silent data corruption.

Usage:
    job = JobContract.validate({
        "company": "Google",
        "title": "SDE Intern",
        "url": "https://...",
    })
    # Raises ContractViolation if required fields missing or wrong type
"""
import re
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from enum import Enum

log = logging.getLogger(__name__)


class Outcome(Enum):
    VALID = "valid"
    DISCARDED = "discarded"
    REVIEWED = "reviewed"
    DUPLICATE = "duplicate"


class ResumeType(Enum):
    SDE = "SDE"
    ML = "ML"
    DA = "DA"


class ContractViolation(Exception):
    """Raised when data doesn't match the expected contract."""
    def __init__(self, field: str, message: str, value=None):
        self.field = field
        self.value = value
        super().__init__(f"Contract violation [{field}]: {message} (got: {repr(value)[:50]})")


@dataclass
class JobContract:
    """
    Strict typed schema for a job posting flowing through the pipeline.
    
    All modules must produce/consume data matching this contract.
    Required fields raise ContractViolation if missing or invalid.
    """
    # Required
    company: str = ""
    title: str = ""
    url: str = ""

    # Optional with defaults
    location: str = "Unknown"
    source: str = "Unknown"
    outcome: str = "valid"
    rejection_reason: str = ""
    resume_type: str = "SDE"
    job_type: str = "Internship"
    job_id: str = "N/A"
    remote: str = "Unknown"
    sponsorship: str = "Unknown"
    entry_date: str = ""
    page_age_days: Optional[int] = None
    salary_low: Optional[float] = None
    salary_high: Optional[float] = None

    def __post_init__(self):
        """Validate on construction."""
        self._validate()

    def _validate(self):
        """Validate all fields against contract rules."""
        # Required fields
        if not self.company or self.company == "Unknown":
            raise ContractViolation("company", "Company name is required", self.company)
        if not self.title or self.title == "Unknown":
            raise ContractViolation("title", "Job title is required", self.title)
        if not self.url or not self.url.startswith("http"):
            raise ContractViolation("url", "Valid URL is required", self.url)

        # Type checks
        if not isinstance(self.company, str):
            raise ContractViolation("company", "Must be string", type(self.company))
        if not isinstance(self.title, str):
            raise ContractViolation("title", "Must be string", type(self.title))

        # Length checks
        if len(self.company) > 100:
            raise ContractViolation("company", "Too long (max 100 chars)", len(self.company))
        if len(self.title) > 200:
            raise ContractViolation("title", "Too long (max 200 chars)", len(self.title))

        # Enum validation
        if self.resume_type not in ("SDE", "ML", "DA"):
            raise ContractViolation("resume_type", "Must be SDE/ML/DA", self.resume_type)
        if self.outcome not in ("valid", "discarded", "reviewed", "duplicate"):
            raise ContractViolation("outcome", "Invalid outcome", self.outcome)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "JobContract":
        """Create from dict with validation. Raises ContractViolation on invalid data."""
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)

    @classmethod
    def validate(cls, data: dict) -> "JobContract":
        """Validate a dict against the contract. Returns validated JobContract."""
        return cls.from_dict(data)

    @classmethod
    def safe_validate(cls, data: dict) -> Optional["JobContract"]:
        """Validate without raising — returns None on violation."""
        try:
            return cls.from_dict(data)
        except ContractViolation as e:
            log.debug(f"Contract validation failed: {e}")
            return None

    @classmethod
    def coerce(cls, data: dict) -> "JobContract":
        """
        Best-effort coercion — fix common issues instead of rejecting.
        Maps legacy field names to current schema.
        """
        # Legacy field name mapping
        aliases = {
            "co": "company",
            "name": "company",
            "ti": "title",
            "loc": "location",
            "remote_status": "remote",
        }
        normalized = {}
        for k, v in data.items():
            key = aliases.get(k, k)
            normalized[key] = v

        # Coerce types
        for str_field in ("company", "title", "url", "location", "source"):
            if str_field in normalized and normalized[str_field] is None:
                normalized[str_field] = "Unknown"
            if str_field in normalized:
                normalized[str_field] = str(normalized[str_field]).strip()

        return cls.from_dict(normalized)

