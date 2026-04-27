"""Base class for all validation stages."""
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class Decision(Enum):
    PASS = "pass"
    REJECT = "reject"
    SKIP = "skip"  # stage doesn't apply


@dataclass
class ValidationResult:
    """Result of a single validation stage."""
    decision: Decision
    reason: Optional[str] = None
    stage_name: str = ""
    details: Optional[dict] = None

    @property
    def rejected(self) -> bool:
        return self.decision == Decision.REJECT

    @property
    def passed(self) -> bool:
        return self.decision == Decision.PASS


@dataclass
class JobContext:
    """All data available to validation stages."""
    title: str = ""
    company: str = ""
    location: str = ""
    url: str = ""
    source: str = ""
    soup: object = None           # BeautifulSoup
    page_text: str = ""           # first 15000 chars of page
    github_category: str = ""
    job_type: str = "Internship"

    def __post_init__(self):
        if self.soup and not self.page_text:
            self.page_text = self.soup.get_text()[:15000].lower()


class ValidationStage:
    """
    Base class for all validation stages.
    
    Subclasses implement check(ctx) -> ValidationResult.
    Each stage is independently testable and composable.
    """
    name: str = "base"
    description: str = ""
    # Outcome counter key for aggregator stats
    outcome_key: str = "skipped_validation"

    def check(self, ctx: JobContext) -> ValidationResult:
        """Override in subclass. Return PASS, REJECT, or SKIP."""
        raise NotImplementedError

    def _pass(self) -> ValidationResult:
        return ValidationResult(Decision.PASS, stage_name=self.name)

    def _reject(self, reason: str, details: dict = None) -> ValidationResult:
        return ValidationResult(Decision.REJECT, reason=reason, stage_name=self.name, details=details)

    def _skip(self, reason: str = "not applicable") -> ValidationResult:
        return ValidationResult(Decision.SKIP, reason=reason, stage_name=self.name)
