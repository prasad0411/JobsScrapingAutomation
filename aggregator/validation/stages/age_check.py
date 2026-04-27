"""Reject jobs posted more than N days ago."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import ValidationHelper

try:
    from aggregator.config import PAGE_AGE_THRESHOLD_DAYS
except ImportError:
    PAGE_AGE_THRESHOLD_DAYS = 3


class AgeCheck(ValidationStage):
    name = "age_check"
    description = f"Reject jobs posted more than {PAGE_AGE_THRESHOLD_DAYS} days ago"
    outcome_key = "skipped_too_old"

    def __init__(self, max_age_days: int = None):
        self.max_age = max_age_days or PAGE_AGE_THRESHOLD_DAYS

    def check(self, ctx: JobContext) -> ValidationResult:
        if not ctx.soup:
            return self._skip("no page content")

        page_age = ValidationHelper.extract_page_age(ctx.soup)
        if page_age is not None and page_age > self.max_age:
            return self._reject(
                f"Posted {page_age} days ago (max {self.max_age})",
                details={"age_days": page_age, "max_days": self.max_age}
            )

        return self._pass()
