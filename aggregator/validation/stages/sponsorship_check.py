"""Extract sponsorship status from job page."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import ValidationHelper


class SponsorshipExtract(ValidationStage):
    name = "sponsorship_extract"
    description = "Extract sponsorship status (does not reject, only annotates)"
    outcome_key = None  # extraction only

    def check(self, ctx: JobContext) -> ValidationResult:
        if not ctx.soup:
            return self._skip("no page content")

        status = ValidationHelper.check_sponsorship_status(ctx.soup, company=ctx.company)
        return self._pass()

    def extract(self, ctx: JobContext) -> str:
        """Return sponsorship status string."""
        if not ctx.soup:
            return "Unknown"
        return ValidationHelper.check_sponsorship_status(ctx.soup, company=ctx.company)
