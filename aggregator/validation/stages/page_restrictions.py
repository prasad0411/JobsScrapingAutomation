"""Check page-level restrictions — clearance, citizenship, degree, etc."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import ValidationHelper


class PageRestrictions(ValidationStage):
    name = "page_restrictions"
    description = "Check clearance, citizenship, degree, undergrad/PhD requirements"
    outcome_key = "skipped_page_restriction"

    def check(self, ctx: JobContext) -> ValidationResult:
        if not ctx.soup:
            return self._skip("no page content")

        decision, reason, _ = ValidationHelper.check_page_restrictions(ctx.soup)
        if decision == "REJECT":
            return self._reject(reason)

        return self._pass()
