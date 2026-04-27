"""Reject jobs paying under minimum hourly rate."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import ValidationHelper


class SalaryCheck(ValidationStage):
    name = "salary_check"
    description = "Reject jobs paying under $25/hr"
    outcome_key = "skipped_low_salary"

    def check(self, ctx: JobContext) -> ValidationResult:
        if not ctx.soup:
            return self._skip("no page content")

        decision, reason = ValidationHelper.check_salary_requirement(ctx.soup)
        if decision == "REJECT":
            return self._reject(reason)

        return self._pass()
