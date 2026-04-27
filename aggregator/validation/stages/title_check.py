"""Validate job title — must be tech/CS role and internship."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import TitleProcessor


class TitleValidation(ValidationStage):
    name = "title_validation"
    description = "Reject non-tech titles, non-internship roles, wrong season"
    outcome_key = "skipped_invalid_title"

    def check(self, ctx: JobContext) -> ValidationResult:
        if not ctx.title or ctx.title == "Unknown":
            return self._skip("no title")

        # Check if valid CS/tech title
        is_valid, reason = TitleProcessor.is_valid_job_title(ctx.title)
        if not is_valid:
            return self._reject(f"Invalid title: {reason}")

        # Check if internship/co-op
        is_intern, intern_reason = TitleProcessor.is_internship_role(
            ctx.title, github_category=ctx.github_category
        )
        if not is_intern:
            return self._reject(intern_reason)

        # Season check
        season_ok, season_reason = TitleProcessor.check_season_requirement(ctx.title)
        if not season_ok:
            return self._reject(season_reason)

        return self._pass()
