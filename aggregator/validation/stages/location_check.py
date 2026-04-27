"""Reject international locations — US only."""
from aggregator.validation.stages.base import ValidationStage, JobContext, ValidationResult
from aggregator.processors import LocationProcessor


class InternationalCheck(ValidationStage):
    name = "international_check"
    description = "Reject jobs outside the United States"
    outcome_key = "skipped_international"

    def check(self, ctx: JobContext) -> ValidationResult:
        # Check location + URL + title + page for international signals
        result = LocationProcessor.check_if_international(
            ctx.location, soup=ctx.soup, url=ctx.url, title=ctx.title
        )
        if result:
            return self._reject(result)

        # Check company name for known international companies
        company_intl = LocationProcessor.check_company_for_international(ctx.company)
        if company_intl:
            return self._reject(company_intl)

        return self._pass()
