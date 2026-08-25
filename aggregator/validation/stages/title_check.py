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

        # Internship check — ONLY for internship-type roles. This stage used
        # to reject anything that was not an internship, which would have
        # thrown away every full-time new-grad SDE role. The goal is a
        # full-time sponsoring offer, so full-time must pass here.
        _jt = (ctx.job_type or "").strip().lower()
        _wants_intern = _jt in ("internship", "co-op", "coop", "intern", "")

        # Full-time roles skip the internship gate, which previously did
        # double duty as a non-tech filter. Without it, titles like
        # "Accounting Analyst" pass, so apply the tech check explicitly.
        if not _wants_intern:
            _t = ctx.title.lower()
            _TECH = ("software", "engineer", "developer", "sde", "swe",
                     "data scien", "data engineer", "machine learning", " ml ",
                     "backend", "back end", "frontend", "front end", "full stack",
                     "fullstack", "devops", "sre", "site reliability", "platform",
                     "infrastructure", "security engineer", "computer vision",
                     "research scientist", "programmer", "architect", "analytics engineer")
            _NON_TECH = ("accounting", "accountant", "marketing", "sales",
                         "recruiter", "recruiting", "hr ", "human resources",
                         "nursing", "nurse", "supply chain", "audit", "auditor",
                         "paralegal", "legal", "counsel", "teacher", "clinical")
            if any(k in _t for k in _NON_TECH):
                return self._reject(f"Non-tech full-time role: {ctx.title[:50]}")
            if not any(k in _t for k in _TECH):
                return self._reject(f"No tech signal in full-time title: {ctx.title[:50]}")

        if _wants_intern:
            is_intern, intern_reason = TitleProcessor.is_internship_role(
                ctx.title, github_category=ctx.github_category
            )
            if not is_intern and _jt != "":
                return self._reject(intern_reason)

            # Season check only applies to internships
            season_ok, season_reason = TitleProcessor.check_season_requirement(ctx.title)
            if not season_ok:
                return self._reject(season_reason)

        return self._pass()
