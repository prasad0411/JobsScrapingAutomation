"""Test the validation pipeline architecture."""
import pytest
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aggregator.validation.pipeline import ValidationPipeline
from aggregator.validation.stages.base import (
    ValidationStage, JobContext, ValidationResult, Decision
)


class AlwaysPass(ValidationStage):
    name = "always_pass"
    def check(self, ctx):
        return self._pass()


class AlwaysReject(ValidationStage):
    name = "always_reject"
    def check(self, ctx):
        return self._reject("test rejection")


class TestPipelineBasics:
    """Test pipeline construction and execution."""

    def test_empty_pipeline_accepts(self):
        pipeline = ValidationPipeline(stages=[])
        result = pipeline.run(JobContext(title="Test"))
        assert result.accepted

    def test_all_pass_accepts(self):
        pipeline = ValidationPipeline(stages=[AlwaysPass(), AlwaysPass()])
        result = pipeline.run(JobContext(title="Test"))
        assert result.accepted
        assert len(result.stage_results) == 2

    def test_reject_stops_pipeline(self):
        pipeline = ValidationPipeline(stages=[AlwaysPass(), AlwaysReject(), AlwaysPass()])
        result = pipeline.run(JobContext(title="Test"))
        assert result.rejected
        assert result.rejection_stage == "always_reject"
        assert result.rejection_reason == "test rejection"
        # Third stage should NOT run
        assert len(result.stage_results) == 2

    def test_metrics_tracked(self):
        pipeline = ValidationPipeline(stages=[AlwaysReject()])
        pipeline.run(JobContext(title="Test"))
        pipeline.run(JobContext(title="Test2"))
        assert pipeline.metrics["total_runs"] == 2
        assert pipeline.metrics["total_rejects"] == 2

    def test_timing_recorded(self):
        pipeline = ValidationPipeline(stages=[AlwaysPass()])
        result = pipeline.run(JobContext(title="Test"))
        assert result.total_time_ms >= 0


class TestPipelineWithRealStages:
    """Test with actual validation stages."""

    def test_default_pipeline_loads(self):
        pipeline = ValidationPipeline.default()
        assert len(pipeline.stages) >= 4
        names = [s.name for s in pipeline.stages]
        assert "title_validation" in names
        assert "international_check" in names

    def test_valid_job_passes(self):
        pipeline = ValidationPipeline.default()
        ctx = JobContext(
            title="Software Engineering Intern",
            company="Google",
            location="Mountain View, CA",
        )
        result = pipeline.run(ctx)
        assert result.accepted

    def test_non_tech_title_rejected(self):
        pipeline = ValidationPipeline.default()
        ctx = JobContext(
            title="Marketing Intern",
            company="Acme",
            location="Boston, MA",
        )
        result = pipeline.run(ctx)
        assert result.rejected
        assert result.rejection_stage == "title_validation"

    def test_international_rejected(self):
        pipeline = ValidationPipeline.default()
        ctx = JobContext(
            title="Software Intern",
            company="SAP",
            location="Toronto, ON, Canada",
        )
        result = pipeline.run(ctx)
        assert result.rejected
        assert "international" in result.rejection_stage.lower() or "Canada" in (result.rejection_reason or "")

    def test_from_config_loads(self):
        pipeline = ValidationPipeline.from_config()
        assert len(pipeline.stages) >= 4

    def test_repr(self):
        pipeline = ValidationPipeline.default()
        r = repr(pipeline)
        assert "ValidationPipeline" in r
        assert "title_validation" in r


class TestJobContext:
    """Test JobContext construction."""

    def test_basic_construction(self):
        ctx = JobContext(title="Test", company="Acme")
        assert ctx.title == "Test"
        assert ctx.company == "Acme"
        assert ctx.location == ""
        assert ctx.page_text == ""

    def test_soup_auto_extracts_text(self, make_soup):
        soup = make_soup("<html><body>Hello World</body></html>")
        ctx = JobContext(title="Test", soup=soup)
        assert "hello world" in ctx.page_text  # page_text is lowercased


class TestValidationResult:
    """Test ValidationResult properties."""

    def test_pass_result(self):
        r = ValidationResult(Decision.PASS, stage_name="test")
        assert r.passed
        assert not r.rejected

    def test_reject_result(self):
        r = ValidationResult(Decision.REJECT, reason="bad", stage_name="test")
        assert r.rejected
        assert not r.passed
        assert r.reason == "bad"

    def test_skip_result(self):
        r = ValidationResult(Decision.SKIP, stage_name="test")
        assert not r.passed
        assert not r.rejected
