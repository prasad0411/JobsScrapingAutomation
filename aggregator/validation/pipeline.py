"""
Validation Pipeline — runs composable stages in declared order.

Usage:
    pipeline = ValidationPipeline.from_config()
    ctx = JobContext(title="...", company="...", soup=soup)
    result = pipeline.run(ctx)
    if result.rejected:
        print(f"Rejected: {result.reason} by {result.stage_name}")
"""
import logging
import time
import yaml
import os
from dataclasses import dataclass, field
from typing import List, Optional

from aggregator.validation.stages.base import (
    ValidationStage, JobContext, ValidationResult, Decision
)

log = logging.getLogger(__name__)

# Registry of available stages
STAGE_REGISTRY = {}


def register_stage(cls):
    """Decorator to register a validation stage."""
    STAGE_REGISTRY[cls.name] = cls
    return cls


def _register_all():
    """Import and register all built-in stages."""
    from aggregator.validation.stages.title_check import TitleValidation
    from aggregator.validation.stages.location_check import InternationalCheck
    from aggregator.validation.stages.page_restrictions import PageRestrictions
    from aggregator.validation.stages.age_check import AgeCheck
    from aggregator.validation.stages.salary_check import SalaryCheck
    from aggregator.validation.stages.sponsorship_check import SponsorshipExtract

    for cls in [TitleValidation, InternationalCheck, PageRestrictions,
                AgeCheck, SalaryCheck, SponsorshipExtract]:
        STAGE_REGISTRY[cls.name] = cls


@dataclass
class PipelineResult:
    """Aggregate result of running all stages."""
    accepted: bool
    rejection_reason: Optional[str] = None
    rejection_stage: Optional[str] = None
    stage_results: List[ValidationResult] = field(default_factory=list)
    total_time_ms: float = 0.0

    @property
    def rejected(self) -> bool:
        return not self.accepted


class ValidationPipeline:
    """
    Runs validation stages in order. First rejection stops the pipeline.
    
    Stages are loaded from YAML config or constructed programmatically.
    Each stage is independently testable.
    """

    def __init__(self, stages: List[ValidationStage] = None):
        self.stages = stages or []
        self._metrics = {"total_runs": 0, "total_rejects": 0, "stage_rejects": {}}

    @classmethod
    def from_config(cls, config_path: str = None) -> "ValidationPipeline":
        """Load pipeline from YAML config file."""
        _register_all()

        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), "config.yaml"
            )

        if os.path.exists(config_path):
            with open(config_path) as f:
                config = yaml.safe_load(f)
            stages = []
            for stage_def in config.get("stages", []):
                if isinstance(stage_def, str):
                    stage_name = stage_def
                    stage_params = {}
                elif isinstance(stage_def, dict):
                    stage_name = stage_def.get("name", "")
                    stage_params = {k: v for k, v in stage_def.items() if k != "name"}
                else:
                    continue

                if stage_name in STAGE_REGISTRY:
                    stage_cls = STAGE_REGISTRY[stage_name]
                    try:
                        stage = stage_cls(**stage_params) if stage_params else stage_cls()
                    except TypeError:
                        stage = stage_cls()
                    stages.append(stage)
                else:
                    log.warning(f"Unknown validation stage: {stage_name}")
            return cls(stages=stages)
        else:
            # Default pipeline if no config
            return cls.default()

    @classmethod
    def default(cls) -> "ValidationPipeline":
        """Create default pipeline with all stages in recommended order."""
        _register_all()
        from aggregator.validation.stages.title_check import TitleValidation
        from aggregator.validation.stages.location_check import InternationalCheck
        from aggregator.validation.stages.page_restrictions import PageRestrictions
        from aggregator.validation.stages.age_check import AgeCheck
        from aggregator.validation.stages.salary_check import SalaryCheck

        return cls(stages=[
            TitleValidation(),
            InternationalCheck(),
            PageRestrictions(),
            AgeCheck(),
            SalaryCheck(),
        ])

    def run(self, ctx: JobContext) -> PipelineResult:
        """
        Run all stages. Stop at first rejection.
        Returns PipelineResult with full trace.
        """
        start = time.monotonic()
        self._metrics["total_runs"] += 1
        results = []

        for stage in self.stages:
            try:
                result = stage.check(ctx)
                result.stage_name = stage.name
                results.append(result)

                if result.rejected:
                    self._metrics["total_rejects"] += 1
                    self._metrics["stage_rejects"][stage.name] = (
                        self._metrics["stage_rejects"].get(stage.name, 0) + 1
                    )
                    elapsed = (time.monotonic() - start) * 1000
                    log.debug(
                        f"Pipeline REJECT at {stage.name}: {result.reason} "
                        f"({elapsed:.1f}ms, {len(results)} stages checked)"
                    )
                    return PipelineResult(
                        accepted=False,
                        rejection_reason=result.reason,
                        rejection_stage=stage.name,
                        stage_results=results,
                        total_time_ms=elapsed,
                    )
            except Exception as e:
                log.warning(f"Stage {stage.name} failed: {e}")
                results.append(ValidationResult(
                    Decision.SKIP, reason=f"error: {e}", stage_name=stage.name
                ))

        elapsed = (time.monotonic() - start) * 1000
        return PipelineResult(
            accepted=True,
            stage_results=results,
            total_time_ms=elapsed,
        )

    @property
    def metrics(self) -> dict:
        """Return pipeline performance metrics."""
        return {
            **self._metrics,
            "rejection_rate": (
                self._metrics["total_rejects"] / self._metrics["total_runs"]
                if self._metrics["total_runs"] > 0 else 0
            ),
            "stage_count": len(self.stages),
            "stage_names": [s.name for s in self.stages],
        }

    def __repr__(self):
        names = " → ".join(s.name for s in self.stages)
        return f"ValidationPipeline([{names}])"
