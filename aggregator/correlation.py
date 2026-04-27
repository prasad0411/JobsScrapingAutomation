"""
Correlation ID system for end-to-end job tracing.

Every aggregator run gets a run_id. Every job gets a job_trace_id.
These propagate through validation, sheet writes, and outreach.

Usage:
    from aggregator.correlation import TraceContext, traced
    
    with TraceContext(run_id="run_20260427_080000") as ctx:
        ctx.set_job("Google", "SDE Intern", "https://...")
        logger.info("Processing", extra=ctx.extra())
"""
import uuid
import time
import threading
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

_thread_local = threading.local()


@dataclass
class TraceContext:
    """Carries correlation IDs through the processing pipeline."""
    run_id: str = ""
    job_trace_id: str = ""
    company: str = ""
    title: str = ""
    source: str = ""
    stage: str = ""
    _start_time: float = 0.0

    def __enter__(self):
        self._start_time = time.monotonic()
        _thread_local.trace = self
        return self

    def __exit__(self, *args):
        _thread_local.trace = None

    def set_job(self, company: str, title: str, url: str = "", source: str = ""):
        """Set job-level context. Generates a unique trace ID."""
        self.company = company
        self.title = title
        self.source = source
        self.job_trace_id = f"job_{uuid.uuid4().hex[:8]}"

    def set_stage(self, stage: str):
        self.stage = stage

    def extra(self) -> dict:
        """Return structured fields for logging."""
        return {
            "run_id": self.run_id,
            "job_trace_id": self.job_trace_id,
            "company": self.company,
            "title": self.title[:50],
            "source": self.source,
            "stage": self.stage,
            "elapsed_ms": round((time.monotonic() - self._start_time) * 1000, 1),
        }

    @staticmethod
    def current() -> Optional["TraceContext"]:
        return getattr(_thread_local, "trace", None)


def generate_run_id() -> str:
    """Generate a unique run ID for this aggregator execution."""
    ts = time.strftime("%Y%m%d_%H%M%S")
    return f"run_{ts}_{uuid.uuid4().hex[:6]}"


class StructuredFormatter(logging.Formatter):
    """JSON-structured log formatter with correlation IDs."""
    
    def format(self, record):
        ctx = TraceContext.current()
        base = super().format(record)
        if ctx and ctx.run_id:
            prefix = f"[{ctx.run_id}]"
            if ctx.job_trace_id:
                prefix += f"[{ctx.job_trace_id}]"
            if ctx.stage:
                prefix += f"[{ctx.stage}]"
            return f"{prefix} {base}"
        return base
