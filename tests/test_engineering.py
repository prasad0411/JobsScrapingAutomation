"""Tests for engineering maturity features: correlation, circuit breaker, retry, hot-reload, contracts."""
import pytest
import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestCorrelation:
    """Test correlation ID system."""

    def test_trace_context(self):
        from aggregator.correlation import TraceContext
        with TraceContext(run_id="test_run") as ctx:
            ctx.set_job("Google", "SDE Intern", source="SimplifyJobs")
            extra = ctx.extra()
            assert extra["run_id"] == "test_run"
            assert extra["company"] == "Google"
            assert extra["job_trace_id"].startswith("job_")

    def test_current_context(self):
        from aggregator.correlation import TraceContext
        with TraceContext(run_id="ctx_test") as ctx:
            current = TraceContext.current()
            assert current is not None
            assert current.run_id == "ctx_test"
        assert TraceContext.current() is None

    def test_generate_run_id(self):
        from aggregator.correlation import generate_run_id
        rid = generate_run_id()
        assert rid.startswith("run_")
        assert len(rid) > 15

    def test_structured_formatter(self):
        import logging
        from aggregator.correlation import TraceContext, StructuredFormatter
        formatter = StructuredFormatter("%(message)s")
        record = logging.LogRecord("test", logging.INFO, "", 0, "hello", (), None)
        # Without context
        assert formatter.format(record) == "hello"
        # With context
        with TraceContext(run_id="fmt_test") as ctx:
            result = formatter.format(record)
            assert "[fmt_test]" in result


class TestCircuitBreaker:
    """Test generalized circuit breaker."""

    def test_starts_closed(self):
        from aggregator.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == State.CLOSED
        assert cb.allow_request()

    def test_trips_after_threshold(self):
        from aggregator.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == State.CLOSED
        cb.record_failure()
        assert cb.state == State.OPEN
        assert not cb.allow_request()

    def test_recovery_after_timeout(self):
        from aggregator.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker(name="test", failure_threshold=2, reset_timeout=0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == State.OPEN
        time.sleep(0.01)
        assert cb.allow_request()  # transitions to HALF_OPEN
        assert cb.state == State.HALF_OPEN

    def test_closes_after_success_in_half_open(self):
        from aggregator.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker(name="test", failure_threshold=2, reset_timeout=0, success_threshold=1)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.01)
        cb.allow_request()  # HALF_OPEN
        cb.record_success()
        assert cb.state == State.CLOSED

    def test_registry(self):
        from aggregator.circuit_breaker import CircuitBreakerRegistry
        cb1 = CircuitBreakerRegistry.get("sheets", failure_threshold=5)
        cb2 = CircuitBreakerRegistry.get("sheets")
        assert cb1 is cb2

    def test_stats(self):
        from aggregator.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker(name="stats_test")
        stats = cb.stats
        assert stats["name"] == "stats_test"
        assert stats["state"] == "closed"


class TestRetry:
    """Test retry with backoff."""

    def test_succeeds_first_try(self):
        from aggregator.retry import retry
        call_count = 0
        @retry(max_attempts=3, base_delay=0.01)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"
        assert succeed() == "ok"
        assert call_count == 1

    def test_retries_on_failure(self):
        from aggregator.retry import retry
        call_count = 0
        @retry(max_attempts=3, base_delay=0.01)
        def fail_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "ok"
        assert fail_twice() == "ok"
        assert call_count == 3

    def test_raises_after_max_attempts(self):
        from aggregator.retry import retry
        @retry(max_attempts=2, base_delay=0.01)
        def always_fail():
            raise RuntimeError("permanent")
        with pytest.raises(RuntimeError):
            always_fail()

    def test_backoff_calculation(self):
        from aggregator.retry import RetryPolicy
        policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0, max_delay=10.0, jitter=False)
        assert policy.calculate_delay(0) == 1.0
        assert policy.calculate_delay(1) == 2.0
        assert policy.calculate_delay(2) == 4.0
        assert policy.calculate_delay(10) == 10.0  # capped

    def test_jitter_adds_randomness(self):
        from aggregator.retry import RetryPolicy
        policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0, jitter=True)
        delays = [policy.calculate_delay(2) for _ in range(10)]
        assert len(set(delays)) > 1  # should not all be identical


class TestHotReload:
    """Test config hot-reload."""

    def test_detects_change(self, tmp_path):
        from aggregator.hot_reload import ConfigWatcher
        cfg = tmp_path / "test.yaml"
        cfg.write_text("stages:\n  - title_validation\n")
        watcher = ConfigWatcher(str(cfg))
        assert not watcher.has_changed()
        cfg.write_text("stages:\n  - title_validation\n  - salary_check\n")
        assert watcher.has_changed()

    def test_no_change(self, tmp_path):
        from aggregator.hot_reload import ConfigWatcher
        cfg = tmp_path / "test.yaml"
        cfg.write_text("key: value")
        watcher = ConfigWatcher(str(cfg))
        assert not watcher.has_changed()
        assert not watcher.has_changed()

    def test_callback_fires(self, tmp_path):
        from aggregator.hot_reload import ConfigWatcher
        cfg = tmp_path / "test.yaml"
        cfg.write_text("v1")
        fired = []
        watcher = ConfigWatcher(str(cfg), on_change=lambda: fired.append(True))
        cfg.write_text("v2")
        watcher.reload_if_changed()
        assert len(fired) == 1

    def test_missing_file(self, tmp_path):
        from aggregator.hot_reload import ConfigWatcher
        watcher = ConfigWatcher(str(tmp_path / "nonexistent.yaml"))
        assert not watcher.has_changed()


class TestDataContracts:
    """Test typed data contracts."""

    def test_valid_job(self):
        from aggregator.contracts import JobContract
        job = JobContract(company="Google", title="SDE Intern", url="https://google.com/jobs/1")
        assert job.company == "Google"

    def test_missing_company_raises(self):
        from aggregator.contracts import JobContract, ContractViolation
        with pytest.raises(ContractViolation):
            JobContract(company="", title="Intern", url="https://x.com")

    def test_missing_url_raises(self):
        from aggregator.contracts import JobContract, ContractViolation
        with pytest.raises(ContractViolation):
            JobContract(company="Acme", title="Intern", url="not-a-url")

    def test_invalid_resume_type_raises(self):
        from aggregator.contracts import JobContract, ContractViolation
        with pytest.raises(ContractViolation):
            JobContract(company="X", title="Y", url="https://x.com", resume_type="INVALID")

    def test_from_dict(self):
        from aggregator.contracts import JobContract
        job = JobContract.from_dict({
            "company": "Meta", "title": "ML Intern",
            "url": "https://meta.com/jobs/1", "resume_type": "ML"
        })
        assert job.resume_type == "ML"

    def test_safe_validate_returns_none(self):
        from aggregator.contracts import JobContract
        assert JobContract.safe_validate({"company": "", "title": "", "url": ""}) is None

    def test_coerce_legacy_fields(self):
        from aggregator.contracts import JobContract
        job = JobContract.coerce({
            "co": "Tesla", "ti": "SDE Intern",
            "url": "https://tesla.com/jobs/1"
        })
        assert job.company == "Tesla"
        assert job.title == "SDE Intern"

    def test_to_dict(self):
        from aggregator.contracts import JobContract
        job = JobContract(company="X", title="Y", url="https://x.com")
        d = job.to_dict()
        assert isinstance(d, dict)
        assert d["company"] == "X"

