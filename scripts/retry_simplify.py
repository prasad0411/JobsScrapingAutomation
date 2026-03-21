#!/usr/bin/env python3
"""
Retry failed Simplify URLs using Brain's intelligent retry queue.
Run 3x/day via launchd. Processes only entries whose next_retry_at has passed.
    python3 scripts/retry_simplify.py
"""
import sys, os, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

def main():
    from outreach.brain import Brain
    from aggregator.extractors import SimplifyRedirectResolver
    b = Brain.get()
    due = b.get_simplify_retries_due()
    if not due:
        log.info("Simplify retry: nothing due")
        return
    log.info(f"Simplify retry: {len(due)} entries due")
    resolved = 0
    failed = 0
    for entry in due:
        jid = entry["job_id"]
        url = entry["url"]
        attempts = entry.get("attempts", 0)
        category = entry.get("category", "unknown")
        log.info(f"  Retrying {jid} (attempt {attempts}, category={category}): {url[:60]}")
        try:
            # Clear both lru_cache (if any) and success cache
            try:
                SimplifyRedirectResolver.resolve.cache_clear()
            except AttributeError:
                pass  # No lru_cache anymore
            SimplifyRedirectResolver._success_cache.pop(jid, None)
            result_url, success = SimplifyRedirectResolver.resolve(url)
            if success and result_url and result_url != url:
                log.info(f"  ✓ Resolved: {result_url[:70]}")
                b.mark_simplify_retry_success(jid)
                resolved += 1
                try:
                    from aggregator.run_aggregator import UnifiedJobAggregator
                    agg = UnifiedJobAggregator()
                    agg._process_single_job_comprehensive(result_url, source="simplify_retry")
                    if agg.valid_jobs:
                        rows = agg.sheets.get_next_row_numbers()
                        agg.sheets.add_valid_jobs(agg.valid_jobs, rows["valid"], rows["valid_sr_no"])
                        log.info(f"  ✓ Added {len(agg.valid_jobs)} jobs from retry")
                except Exception as _pe:
                    log.debug(f"Pipeline inject failed: {_pe}")
            else:
                if attempts >= 2:
                    log.info(f"  ✗ Exhausted after {attempts+1} attempts: {jid}")
                    b.mark_simplify_retry_exhausted(jid)
                    failed += 1
                else:
                    log.info(f"  ✗ Still failing: {jid}")
                    failed += 1
        except Exception as e:
            log.warning(f"  ✗ Exception retrying {jid}: {e}")
            failed += 1
    log.info(f"Simplify retry done: {resolved} resolved, {failed} still failing")

if __name__ == "__main__":
    main()
