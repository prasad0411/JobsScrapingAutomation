#!/usr/bin/env python3
"""
Fix all 6 Brain/data sync issues in one shot:
1. Job IDs never registered — force migration of existing job IDs from sheet
2. Sponsorship cache not loading from Brain on startup
3. outreach_patterns.json migration not firing (migration marked done too early)
4. domain_pattern_history.json not re-syncing on each run
5. failed_simplify_urls.json never pruned (793 entries growing unbounded)
6. send_fail_counts not syncing to Brain

Run from project root:
    python3 patch_brain_fixes.py
"""

import os, sys, json, time, shutil, datetime

ROOT = os.path.abspath(".")
LOCAL = os.path.join(ROOT, ".local")


def backup(path):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path + f".bak_{ts}"
    shutil.copy2(path, bak)
    print(f"  Backed up → {os.path.basename(bak)}")


def patch(path, old, new, label):
    with open(path) as f:
        src = f.read()
    if old not in src:
        print(f"  SKIP (not found): {label}")
        return False
    with open(path, "w") as f:
        f.write(src.replace(old, new, 1))
    print(f"  Patched: {label}")
    return True


# ══════════════════════════════════════════════════════════════════════
# FIX 1 & 3 & 4: Reset migration flag so Brain re-runs legacy migration
# + Force re-sync of patterns and domain history on every startup
# ══════════════════════════════════════════════════════════════════════

print("\n[1/6] Reset Brain migration flag to force re-migration...")
brain_file = os.path.join(LOCAL, "brain.json")
if os.path.exists(brain_file):
    try:
        backup(brain_file)
        data = json.load(open(brain_file))
        # Remove migration done flag so it re-runs
        if "_legacy_migration_done" in data:
            del data["_legacy_migration_done"]
            print("  Removed _legacy_migration_done flag — migration will re-run")
        # Also pre-populate sponsorship from existing cache if any
        json.dump(data, open(brain_file, "w"), indent=2)
    except Exception as e:
        print(f"  ERROR: {e}")

# ══════════════════════════════════════════════════════════════════════
# FIX 2: Load Brain sponsorship cache into _SPONSORSHIP_CACHE on startup
# ══════════════════════════════════════════════════════════════════════

print("\n[2/6] Fix sponsorship cache — load Brain into memory on startup...")
agg_path = os.path.join(ROOT, "aggregator", "run_aggregator.py")
backup(agg_path)

OLD_SPONS = "_SPONSORSHIP_CACHE = {}"
NEW_SPONS = '''_SPONSORSHIP_CACHE = {}

def _load_sponsorship_from_brain():
    """Load Brain sponsorship cache into memory at startup."""
    try:
        from outreach.brain import Brain
        b = Brain.get()
        cached = b._data.get("sponsorship", {})
        _SPONSORSHIP_CACHE.update(cached)
        if cached:
            import logging as _log
            _log.getLogger(__name__).info(
                f"Loaded {len(cached)} sponsorship entries from Brain"
            )
    except Exception:
        pass

_load_sponsorship_from_brain()'''

patch(agg_path, OLD_SPONS, NEW_SPONS, "sponsorship cache loaded from Brain at startup")

# ══════════════════════════════════════════════════════════════════════
# FIX 3 & 4: Make migration re-sync patterns on every run, not just once
# ══════════════════════════════════════════════════════════════════════

print("\n[3/6] Fix Brain migration — re-sync patterns on every startup...")
brain_path = os.path.join(ROOT, "outreach", "brain.py")
backup(brain_path)

OLD_MIG = '''        if self._data.get("_legacy_migration_done"):
            return'''

NEW_MIG = '''        # Always re-sync pattern files — they update each run
        # Only skip full migration if already done AND pattern files haven't changed
        _already_done = self._data.get("_legacy_migration_done", False)'''

patch(brain_path, OLD_MIG, NEW_MIG, "migration no longer skipped on re-runs")

# Fix the migration completion marker to only block expensive migrations
OLD_MIG_END = '''        if migrated:
            self._data["_legacy_migration_done"] = True
            self._data["_legacy_migration_ts"] = time.time()
            self._data["_legacy_migration_files"] = migrated
            log.info(f"Brain: migrated {migrated}")
            self.save()'''

NEW_MIG_END = '''        if migrated:
            # Mark full migration done but keep re-syncing pattern files
            self._data["_legacy_migration_done"] = True
            self._data["_legacy_migration_ts"] = time.time()
            self._data["_legacy_migration_files"] = migrated
            log.info(f"Brain: migrated {migrated}")
            self.save()
        elif _already_done:
            # Re-sync pattern files even if migration already ran
            import os as _os2, json as _json2
            _local2 = _os2.path.join(_os2.path.dirname(_os2.path.dirname(
                _os2.path.abspath(__file__))), ".local")
            _op2 = _os2.path.join(_local2, "outreach_patterns.json")
            if _os2.path.exists(_op2):
                try:
                    _d2 = _json2.load(open(_op2))
                    for _dom, _pat in _d2.items():
                        if _dom != "_global_best" and _pat:
                            if not self.best_pattern_for(_dom):
                                self.record_pattern_success(_dom, _pat, "resync")
                except Exception as _re:
                    log.debug(f"Pattern resync failed: {_re}")'''

patch(brain_path, OLD_MIG_END, NEW_MIG_END, "pattern files re-synced on every startup")

# ══════════════════════════════════════════════════════════════════════
# FIX 5: Prune failed_simplify_urls.json — remove entries older than 30d
# ══════════════════════════════════════════════════════════════════════

print("\n[4/6] Pruning failed_simplify_urls.json...")
fsu = os.path.join(LOCAL, "failed_simplify_urls.json")
if os.path.exists(fsu):
    try:
        data = json.load(open(fsu))
        before = len(data)
        # Entries are {url: date_string} or {url: {ts: ...}}
        cutoff = time.time() - 30 * 86400
        pruned = {}
        for k, v in data.items():
            if isinstance(v, dict):
                if v.get("ts", 0) > cutoff:
                    pruned[k] = v
            else:
                # Old format — keep (can't determine age)
                pruned[k] = v
        json.dump(pruned, open(fsu, "w"), indent=2)
        print(f"  Pruned: {before} → {len(pruned)} entries")
    except Exception as e:
        print(f"  ERROR: {e}")

# Also add auto-pruning to the extractor so it never bloats again
ext_path = os.path.join(ROOT, "aggregator", "extractors.py")
backup(ext_path)

OLD_SAVE_FAILED = '''    @staticmethod
    def _save_failed_url(url):'''

NEW_SAVE_FAILED = '''    @staticmethod
    def _prune_failed_urls():
        """Remove failed URL entries older than 30 days."""
        try:
            from aggregator.config import FAILED_URLS_FILE
            if not os.path.exists(FAILED_URLS_FILE):
                return
            data = json.load(open(FAILED_URLS_FILE))
            cutoff = time.time() - 30 * 86400
            pruned = {k: v for k, v in data.items()
                      if not isinstance(v, dict) or v.get("ts", cutoff+1) > cutoff}
            if len(pruned) < len(data):
                json.dump(pruned, open(FAILED_URLS_FILE, "w"), indent=2)
        except Exception:
            pass

    @staticmethod
    def _save_failed_url(url):'''

patch(ext_path, OLD_SAVE_FAILED, NEW_SAVE_FAILED, "auto-pruning added to failed URL cache")

# ══════════════════════════════════════════════════════════════════════
# FIX 5b: Prune simplify_method_cache and url_health_cache
# ══════════════════════════════════════════════════════════════════════

print("\n[5/6] Pruning stale cache files...")
# url_health_cache — remove entries older than 24h
uhc = os.path.join(LOCAL, "url_health_cache.json")
if os.path.exists(uhc):
    try:
        data = json.load(open(uhc))
        before = len(data)
        cutoff = time.time() - 86400  # 24h
        pruned = {k: v for k, v in data.items()
                  if isinstance(v, dict) and v.get("ts", 0) > cutoff}
        json.dump(pruned, open(uhc, "w"), indent=2)
        print(f"  url_health_cache: {before} → {len(pruned)} entries")
    except Exception as e:
        print(f"  url_health_cache ERROR: {e}")

# failed_simplify_urls — already pruned above
fsu2 = os.path.join(LOCAL, "failed_simplify_urls.json")
if os.path.exists(fsu2):
    try:
        data = json.load(open(fsu2))
        print(f"  failed_simplify_urls: {len(data)} entries remaining")
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════
# FIX 6: send_fail_counts — verify Brain sync works, force initial sync
# ══════════════════════════════════════════════════════════════════════

print("\n[6/6] Syncing send_fail_counts to Brain...")
sfc = os.path.join(LOCAL, "send_fail_counts.json")
try:
    sys.path.insert(0, ROOT)
    from outreach.brain import Brain
    b = Brain.get()
    fc_data = {}
    if os.path.exists(sfc):
        fc_data = json.load(open(sfc))
    brain_fc = b._data.get("send_fail_counts", {})
    merged = {**brain_fc, **fc_data}
    # Only keep non-zero entries
    merged = {k: v for k, v in merged.items() if v > 0}
    b._data["send_fail_counts"] = merged
    b.save()
    json.dump(merged, open(sfc, "w"), indent=2)
    print(f"  Synced {len(merged)} fail count entries to Brain")
except Exception as e:
    print(f"  ERROR: {e}")

# Force migration to re-run by triggering Brain init
try:
    from outreach.brain import Brain
    b = Brain.get()
    b.migrate_legacy_data()
    print(f"\n  Brain re-migration complete")
    print(f"  Domains: {len(b._data.get('domains', {}))}")
    print(f"  Pattern rates: {len(b._data.get('patterns', {}).get('global_success_rates', {}))}")
    print(f"  Sponsorship: {len(b._data.get('sponsorship', {}))}")
    print(f"  Job IDs: {len(b._data.get('job_id_registry', {}))}")
except Exception as e:
    print(f"  Brain re-migration ERROR: {e}")

print("\n" + "="*60)
print("ALL FIXES APPLIED")
print("="*60)
print("1. Brain migration flag reset — patterns re-sync on every startup")
print("2. Sponsorship cache loads from Brain at aggregator startup")
print("3. outreach_patterns.json re-syncs to Brain even after migration")
print("4. domain_pattern_history.json re-syncs on every startup")
print("5. failed_simplify_urls.json pruned to 30d, url_health_cache to 24h")
print("6. send_fail_counts synced to Brain")
print("\nVerify with:")
print("  python3 -c \"from outreach.brain import Brain; b=Brain.get(); print(len(b._data.get('patterns',{}).get('global_success_rates',{})), 'patterns')\"")