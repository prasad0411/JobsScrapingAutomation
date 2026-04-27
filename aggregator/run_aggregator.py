#!/usr/bin/env python3

import time
import datetime
import random
import re
import json
import os
import logging
import sqlite3
from collections import defaultdict
from bs4 import BeautifulSoup

from aggregator.config import (
    SIMPLIFY_URL,
    VANSHB03_URL,
    SPEEDYAPPLY_SWE_URL,
    MAX_JOB_AGE_DAYS,
    PAGE_AGE_THRESHOLD_DAYS,
    MIN_QUALITY_SCORE,
    COMPANY_BLACKLIST,
    COMPANY_BLACKLIST_REASONS,
    PLATFORM_BLACKLIST,
    PLATFORM_BLACKLIST_REASONS,
    BLACKLIST_DOMAINS,
    PROCESSED_EMAILS_FILE,
    EMAIL_TRACKING_RETENTION_DAYS,
    REPROCESS_EMAILS_DAYS,
    EMAIL_DATE_FILTER_ENABLED,
    TERMINAL_COMPANY_WIDTH,
    VERBOSE_OUTPUT,
    SHOW_GITHUB_COUNTS,
)

from aggregator.extractors import (
    EmailExtractor,
    PageFetcher,
    PageParser,
    SourceParsers,
    JobrightAuthenticator,
    JobrightRedirectResolver,
    SimplifyRedirectResolver,
    SimplifyGitHubScraper,
    ZipRecruiterResolver,
    safe_parse_html,
    retry_request,
)

from aggregator.processors import (
    TitleProcessor,
    LocationExtractor,
    LocationProcessor,
    ValidationHelper,
    CompanyExtractor,
    QualityScorer,
    log_detailed_rejection,
)

from aggregator.sheets_manager import SheetsManager

from aggregator.utils import (
    PlatformDetector,
    CompanyNormalizer,
    CompanyValidator,
    RoleCategorizer,
    URLCleaner,
    DateParser,
    DataSanitizer,
    ExtractionVoter,
)

logging.basicConfig(
    filename=os.path.join(".local", "skipped_jobs.log"),
    filemode="a",
    level=logging.INFO,
    force=True,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Log header written via logging
logging.info("=" * 80)
logging.info(f"JOB PROCESSING LOG - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Greenhouse company slug extraction
GREENHOUSE_COMPANY_MAP = {
    "tenstorrentuniversity": "Tenstorrent",
    "alarmcom": "Alarm.com",
    "skyryse": "Skyryse",
    "trumid": "Trumid",
    "leagueinc": "League",
    "fccincinnati": "FC Cincinnati",
    "antora": "Antora Energy",
    "samsungresearchamericainternship": "Samsung Research America",
    "point72": "Point72",
    "sonatus": "Sonatus",
}

GARBAGE_COMPANY_NAMES = {
    "myworkdayjobs",
    "www",
    "job-boards",
    "company",
    "unknown",
    "careers",
    "jobs",
    "external",
    "portal",
    "applicant",
    "job-boards.greenhouse.io",
    "job-boards.eu.greenhouse.io",
    "your future starts here",
    "t commission",
    "corporate office",
    "learning",
    "insurance services",
    "gardacp",
    "fiveringsllc",
    "oakland",
    "3s business",
    "usa",
    "us",
    "worldwide",
    "intelligent solutions",
    "caliber holdings",
    "cardinal health 5",
    "beone medicines usa",
    "vernova",
    "calix north america",
    "sono",
    "amr-jones lang lasalle americas",
    "company 19 - john hancock life insurance company (u.s.a.)",
    "laboratories",
    "international gmbh",
    "management services",
    "fintech services",
    "employment services",
    "marketing",
    "ats",
    "retail markets",
    "y99000 general electric",
}


# Company name normalization — fix common extraction errors
COMPANY_NAME_FIXES = {
    "pg&e": "PG&E",
    "amat": "Applied Materials",
    "hp": "HP",
    "usa": "Unknown",
    "us": "Unknown",
    "worldwide": "Unknown",
    "tik tok": "TikTok",
    "tmobile": "T-Mobile",
    "myworkdayjobs": "Unknown",
    "job-boards": "Unknown",
    "sono": "Sonoco",
    "vernova": "GE Vernova",
    "wsp": "WSP",
    "adp": "ADP",
    "abb": "ABB",
    "sas": "SAS",
    "exl": "EXL",
    "bmo": "BMO",
    "rtx": "RTX",
    "nxp": "NXP",
    "impinjexternal": "Impinj",
    "abacusinsights": "Abacus Insights",
    "sigmacomputing": "Sigma Computing",
    "aloyoga": "Alo Yoga",
    "disneyland": "Disney",
    "y99000 general electric": "GE Aerospace",
    "assaabloy": "ASSA ABLOY",
    "ats": "Unknown",
    "retail markets": "Unknown",
    "gts": "GTS",
    "aegworldwide": "AEG",
    "aeg": "AEG",
    "gts": "GTS",
    "colliers engineering & design home": "Colliers Engineering",
    "simplify": "Unknown",
    "intelligent solutions": "CCC Intelligent Solutions",
    "caliber holdings": "Caliber Collision",
    "cardinal health 5": "Cardinal Health",
    "beone medicines usa": "BeiGene",
    "calix north america": "Calix",
    "(marvell semiconductor inc.) us": "Marvell",
}


class JobrightEmailParser:
    @staticmethod
    def parse_email_jobs(email_html):
        if not email_html:
            return {}

        try:
            soup = BeautifulSoup(email_html, "html.parser")
            job_map = {}

            job_sections = soup.find_all("table", id="job-section")

            for section in job_sections:
                try:
                    parent_link = section.find_parent(
                        "a", href=re.compile(r"jobright\.ai/jobs/info/")
                    )
                    if not parent_link:
                        title_link = section.find("p", id="job-title")
                        if title_link:
                            a_tag = title_link.find(
                                "a", href=re.compile(r"jobright\.ai/jobs/info/")
                            )
                            if a_tag:
                                jr_url = a_tag.get("href", "")
                            else:
                                continue
                        else:
                            continue
                    else:
                        jr_url = parent_link.get("href", "")

                    if not jr_url or "jobright.ai/jobs/info/" not in jr_url:
                        continue

                    company_elem = section.find("p", id="job-company-name")
                    company = (
                        re.sub(r"\s+", " ", company_elem.get_text(separator=" ", strip=True)).strip()
                        if company_elem else "Unknown"
                    )

                    title_elem = section.find("p", id="job-title")
                    title = (
                        re.sub(r"\s+", " ", title_elem.get_text(separator=" ", strip=True)).strip()
                        if title_elem else "Unknown"
                    )

                    location = "Unknown"
                    tags = section.find_all("p", id="job-tag")
                    for tag in tags:
                        tag_text = tag.get_text(strip=True)
                        if not tag_text:
                            continue
                        if any(
                            skip in tag_text
                            for skip in ["$", "/hr", "/yr", "/mo", "referral"]
                        ):
                            continue
                        if re.search(r"[A-Z][a-z]+.*,\s*[A-Z]{2}", tag_text):
                            location = tag_text
                            break
                        if "remote" in tag_text.lower() and len(tag_text) < 20:
                            location = "Remote"
                            break
                        if len(tag_text) < 60 and "," in tag_text:
                            location = tag_text
                            break

                    clean_jr_url = re.sub(r"\?.*$", "", jr_url)
                    job_data = {
                        "company": company,
                        "title": title,
                        "location": location,
                        "apply_url": None,
                    }
                    job_map[clean_jr_url] = job_data
                    job_map[jr_url] = job_data

                except Exception as e:
                    logging.debug(f"Failed to parse job section: {e}")
                    continue

            unique_count = len(set(id(v) for v in job_map.values()))
            logging.info(f"Jobright email parser: extracted {unique_count} job cards")
            return job_map

        except Exception as e:
            logging.error(f"Jobright email parser failed: {e}")
            return {}


class ProcessedEmailTracker:
    @staticmethod
    def load():
        if os.path.exists(PROCESSED_EMAILS_FILE):
            try:
                with open(PROCESSED_EMAILS_FILE, "r") as f:
                    data = json.load(f)
                cutoff = (
                    datetime.datetime.now()
                    - datetime.timedelta(days=EMAIL_TRACKING_RETENTION_DAYS)
                ).strftime("%Y-%m-%d")
                cleaned = {
                    k: v
                    for k, v in data.items()
                    if v.get("processed_date", "") >= cutoff
                }
                return cleaned
            except Exception:
                return {}
        return {}

    @staticmethod
    def save(processed_emails):
        try:
            # FIX 11: cap at 10,000 entries — prune oldest to prevent unbounded growth
            MAX_ENTRIES = 10000
            if len(processed_emails) > MAX_ENTRIES:
                sorted_items = sorted(
                    processed_emails.items(),
                    key=lambda x: x[1].get("processed_date", "") + x[1].get("processed_time", "")
                )
                processed_emails = dict(sorted_items[-MAX_ENTRIES:])
                logging.info(f"ProcessedEmailTracker pruned to {MAX_ENTRIES} entries")
            with open(PROCESSED_EMAILS_FILE, "w") as f:
                json.dump(processed_emails, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save processed emails: {e}")

    @staticmethod
    def mark_email_processed(processed_emails, email_id, subject, url_count):
        processed_emails[email_id] = {
            "subject": subject,
            "processed_date": datetime.datetime.now().strftime("%Y-%m-%d"),
            "processed_time": datetime.datetime.now().strftime("%H:%M:%S"),
            "url_count": url_count,
        }


# Module-level Claude sponsorship classifier
# Returns "no" if company is known to not sponsor, "unknown" otherwise.
# Uses a simple cache to avoid repeat API calls for same company.
_SPONSORSHIP_CACHE = {}
import threading as _threading
class _NOOP_LOCK:
    def __enter__(self): return self
    def __exit__(self, *a): pass
_NOOP_LOCK = _NOOP_LOCK()

def _load_sponsorship_from_brain(): pass

# Auto-prune stale failed URL cache on startup
try:
    from aggregator.extractors import PageFetcher as _PF
    _PF._prune_failed_urls()
except Exception:
    pass

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

_load_sponsorship_from_brain()

def _claude_sponsorship_check(company, title):
    """
    Ask Claude whether this company sponsors F-1/H-1B visas.
    Returns 'no' ONLY when highly confident. Returns 'unknown' on any doubt.
    Self-learning: results cached in Brain permanently — same company never re-queried.
    Skips if ANTHROPIC_API_KEY not set — zero impact on existing behaviour.
    """
    # Check Brain cache first — permanent, cross-run memory
    try:
        from outreach.brain import Brain
        _brain = Brain.get()
        _bspons = _brain._data.get("sponsorship", {}).get(company.lower().strip())
        if _bspons is not None:
            return _bspons
    except Exception:
        pass
    import os as _os
    _root = _os.path.dirname(_os.path.abspath(__file__))
    _env = _os.path.join(_root, ".env")
    _api_key = ""
    if _os.path.exists(_env):
        for ln in open(_env):
            ln = ln.strip()
            if ln.startswith("ANTHROPIC_API_KEY="):
                _api_key = ln.split("=", 1)[1].strip()
                break
    _api_key = _api_key or _os.environ.get("ANTHROPIC_API_KEY", "")
    if not _api_key:
        return "unknown"

    cache_key = company.lower().strip()
    if cache_key in _SPONSORSHIP_CACHE:
        return _SPONSORSHIP_CACHE[cache_key]

    try:
        import urllib.request, json as _j
        prompt = (
            f"Company: {company}\nJob title: {title}\n\n"
            "Does this company sponsor F-1 OPT or H-1B visas for internships? "
            "Answer ONLY with one word: 'yes', 'no', or 'unknown'. "
            "IMPORTANT: Answer 'no' ONLY if you are 95%+ certain this company NEVER "
            "sponsors international students. Answer 'unknown' for any uncertainty. "
            "When in doubt, always answer 'unknown'. Never guess 'no'."
        )
        body = _j.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": prompt}]
        }).encode()
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": _api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
        )
        resp = urllib.request.urlopen(req, timeout=8)
        data = _j.loads(resp.read())
        answer = data["content"][0]["text"].strip().lower().rstrip(".")
        result = "no" if answer == "no" else "unknown"
        _SPONSORSHIP_CACHE[cache_key] = result
        # Save to Brain permanently — never re-query same company again
        try:
            from outreach.brain import Brain
            b = Brain.get()
            if "sponsorship" not in b._data:
                b._data["sponsorship"] = {}
            b._data["sponsorship"][cache_key] = result
            b.save()
        except Exception:
            pass
        logging.info(f"Claude sponsorship: {company} → {result} (saved to Brain)")
        return result
    except Exception as e:
        logging.debug(f"Claude sponsorship check failed for {company}: {e}")
        return "unknown"


class UnifiedJobAggregator:
    def __init__(self):
        print("=" * 80)
        self.sheets = SheetsManager()
        self.email_extractor = EmailExtractor()
        self.page_fetcher = PageFetcher()
        self.jobright_auth = JobrightAuthenticator()

        existing = self.sheets.load_existing_jobs()
        self.existing_jobs = existing["jobs"]
        self.existing_urls = existing["urls"]
        self.existing_job_ids = existing["job_ids"]
        self.processed_cache = existing["cache"]

        self.processing_lock = set()
        self.valid_jobs = []
        self.discarded_jobs = []
        self.duplicate_jobs = []

        self.outcomes = defaultdict(int)
        self.source_stats = defaultdict(lambda: defaultdict(int))
        import threading as _t; self._github_lock = _t.Lock()  # thread safety for parallel processing

        print(
            # (loaded silently)
        )
        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs from sheets")

    def run(self):
        start_time = time.time()

        # Selenium health check — catch ChromeDriver mismatches immediately
        self._check_selenium_health()

        if not self.jobright_auth.cookies:
            if os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"):
                logging.warning("CI environment: skipping Jobright interactive login. Using email-only mode.")
            else:
                self.jobright_auth.login_interactive()

        # (silent)
        self._scrape_simplify_github()

        print("\nProcessing email jobs...")
        try:
            emails_data = self.email_extractor.fetch_job_emails()
            if emails_data:
                total_urls = sum(len(email["urls"]) for email in emails_data)
                print(
                    f"Processing {total_urls} URLs from {len(emails_data)} emails...\n"
                )
                self._process_emails_grouped(emails_data)
            else:
                print("No email jobs found")
                logging.warning("No email data received from Gmail")
        except Exception as e:
            print(f"Email processing error: {e}")
            logging.error(f"Email processing error: {e}", exc_info=True)

        self._ensure_mutual_exclusion()

        # Save Brain once after all job_id registrations
        try:
            from outreach.brain import Brain
            Brain.get().save()
        except Exception:
            pass

        rows = self.sheets.get_next_row_numbers()

        # ── WAL: wrap sheet writes in transactions for crash safety ──
        _wal = None
        _tx_valid = None
        _tx_discarded = None
        try:
            from aggregator.wal import WriteAheadLog
            _wal = WriteAheadLog()
            # Replay any pending transactions from previous crashed runs
            _pending = _wal.get_pending()
            if _pending:
                logging.info(f"WAL: {len(_pending)} pending transactions from previous run")
                _wal.replay_pending()
        except Exception as _wal_e:
            logging.debug(f"WAL init: {_wal_e}")

        # Write valid jobs with WAL protection
        try:
            if _wal and self.valid_jobs:
                _tx_valid = _wal.begin("add_valid_jobs", {
                    "count": len(self.valid_jobs),
                    "start_row": rows["valid"],
                })
        except Exception:
            pass

        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )

        try:
            if _wal and _tx_valid:
                _wal.commit(_tx_valid)
        except Exception:
            pass

        # Write discarded jobs with WAL protection
        try:
            if _wal and self.discarded_jobs:
                _tx_discarded = _wal.begin("add_discarded_jobs", {
                    "count": len(self.discarded_jobs),
                    "start_row": rows["discarded"],
                })
        except Exception:
            pass

        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )

        try:
            if _wal and _tx_discarded:
                _wal.commit(_tx_discarded)
        except Exception:
            pass

        # ── Analytics: record every processed job in real-time ──
        try:
            from analytics.store import AnalyticsStore
            from analytics.models import JobRecord
            from datetime import datetime
            _astore = AnalyticsStore()
            _run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
            _analytics_jobs = []

            for j in self.valid_jobs:
                _analytics_jobs.append(JobRecord(
                    url=j.get("url", ""),
                    company=j.get("company", "Unknown"),
                    title=j.get("title", "Unknown"),
                    location=j.get("location", "Unknown"),
                    source=j.get("source", "Unknown"),
                    outcome="valid",
                    resume_type=j.get("resume_type", "SDE"),
                    job_type=j.get("job_type", "Internship"),
                    job_id=j.get("job_id", "N/A"),
                    remote=j.get("remote", "Unknown"),
                    sponsorship=j.get("sponsorship", "Unknown"),
                    entry_date=j.get("entry_date", ""),
                ))

            for d in self.discarded_jobs:
                _analytics_jobs.append(JobRecord(
                    url=d.get("url", ""),
                    company=d.get("company", "Unknown"),
                    title=d.get("title", "Unknown"),
                    location=d.get("location", "Unknown"),
                    source=d.get("source", "Unknown"),
                    outcome="discarded",
                    rejection_reason=d.get("reason", ""),
                    job_type=d.get("job_type", "Internship"),
                    job_id=d.get("job_id", "N/A"),
                    entry_date=d.get("entry_date", ""),
                ))

            if _analytics_jobs:
                _astore.record_jobs_batch(_analytics_jobs, run_id=_run_id)
                logging.info(f"Analytics: recorded {len(_analytics_jobs)} jobs (run={_run_id})")
            _astore.close()
        except Exception as _a_e:
            logging.debug(f"Analytics recording skipped: {_a_e}")

        # ── WAL cleanup: remove old committed transactions ──
        try:
            if _wal:
                _wal.cleanup_committed(max_age_days=7)
        except Exception:
            pass

        self._print_summary()
        elapsed = time.time() - start_time
        print(f"\n✓ DONE: {added_valid} valid, {added_discarded} discarded")
        print(f"Execution time: {elapsed / 60:.1f} minutes")
        print("=" * 80 + "\n")

        logging.info(f"SUMMARY: {added_valid} valid, {added_discarded} discarded")
        self._log_run_to_db(added_valid, added_discarded, elapsed)

    def _log_run_to_db(self, valid, discarded, elapsed_seconds):
        """Append this run's stats to .local/run_history.db for trend analysis."""
        try:
            db_path = os.path.join(".local", "run_history.db")
            os.makedirs(".local", exist_ok=True)
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    valid INTEGER,
                    discarded INTEGER,
                    duplicate_url INTEGER,
                    duplicate_job INTEGER,
                    skipped_old INTEGER,
                    skipped_non_tech INTEGER,
                    skipped_international INTEGER,
                    skipped_clearance INTEGER,
                    skipped_blacklisted INTEGER,
                    failed_http INTEGER,
                    elapsed_seconds REAL
                )
            """)
            cur.execute("""
                INSERT INTO runs (
                    ts, valid, discarded, duplicate_url, duplicate_job,
                    skipped_old, skipped_non_tech, skipped_international,
                    skipped_clearance, skipped_blacklisted, failed_http, elapsed_seconds
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                datetime.datetime.now().isoformat(),
                valid,
                discarded,
                self.outcomes.get("skipped_duplicate_url", 0),
                self.outcomes.get("skipped_duplicate_company_title", 0),
                self.outcomes.get("skipped_too_old", 0),
                self.outcomes.get("skipped_non_tech", 0),
                self.outcomes.get("skipped_international", 0),
                self.outcomes.get("skipped_page_restriction", 0),
                self.outcomes.get("skipped_blacklisted", 0),
                self.outcomes.get("failed_http", 0),
                elapsed_seconds,
            ))
            con.commit()
            con.close()
            logging.info(f"Run logged to {db_path}")
            try:
                from outreach.brain import Brain
                b = Brain.get()
                for source_name, stats in self.source_stats.items():
                    fetched = stats.get("valid",0)+stats.get("rejected",0)+stats.get("failed",0)
                    b.record_source_run(source_name, fetched, stats.get("valid", 0))
                new_bl = b.new_blacklisted_companies()
                if new_bl:
                    logging.info(f"Brain: {len(new_bl)} companies ready for config sync")
            except Exception as _be:
                logging.debug(f"Brain run update failed: {_be}")
        except Exception as e:
            logging.debug(f"Run log failed (non-fatal): {e}")

    def _check_selenium_health(self):
        """Selenium health check with auto-repair and Brain tracking."""
        from outreach.brain import Brain
        b = Brain.get()
        try:
            from aggregator.extractors import PageFetcher as _PF
            _pf = _PF()
            resp, _, _ = _pf.fetch_page("https://www.google.com", force_requests=True)
            if resp:
                logging.info("Selenium health check: OK (requests mode)")
                b.record_selenium_ok()
                return
        except Exception:
            pass
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            opts = Options()
            opts.add_argument("--headless")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=opts)
            driver.get("about:blank")
            try:
                ver = driver.capabilities.get("chrome", {}).get("chromedriverVersion", "").split(" ")[0]
                b.record_selenium_ok(ver)
            except Exception:
                b.record_selenium_ok()
            driver.quit()
            logging.info("Selenium health check: OK")
            return
        except Exception as e:
            fail_count = b.record_selenium_failure(str(e))
            logging.warning(f"Selenium health check FAILED (attempt {fail_count}): {e}")
            repaired = False
            # Method 1: webdriver-manager auto-download
            try:
                from selenium.webdriver.chrome.service import Service as _Svc
                from webdriver_manager.chrome import ChromeDriverManager as _CDM
                from selenium import webdriver as _wd
                from selenium.webdriver.chrome.options import Options as _Opts
                _opts = _Opts()
                _opts.add_argument("--headless")
                _opts.add_argument("--no-sandbox")
                _opts.add_argument("--disable-dev-shm-usage")
                _driver = _wd.Chrome(service=_Svc(_CDM().install()), options=_opts)
                _driver.get("about:blank")
                _driver.quit()
                b.record_selenium_repair("webdriver_manager", True)
                b.record_selenium_ok()
                logging.info("Selenium repaired via webdriver-manager")
                print("  ✓ Selenium auto-repaired via webdriver-manager")
                repaired = True
            except Exception as _wde:
                b.record_selenium_repair("webdriver_manager", False)
                logging.debug(f"webdriver-manager repair failed: {_wde}")
            # Method 2: brew upgrade chromedriver
            if not repaired:
                try:
                    import subprocess, shutil
                    brew = shutil.which("brew") or "/opt/homebrew/bin/brew"
                    result = subprocess.run([brew, "upgrade", "chromedriver"],
                        capture_output=True, text=True, timeout=120)
                    if result.returncode == 0:
                        from selenium import webdriver as _wd2
                        from selenium.webdriver.chrome.options import Options as _O2
                        _o2 = _O2()
                        _o2.add_argument("--headless")
                        _o2.add_argument("--no-sandbox")
                        _d2 = _wd2.Chrome(options=_o2)
                        _d2.get("about:blank")
                        _d2.quit()
                        b.record_selenium_repair("brew_upgrade", True)
                        b.record_selenium_ok()
                        logging.info("Selenium repaired via brew upgrade chromedriver")
                        print("  ✓ Selenium repaired via brew upgrade chromedriver")
                        repaired = True
                    else:
                        b.record_selenium_repair("brew_upgrade", False)
                except Exception as _bre:
                    b.record_selenium_repair("brew_upgrade", False)
                    logging.debug(f"brew repair failed: {_bre}")
            if not repaired:
                if fail_count >= 3:
                    b.send_email_alert(
                        "🔧 Selenium broken — Workday/Ashby jobs failing",
                        f"ChromeDriver failed {fail_count} times.\nError: {e}\n\n"
                        f"Auto-repair failed. Manual fix:\n"
                        f"  chromedriver --version\n"
                        f"  google-chrome --version\n"
                        f"  brew install --cask chromedriver"
                    )
                print(f"\n{'='*60}\n  WARNING: Selenium auto-repair failed (attempt {fail_count})\n"
                      f"  Error: {e}\n  Workday/Ashby/Oracle jobs will fail.\n{'='*60}")

    def _scrape_simplify_github(self):
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            f1 = ex.submit(self._safe_scrape, SIMPLIFY_URL, "SimplifyJobs")
            f2 = ex.submit(self._safe_scrape, VANSHB03_URL, "vanshb03")
            f3 = ex.submit(self._safe_scrape, SPEEDYAPPLY_SWE_URL, "speedyapply_swe")
            simplify_jobs = f1.result()
            vanshb03_jobs = f2.result()
            speedyapply_jobs = f3.result()
        self._github_mode = True

        logging.info(
            f"GitHub: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03"
        )

        import concurrent.futures

        def _process_github_batch(jobs, source_name):
            fresh, skipped_old = [], 0
            for job in jobs:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped_old += 1
                else:
                    fresh.append(job)
            print(f"  {source_name}: {len(fresh)} fresh, {skipped_old} too old")
            errors = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
                futures = {pool.submit(self._process_single_github_job, job): job for job in fresh}
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        errors += 1
                        job = futures[fut]
                        logging.error(f"Failed {job.get('company','?')}: {e}", exc_info=True)
            print(f"  {source_name}: done ({errors} errors)")

        print(f"\n  Processing Simplify repository...")
        _process_github_batch(simplify_jobs, "SimplifyJobs")
        print(f"\n  Processing Vanshb repository...")
        _process_github_batch(vanshb03_jobs, "vanshb03")
        print(f"\n  Processing SpeedyApply SWE repository...")
        _process_github_batch(speedyapply_jobs, "speedyapply_swe")

        self._github_mode = False

        github_valid = sum(
            1 for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03", "speedyapply_swe"]
        )
        print(f"\n  GitHub: {github_valid} valid jobs total")
        logging.info(f"GitHub summary: {github_valid} valid jobs")

    def _process_single_github_job(self, job):
        title = TitleProcessor.clean_title_aggressive(job["title"])
        url = job["url"]
        source = job.get("source", "GitHub")
        company_from_github = job.get("company", "Unknown")
        location_from_github = job.get("location", "Unknown")

        # Normalize company name
        company_from_github_lower = company_from_github.lower().strip()
        if company_from_github_lower in COMPANY_NAME_FIXES:
            fixed = COMPANY_NAME_FIXES[company_from_github_lower]
            if fixed != "Unknown":
                logging.info(f"GitHub company normalized: '{company_from_github}' → '{fixed}'")
                company_from_github = fixed
            elif company_from_github_lower in GARBAGE_COMPANY_NAMES:
                # Try ATS path extraction before giving up
                _ATS_SLUGS = {"greenhouse", "lever", "workable", "ashbyhq", "rippling",
                              "smartrecruiters", "icims", "myworkdayjobs", "successfactors",
                              "bamboohr", "jobvite", "applytojob", "oraclecloud", "stream",
                              "telecom", "church", "atp", "hpiq", "beone", "bmwgroup"}
                if company_from_github_lower in _ATS_SLUGS and url:
                    try:
                        from urllib.parse import urlparse as _up, unquote as _uq
                        _path = _uq(_up(url).path)
                        _parts = [p for p in _path.split("/") if p and len(p) > 2
                                 and p.lower() not in ("jobs", "job", "careers", "apply",
                                 "en", "external", "internal", "hcmui", "sites",
                                 "candidateexperience", "recruiting", "search")]
                        if _parts:
                            _real = _parts[0].replace("-", " ").replace("_", " ").title()
                            if len(_real) > 2:
                                logging.info(f"ATS fix: '{company_from_github}' → '{_real}'")
                                company_from_github = _real
                                company_from_github_lower = _real.lower()
                                # Don't return — continue processing with fixed name
                            else:
                                return
                        else:
                            return
                    except Exception:
                        return
                else:
                    return  # Skip garbage companies with no fix

        if job.get("is_closed", False):
            return

        is_valid_title, reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            # Try extracting title from URL slug before rejecting
            url_title = None
            try:
                from urllib.parse import urlparse, unquote
                path = unquote(urlparse(url).path)
                # Get last meaningful path segment
                segments = [s for s in path.split("/") if s and len(s) > 5]
                if segments:
                    slug = segments[-1]
                    # Remove IDs, hashes, query fragments
                    slug = re.sub(r"^[a-f0-9-]{8,}[-]?", "", slug)
                    slug = re.sub(r"[-_]", " ", slug).strip()
                    if len(slug) > 10:
                        url_title = TitleProcessor.clean_title_aggressive(slug)
                        valid2, _ = TitleProcessor.is_valid_job_title(url_title)
                        if valid2:
                            title = url_title
                            is_valid_title = True
            except Exception:
                pass
            if not is_valid_title:
                self.outcomes["skipped_invalid_title"] += 1
                self.source_stats[source]["rejected"] += 1
                self._print_rejected(company_from_github, f"Invalid title: {reason}")
                logging.info(
                    f"REJECTED | {company_from_github} | {title} | Invalid title: {reason}"
                )
                return

        resolved_url = url
        if "simplify.jobs" in url.lower():
            resolved_url, resolved = SimplifyRedirectResolver.resolve(url)
            if resolved_url == "__INACTIVE__":
                self.outcomes["skipped_inactive"] = self.outcomes.get("skipped_inactive", 0) + 1
                self._add_discarded(
                    company_from_github, title, location_from_github, "Unknown",
                    url, "N/A", "Internship", source, "Simplify listing inactive/closed",
                )
                logging.info(f"REJECTED | {company_from_github} | {title} | Simplify INACTIVE")
                return
            if not resolved:
                # Don't write Simplify wrapper URL — queue for retry and skip
                try:
                    from outreach.brain import Brain
                    _jid_m = __import__('re').search(r'/p/([a-f0-9-]+)', url)
                    if _jid_m:
                        Brain.get().queue_simplify_retry(_jid_m.group(1), url, "github_unresolved")
                except Exception:
                    pass
                self.outcomes["failed_simplify_resolution"] = self.outcomes.get("failed_simplify_resolution", 0) + 1
                logging.info(f"Simplify unresolved — queued for retry: {url[:60]}")
                return
                # Fallback: extract title from URL slug if current title is Unknown/generic
                import re as _url_re
                slug_match = _url_re.search(r'/p/[a-f0-9-]+/([A-Za-z0-9-]+)', url)
                if slug_match and (not title or title == 'Unknown' or len(title) < 5):
                    slug = slug_match.group(1).replace('?utm_source=swelist', '').replace('?utm_source=', '')
                    slug_title = slug.replace('-', ' ').strip()
                    if len(slug_title) >= 5:
                        slug_title = TitleProcessor.clean_title_aggressive(slug_title)
                        if slug_title and len(slug_title) >= 5:
                            title = slug_title
                            logging.info(f'Title from URL slug: {title}')
                # Use metadata from Simplify page if available
                try:
                    from aggregator.extractors import SimplifyRedirectResolver as _SRR
                    smeta = _SRR._last_metadata
                    if smeta.get("location"):
                        logging.info(f"Simplify metadata location available: {smeta['location']}")
                    if smeta.get("remote"):
                        # Store remote status for later use
                        _simplify_remote = smeta['remote']
                        logging.info(f"Using Simplify metadata remote: {smeta['remote']}")
                    if smeta.get("no_h1b"):
                        logging.info(f"Simplify: No H1B sponsorship for this role")
                    if smeta.get("no_h1b"):
                        logging.info(f"Simplify metadata: No H1B sponsorship detected")
                        # Reject immediately — no sponsorship confirmed by Simplify
                        self.outcomes["skipped_page_restriction"] = self.outcomes.get("skipped_page_restriction", 0) + 1
                        self._add_discarded(
                            company_from_github, title, location_from_github, "Unknown",
                            resolved_url, "N/A", "Internship", source,
                            "No H1B sponsorship (Simplify metadata)"
                        )
                        self._print_rejected(company_from_github, "No H1B sponsorship (Simplify)")
                        logging.info(f"REJECTED | {company_from_github} | {title} | No H1B (Simplify metadata)")
                        return
                except Exception:
                    pass

        # Detect Simplify URL-company mismatches (e.g. Ingram Micro URL for Bose job)
        # If the URL domain clearly belongs to a different known company, reject
        try:
            from urllib.parse import urlparse as _urlp
            _url_domain = _urlp(resolved_url).netloc.lower().replace("www.", "")
            _company_norm = re.sub(r"[^a-z0-9]", "", company_from_github.lower())
            # Extract company slug from domain (e.g. "ingrammicro" from "ingrammicro.wd5.myworkdayjobs.com")
            _domain_slug = _url_domain.split(".")[0].lower()
            # Special case: ashbyhq.com/company-name/job → extract company from path
            if "ashbyhq" in _url_domain or "jobs.ashbyhq" in _url_domain:
                try:
                    from urllib.parse import urlparse as _up2
                    _path_parts = [p for p in _up2(resolved_url).path.split("/") if p]
                    if _path_parts:
                        _ashby_company = re.sub(r"[^a-z0-9]", "", _path_parts[0].lower())
                        if _ashby_company and _ashby_company not in _company_norm and len(_ashby_company) > 3:
                            logging.info(f"Ashby URL company: {_path_parts[0]} vs hint: {company_from_github}")
                            if _ashby_company not in _company_norm and _company_norm not in _ashby_company:
                                logging.info(f"Ashby URL-company mismatch (logged only): {_path_parts[0]} vs {company_from_github}")
                except Exception:
                    pass
            _domain_slug = re.sub(r"[^a-z0-9]", "", _domain_slug)
            # Only flag mismatch if domain slug is a known company AND clearly != hint company
            _known_workday_companies = {
                "ingrammicro": "ingram micro",
                "synnex": "td synnex",
                "vishay": "vishay",
                "cooperstandard": "cooper standard",
                "edwards": "edwards lifesciences",
                "biorad": "bio-rad",
                "careers-biorad": "bio-rad",
                "arlo": "arlo",
                "revvity": "revvity",
                "hyperiongrp": "hyperion",
                "ffive": "f5",
                "pae": "amentum",
                "vhr-genband": "ribbon",
                "vhr-otsuka": "otsuka",
                "hcjy": "cooper companies",
                "nordsonhcm": "nordson",
                "vareximaging": "varex imaging",
                "sonyglobal": "sony",
                "rgare": "reinsurance group of america",
                "statestreet": "state street",
                "eversource": "eversource",
                "argonne": "argonne national laboratory",
                "primerica": "primerica",
                "bxp": "bxp",
                "teledyneetm": "teledyne etm",
                "cohu": "cohu",
                "situsaac": "situsamc",
                "dustyrobotics": "dusty robotics",
                "botauto": "bot auto",
                "moog": "moog",
                "takeda": "takeda",
                "socure": "socure",
                "dmatrix": "d-matrix",
                "ashbyhq": None,  # ashby is an ATS, not a company
            }
            if _domain_slug in _known_workday_companies:
                _expected = re.sub(r"[^a-z0-9]", "", _known_workday_companies[_domain_slug])
                if _expected not in _company_norm and _company_norm not in _expected:
                    # Log mismatch but continue — job still processed normally
                    logging.info(f"URL-COMPANY MISMATCH (logged only) | {company_from_github} | URL domain: {_domain_slug}")
        except Exception:
            pass

        if self._is_duplicate(company_from_github, title, resolved_url):
            return
        # Thread safety ensured via _github_lock for all shared state below

        is_internship, intern_reason = TitleProcessor.is_internship_role(title, github_category="Software Engineering Internship")
        if not is_internship:
            self.outcomes["skipped_senior_role"] += 1
            self.source_stats[source]["rejected"] += 1
            self._print_rejected(company_from_github, intern_reason)
            logging.info(
                f"REJECTED | {company_from_github} | {title} | {intern_reason}"
            )
            return

        season_ok, season_reason = TitleProcessor.check_season_requirement(title)
        if not season_ok:
            self.outcomes["skipped_wrong_season"] += 1
            self.source_stats[source]["rejected"] += 1
            self._print_rejected(company_from_github, season_reason)
            logging.info(
                f"REJECTED | {company_from_github} | {title} | {season_reason}"
            )
            return

        github_category = job.get("github_category", "")
        is_tech = TitleProcessor.is_cs_engineering_role(title)
        if not is_tech and github_category:
            logging.info(f"OVERRIDE | {company_from_github} | {title} | GitHub category: {github_category}")
            is_tech = True
        if not is_tech:
            self.outcomes["skipped_non_tech"] += 1
            self.source_stats[source]["rejected"] += 1
            self._print_rejected(company_from_github, "Not CS/Engineering")
            logging.info(
                f"REJECTED | {company_from_github} | {title} | Not a CS/Engineering role"
            )
            return

        company_lower = company_from_github.lower().strip()
        if any(bl.lower() == company_lower for bl in COMPANY_BLACKLIST):
            reason = COMPANY_BLACKLIST_REASONS.get(
                company_from_github, "Blacklisted company"
            )
            self.outcomes["skipped_blacklisted"] += 1
            self.source_stats[source]["rejected"] += 1
            self._add_discarded(
                company_from_github,
                title,
                location_from_github,
                "Unknown",
                resolved_url,
                "N/A",
                "Internship",
                source,
                reason,
            )
            self._print_rejected(company_from_github, f"Blacklisted")
            logging.info(
                f"REJECTED | {company_from_github} | {title} | Blacklisted: {reason}"
            )
            return

        # Sponsorship check disabled — unreliable and slow, handled by page fetch
        # sponsorship_github = _claude_sponsorship_check(company_from_github, title)

        # HQ fallback: if location still Unknown, try known company HQ
        if not location_from_github or location_from_github == "Unknown":
            try:
                from aggregator.config import COMPANY_HQ as _HQ
                _hq = _HQ.get(company_from_github.lower().strip())
                if _hq:
                    location_from_github = _hq
                    logging.info(f"HQ fallback: {company_from_github} → {_hq}")
            except Exception:
                pass

        # URL-based location extraction using Workday URL city slugs
        if not location_from_github or location_from_github == "Unknown":
            try:
                from aggregator.config import URL_CITY_STATE_MAP
                import re as _re_loc
                _loc_match = _re_loc.search(r"/job/([^/]+)/", resolved_url)
                if _loc_match:
                    _slug = _loc_match.group(1).lower()
                    # Remove country/state suffixes
                    for _sfx in ["-united-states-of-america", "-united-states", "-usa", "-us"]:
                        _slug = _slug.replace(_sfx, "")
                    # Try full slug first, then first part
                    _city_key = _slug.replace("-", " ").strip()
                    _city_key2 = _slug.split("-")[0]
                    _mapped = URL_CITY_STATE_MAP.get(_city_key) or URL_CITY_STATE_MAP.get(_city_key2) or URL_CITY_STATE_MAP.get(_slug)
                    if _mapped:
                        location_from_github = _mapped
                        logging.info(f"URL location: {company_from_github} → {_mapped}")
            except Exception:
                pass

        international_check = LocationProcessor.check_if_international(
            location_from_github, url=resolved_url, title=title
        )
        if international_check:
            self.outcomes["skipped_international"] += 1
            self.source_stats[source]["rejected"] += 1
            self._add_discarded(
                company_from_github,
                title,
                location_from_github,
                "Unknown",
                resolved_url,
                "N/A",
                "Internship",
                source,
                international_check,
            )
            short_reason = international_check.replace("Location: ", "")
            self._print_rejected(company_from_github, short_reason)
            logging.info(
                f"REJECTED | {company_from_github} | {title} | {international_check}"
            )
            return

        result = self._process_single_job_comprehensive(
            resolved_url,
            company_hint=company_from_github,
            title_hint=title,
            location_hint=location_from_github,
            source=source,
        )

        if result:
            alert = RoleCategorizer.get_terminal_alert(result["title"])
            company_display = result["company"][:TERMINAL_COMPANY_WIDTH]
            print(f"  {company_display}: ✓ Valid {alert}")
            with self._github_lock:
                self.source_stats[source]["valid"] += 1
        else:
            with self._github_lock:
                self.source_stats[source]["rejected"] += 1
            logging.info(f"REJECTED (comprehensive) | {company_from_github} | {title} | url={resolved_url[:60]}")

    def _process_emails_grouped(self, emails_data):
        processed_emails = ProcessedEmailTracker.load()
        email_counter = 0

        self._jobright_email_map = {}
        seen_jobright_urls = set()
        seen_jobright_company_titles = set()

        for email in emails_data:
            email_id = email["email_id"]
            sender = email["sender"]
            subject = email["subject"]
            html_content = email.get("html", "")
            urls = email["urls"]

            if email_id in processed_emails:
                logging.info(f"Skipping already processed email: {subject}")
                continue

            if sender == "ZipRecruiter" and html_content:
                zr_jobs = ZipRecruiterResolver.parse_email_jobs(html_content)
                if zr_jobs:
                    self._ziprecruiter_jobs_cache = zr_jobs
                    logging.info(f"Pre-parsed {len(zr_jobs)} ZipRecruiter jobs from: {subject}")

            if sender == "Jobright" and html_content:
                parsed_jobs = JobrightEmailParser.parse_email_jobs(html_content)
                if parsed_jobs:
                    self._jobright_email_map.update(parsed_jobs)
                    unique = len(set(id(v) for v in parsed_jobs.values()))
                    logging.info(f"Pre-parsed {unique} Jobright jobs from: {subject}")

            deduped_urls = []
            for url_entry in urls:
                # SWE List returns (url, company_hint, title_hint) tuples
                if isinstance(url_entry, tuple):
                    url, _co_hint, _ti_hint = url_entry
                else:
                    url, _co_hint, _ti_hint = url_entry, "", ""
                clean = re.sub(r"\?.*$", "", url).lower()
                if "jobright.ai/jobs/info/" in clean:
                    if clean in seen_jobright_urls:
                        self.outcomes["skipped_duplicate_url"] += 1
                        continue
                    seen_jobright_urls.add(clean)

                    fallback = self._get_jobright_email_fallback(url)
                    if fallback:
                        ct_key = URLCleaner.normalize_text(
                            f"{fallback.get('company', '')}_{fallback.get('title', '')}"
                        )
                        if (
                            ct_key in seen_jobright_company_titles
                            or ct_key in self.existing_jobs
                        ):
                            self.outcomes["skipped_duplicate_company_title"] += 1
                            continue
                        seen_jobright_company_titles.add(ct_key)

                deduped_urls.append(url_entry)  # preserve tuple for SWE List

            email_counter += 1
            pre_dedup = len(urls) - len(deduped_urls)
            dedup_parts = []
            if pre_dedup > 0:
                dedup_parts.append(f"{pre_dedup} pre-deduped")
            pre_msg = f" ({', '.join(dedup_parts)})" if dedup_parts else ""
            print(
                f"\n  Email #{email_counter}: {subject} ({sender}) - {len(deduped_urls)} URLs{pre_msg}"
            )

            inline_dups = 0
            # Process URLs in parallel (10 threads) for speed
            import concurrent.futures as _cf
            def _process_url(args):
                idx, url_entry = args
                # Handle SWE List (url, company, title) tuples
                if isinstance(url_entry, tuple):
                    url, _swe_co, _swe_ti = url_entry
                else:
                    url, _swe_co, _swe_ti = url_entry, "", ""
                # Build enhanced subject hint from SWE List structured data
                _effective_subject = subject
                if _swe_co and _swe_ti:
                    _effective_subject = f"{_swe_ti} @ {_swe_co}"
                try:
                    return self._process_single_email_url(
                        url, sender, html_content, _effective_subject,
                        url_idx=idx + 1, url_total=len(deduped_urls),
                    )
                except Exception as e:
                    logging.error(f"Failed to process email URL {url}: {e}")
                    return None

            with _cf.ThreadPoolExecutor(max_workers=10) as pool:
                results = list(pool.map(_process_url, enumerate(deduped_urls)))

            inline_dups = sum(1 for r in results if r == "duplicate")
            if inline_dups > 0:
                print(f"    [{inline_dups} duplicates skipped]")

            ProcessedEmailTracker.mark_email_processed(
                processed_emails, email_id, subject, len(urls)
            )

        ProcessedEmailTracker.save(processed_emails)

    def _process_single_email_url(
        self, url, sender, email_html, subject, url_idx=0, url_total=0
    ):
        if any(domain in url.lower() for domain in BLACKLIST_DOMAINS):
            return "skipped"

        is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
        if not is_valid_url:
            return "skipped"

        resolved_url = url
        is_company_site = False

        if "simplify.jobs" in url.lower():
            resolved_url, resolved = SimplifyRedirectResolver.resolve(url)
            if resolved_url == "__INACTIVE__":
                self.outcomes["skipped_inactive"] = self.outcomes.get("skipped_inactive", 0) + 1
                logging.info(f"REJECTED | Simplify INACTIVE | {url[:60]}")
                return "skipped"
            if not resolved:
                # Don't write Simplify wrapper URL — queue for retry and skip
                try:
                    from outreach.brain import Brain
                    _jid_m = __import__('re').search(r'/p/([a-f0-9-]+)', url)
                    if _jid_m:
                        Brain.get().queue_simplify_retry(_jid_m.group(1), url, "github_unresolved")
                except Exception:
                    pass
                self.outcomes["failed_simplify_resolution"] = self.outcomes.get("failed_simplify_resolution", 0) + 1
                logging.info(f"Simplify unresolved — queued for retry: {url[:60]}")
                return
                # Use metadata from Simplify page if available
                try:
                    from aggregator.extractors import SimplifyRedirectResolver as _SRR
                    smeta = _SRR._last_metadata
                    if smeta.get("location") and (not location_from_github or location_from_github == "Unknown"):
                        location_from_github = smeta["location"]
                        logging.info(f"Using Simplify metadata location: {location_from_github}")
                except Exception:
                    pass

        if "jobright.ai" in url.lower():
            self._process_jobright_url(url, sender, email_html, subject)
            return "processed"

        if "ziprecruiter.com" in url.lower():
            self._process_ziprecruiter_url(url, sender, email_html, subject)
            return "processed"

        if self._is_duplicate_url(resolved_url):
            self.outcomes["skipped_duplicate_url"] += 1
            return "duplicate"

        # Extract company and title hints from SWE List subject "Title @ Company | Simplify"
        _company_hint = ""
        _title_hint = ""
        if subject:
            import re as _re
            _at_match = _re.search(r'@\s*([^|]+?)(?:\s*\||\s*$)', subject)
            if _at_match:
                _company_hint = _at_match.group(1).strip()
            _title_match = _re.match(r'^(.+?)\s*@', subject)
            if _title_match:
                _title_hint = _title_match.group(1).strip()

        result = self._process_single_job_comprehensive(
            resolved_url,
            source=sender,
            email_html=email_html,
            company_hint=_company_hint or "",
            title_hint=_title_hint or "",
        )
        if result:
            # Cross-validate: if extracted company doesn't match hint, use hint
            # This catches SWE List URL/company mismatches
            if _company_hint and result.get("company", "Unknown") not in ("Unknown", ""):
                from aggregator.utils import CompanyNormalizer
                _extracted_norm = re.sub(r"[^a-z0-9]", "", result["company"].lower())
                _hint_norm = re.sub(r"[^a-z0-9]", "", _company_hint.lower())
                # If extracted company shares <40% of chars with hint, prefer hint
                _common = sum(1 for c in _hint_norm if c in _extracted_norm)
                if _hint_norm and _common / len(_hint_norm) < 0.4:
                    logging.info(
                        f"SWE List company mismatch: extracted='{result['company']}' "
                        f"hint='{_company_hint}' — using hint"
                    )
                    _cleaned_hint = CompanyNormalizer.normalize(_company_hint)
                    if _cleaned_hint:
                        result["company"] = _cleaned_hint
            if _title_hint and result.get("title", "Unknown") == "Unknown":
                result["title"] = _title_hint
            alert = RoleCategorizer.get_terminal_alert(result["title"])
            print(f"    {result['company'][:50]}: ✓ Valid {alert}")
            self.source_stats[sender]["valid"] += 1
            return "valid"
        else:
            self.source_stats[sender]["rejected"] += 1
            return "rejected"

    def _process_jobright_url(self, url, sender, email_html, subject):
        email_fallback = self._get_jobright_email_fallback(url)

        if email_fallback and email_fallback.get("title", "Unknown") != "Unknown":
            company = email_fallback.get("company", "Unknown")
            title = TitleProcessor.clean_title_aggressive(
                email_fallback.get("title", "Unknown")
            )
            location = email_fallback.get("location", "Unknown")

            logging.info(
                f"STEP 1 | Jobright email data: {company} | {title} | {location}"
            )

            if self._is_duplicate(company, title, url):
                logging.info(f"STEP 2 | Duplicate: {company} | {title}")
                return

            is_internship, intern_reason = TitleProcessor.is_internship_role(title)
            if not is_internship:
                self.outcomes["skipped_senior_role"] += 1
                self.source_stats[sender]["rejected"] += 1
                self._print_rejected(company, intern_reason)
                logging.info(
                    f"STEP 2 | REJECTED | {company} | {title} | {intern_reason}"
                )
                return

            is_tech = TitleProcessor.is_cs_engineering_role(title)
            if not is_tech:
                self.outcomes["skipped_non_tech"] += 1
                self.source_stats[sender]["rejected"] += 1
                self._print_rejected(company, "Not CS/Engineering")
                logging.info(
                    f"STEP 2 | REJECTED | {company} | {title} | Not CS/Engineering"
                )
                return

            company_lower = company.lower().strip()
            if any(bl.lower() == company_lower for bl in COMPANY_BLACKLIST):
                reason = COMPANY_BLACKLIST_REASONS.get(company, "Blacklisted")
                self.outcomes["skipped_blacklisted"] += 1
                self.source_stats[sender]["rejected"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Internship",
                    sender,
                    reason,
                )
                self._print_rejected(company, "Blacklisted")
                logging.info(f"STEP 2 | REJECTED | {company} | Blacklisted: {reason}")
                return

            international_check = LocationProcessor.check_if_international(
                location, url=url, title=title
            )
            if international_check:
                self.outcomes["skipped_international"] += 1
                self.source_stats[sender]["rejected"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Internship",
                    sender,
                    international_check,
                )
                short_reason = international_check.replace("Location: ", "")
                self._print_rejected(company, short_reason)
                logging.info(f"STEP 2 | REJECTED | {company} | {international_check}")
                return

            logging.info(f"STEP 3 | Extracting original URL for: {company} | {title}")
            actual_url = self._extract_original_job_post_url(url)

            if actual_url:
                logging.info(f"STEP 4 | Original URL found: {actual_url[:80]}")

                if self._is_duplicate_url(actual_url):
                    self.outcomes["skipped_duplicate_url"] += 1
                    logging.info(f"STEP 5 | Duplicate URL: {actual_url[:60]}")
                    return

                result = self._process_single_job_comprehensive(
                    actual_url,
                    company_hint=company,
                    title_hint=title,
                    location_hint=location,
                    source=sender,
                    email_html=email_html,
                )
                if result:
                    alert = RoleCategorizer.get_terminal_alert(result["title"])
                    print(
                        f"    {result['company'][:50]}: ✓ Valid {alert} (email→original)"
                    )
                    self.source_stats[sender]["valid"] += 1
                else:
                    self.source_stats[sender]["rejected"] += 1
                return

            logging.info(f"STEP 4 | Original URL NOT found for: {company} | {title}")
            logging.info(
                f"SKIPPED | {company} | {title} | Jobright original URL extraction failed"
            )
            self._print_rejected(company, "Jobright URL unresolved")
            self.outcomes["failed_jobright_resolution"] += 1
            self.source_stats[sender]["failed"] += 1
            return

        logging.info(f"STEP 1 | No email data for Jobright URL: {url[:60]}")

        if self._is_duplicate_url(url):
            self.outcomes["skipped_duplicate_url"] += 1
            return

        actual_url = self._extract_original_job_post_url(url)
        if actual_url:
            logging.info(f"STEP 2 | Original URL (no email data): {actual_url[:80]}")

            if self._is_duplicate_url(actual_url):
                self.outcomes["skipped_duplicate_url"] += 1
                return

            result = self._process_single_job_comprehensive(
                actual_url,
                source=sender,
                email_html=email_html,
            )
            if result:
                alert = RoleCategorizer.get_terminal_alert(result["title"])
                print(
                    f"    {result['company'][:50]}: ✓ Valid {alert} (jobright→original)"
                )
                self.source_stats[sender]["valid"] += 1
            else:
                self.source_stats[sender]["rejected"] += 1
            return

        logging.info(f"SKIPPED | Jobright URL unresolvable | {url[:60]}")
        self._print_rejected("Jobright", "URL unresolvable")
        self.outcomes["failed_jobright_resolution"] += 1
        self.source_stats[sender]["failed"] += 1


    # ── Dead URL patterns ──────────────────────────────────────────────────
    _DEAD_URL_PATTERNS = [
        "notfound=1", "not_found=true", "ss=1&notfound=1",
        "/jobnot found", "/job-not-found", "/position-not-available",
        "/jobs/search?ss=1", "/search?ss=1", "jobnotfound",
        "/careersmarketplace/error", "/careers/error", "/job/error",
        "/error?", "/jobs/error", "?error=true", "?error=404",
        "?not_found=true", "?notfound=true",
        "position-not-available", "job-not-available",
    ]

    _DEAD_PAGE_TITLES = [
        "not found", "page not found", "job not found", "no longer available",
        "position no longer", "posting is no longer", "job has been filled",
        "come work with us", "not ready to apply", "page does not exist",
        "this job is closed",
        "this opportunity is currently not available",
        "opportunity is not available",
        "this position is no longer available",
        "this role has been filled",
        "job posting has expired",
        "application deadline has passed",
        "no longer accepting applications", "job has expired", "position has been closed",
        "sorry, this job", "opening is no longer", "role is no longer",
        "no longer accepting", "job listing not found", "search results",
        "career opportunities", "all jobs", "explore opportunities",
        "join our team", "current openings", "working at ",
    ]

    def _is_dead_url(self, url):
        """Check if URL pattern indicates a dead/expired job posting."""
        if not url:
            return False
        url_lower = url.lower()
        for pattern in self._DEAD_URL_PATTERNS:
            if pattern in url_lower:
                return True
        return False

    def _is_dead_page(self, title, final_url=None):
        """Check if page title or final URL indicates an expired/dead posting."""
        if title:
            title_lower = title.lower().strip()
            for pattern in self._DEAD_PAGE_TITLES:
                if pattern in title_lower:
                    return True
        if final_url:
            return self._is_dead_url(final_url)
        return False

    def _process_ziprecruiter_url(self, url, sender, email_html, subject):
        """Process ZipRecruiter URL: try HTTP redirect first, fall back to pre-parsed email data."""
        try:
            # FIX 6: check expires param before fetching — skip if expired
            try:
                import urllib.parse as _urlparse, time as _time
                _parsed = _urlparse.urlparse(url)
                _params = _urlparse.parse_qs(_parsed.query)
                _expires = _params.get("expires", [None])[0]
                if _expires:
                    _exp_ts = int(_expires)
                    _now_ts = int(_time.time())
                    _age_days = (_now_ts - (_exp_ts - 345600)) / 86400  # expires is ~4 days after post
                    if _now_ts > _exp_ts:
                        logging.info(f"ZipRecruiter URL expired (expires={_expires}), skipping")
                        self.outcomes["skipped_too_old"] = self.outcomes.get("skipped_too_old", 0) + 1
                        return
                    # Also check if posted more than 3 days ago based on expires offset
                    if _age_days > 3:
                        logging.info(f"ZipRecruiter URL too old ({_age_days:.1f}d), skipping")
                        self.outcomes["skipped_too_old"] = self.outcomes.get("skipped_too_old", 0) + 1
                        return
            except Exception as _exp_e:
                logging.debug(f"ZipRecruiter expiry check failed: {_exp_e}")

            actual_url = None
            try:
                import requests as _req
                resp = _req.get(url, allow_redirects=True, timeout=10,
                               headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
                if resp and resp.status_code == 200 and resp.url != url and "ziprecruiter.com" not in resp.url:
                    actual_url = resp.url
                elif resp and resp.status_code == 200 and "ziprecruiter.com" in resp.url:
                    # Check posting age on ZipRecruiter page
                    import re as _re
                    age_match = _re.search(r"Posted\s+(\d+)\s+days?\s+ago", resp.text)
                    if age_match and int(age_match.group(1)) > 3:
                        logging.info(f"ZipRecruiter job too old: {age_match.group(0)}")
                        self.outcomes["skipped_too_old"] = self.outcomes.get("skipped_too_old", 0) + 1
                        return
                    age_match2 = _re.search(r"Posted\s+30\+\s+Days?\s+Ago", resp.text, _re.I)
                    if age_match2:
                        logging.info(f"ZipRecruiter job too old: 30+ days")
                        self.outcomes["skipped_too_old"] = self.outcomes.get("skipped_too_old", 0) + 1
                        return
                    actual_url = ZipRecruiterResolver.resolve(resp.url)
            except Exception as e:
                logging.debug(f"ZipRecruiter redirect failed: {e}")

            if actual_url:
                if self._is_duplicate_url(actual_url):
                    self.outcomes["skipped_duplicate_url"] += 1
                    return
                result = self._process_single_job_comprehensive(
                    actual_url, source=sender, email_html=email_html
                )
                if result:
                    alert = RoleCategorizer.get_terminal_alert(result["title"])
                    print(f"    {result['company'][:50]}: ✓ Valid {alert} (ZipRecruiter→resolved)")
                    self.source_stats[sender]["valid"] += 1
                else:
                    self.source_stats[sender]["rejected"] += 1
                return

            cached = self._match_ziprecruiter_cache(url)
            if not cached:
                self.outcomes["failed_ziprecruiter_resolution"] = self.outcomes.get("failed_ziprecruiter_resolution", 0) + 1
                return

            company = cached.get("company", "").strip()
            title = cached.get("title", "").strip()
            location = cached.get("location", "Unknown").strip()

            if not title or not company or company == "Unknown":
                self.outcomes["failed_ziprecruiter_resolution"] = self.outcomes.get("failed_ziprecruiter_resolution", 0) + 1
                return

            ct_key = URLCleaner.normalize_text(f"{company}_{title}")
            if ct_key in self.existing_jobs:
                self.outcomes["skipped_duplicate_company_title"] += 1
                return

            is_valid_title, reason = TitleProcessor.is_valid_job_title(title)
            if not is_valid_title:
                self.outcomes["skipped_invalid_title"] += 1
                self._print_rejected(company, f"Invalid title: {reason}")
                logging.info(f"REJECTED | {company} | {title} | Invalid title: {reason} | ZipRecruiter")
                self.source_stats[sender]["rejected"] += 1
                return

            is_intern = any(ind in title.lower() for ind in ["intern", "co-op", "coop", "apprentice"])
            if not is_intern:
                self.outcomes["skipped_not_internship"] = self.outcomes.get("skipped_not_internship", 0) + 1
                self._print_rejected(company, "Not internship")
                logging.info(f"REJECTED | {company} | {title} | Not internship | ZipRecruiter")
                self.source_stats[sender]["rejected"] += 1
                return

            # Early CS role check on title alone — avoids fetching non-CS pages
            _early_cs = TitleProcessor.is_cs_engineering_role(title)
            if not _early_cs:
                self.outcomes["skipped_non_tech"] = self.outcomes.get("skipped_non_tech", 0) + 1
                self._print_rejected(company, "Not CS/Engineering (title)")
                logging.info(f"REJECTED | {company} | {title} | Not CS/Engineering (early title check) | ZipRecruiter")
                self.source_stats[sender]["rejected"] += 1
                return
            # ── Full page validation on ZipRecruiter page itself ──────
            try:
                import requests as _req
                from aggregator.extractors import safe_parse_html as _sph
                zr_resp = _req.get(url, allow_redirects=True, timeout=10,
                                   headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
                if zr_resp and zr_resp.status_code == 200:
                    zr_soup, _ = _sph(zr_resp.text)
                    if zr_soup:
                        # Check CS/Engineering role using page description
                        zr_desc = zr_soup.get_text(separator=" ", strip=True)[:8000]
                        is_cs = TitleProcessor.is_cs_engineering_role(title, zr_desc)
                        if not is_cs:
                            self.outcomes["skipped_not_cs"] = self.outcomes.get("skipped_not_cs", 0) + 1
                            self._print_rejected(company, "Not CS/Engineering")
                            logging.info(f"REJECTED | {company} | {title} | Not a CS/Engineering role | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check undergraduate-only
                        ug_result, _ = ValidationHelper._check_undergraduate_only_requirements(zr_soup)
                        if ug_result:
                            self.outcomes["skipped_undergrad"] = self.outcomes.get("skipped_undergrad", 0) + 1
                            self._print_rejected(company, "Undergraduate students only")
                            logging.info(f"REJECTED | {company} | {title} | Undergraduate students only (MS not eligible) | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check PhD-only
                        phd_result, _ = ValidationHelper._check_phd_only_requirements(zr_soup)
                        if phd_result:
                            self.outcomes["skipped_phd"] = self.outcomes.get("skipped_phd", 0) + 1
                            self._print_rejected(company, "PhD students only")
                            logging.info(f"REJECTED | {company} | {title} | PhD students only | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check page age (e.g. "Posted 29 days ago")
                        zr_age = ValidationHelper.extract_page_age(zr_soup)
                        if zr_age is not None and zr_age > PAGE_AGE_THRESHOLD_DAYS:
                            self.outcomes["skipped_too_old"] += 1
                            self._print_rejected(company, f"Posted {zr_age}d ago")
                            logging.info(f"REJECTED | {company} | {title} | Posted {zr_age}d ago | ZipRecruiter")
                            self._add_discarded(company, title, location, "Unknown", url, "N/A", "Internship", sender, f"Posted {zr_age} days ago (max {PAGE_AGE_THRESHOLD_DAYS})")
                            self.source_stats[sender]["rejected"] += 1
                            return
            except Exception as _ze:
                logging.debug(f"ZipRecruiter page validation failed: {_ze}")

            remote = "Remote" if "remote" in location.lower() else "Unknown"
            if location.lower() in ("unknown", "", "n/a"):
                location = "Unknown"

            job_data = {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": "N/A",
                "job_type": "Internship",
                "sponsorship": "Unknown",
                "entry_date": self._format_date(),
                "source": sender,
            }

            quality = QualityScorer.calculate_score(job_data)
            if quality < MIN_QUALITY_SCORE:
                self.outcomes["skipped_low_quality"] += 1
                self._print_rejected(company, f"Low quality ({quality})")
                logging.info(f"REJECTED | {company} | {title} | Low quality: {quality} | ZipRecruiter")
                self.source_stats[sender]["rejected"] += 1
                return

            self.valid_jobs.append(job_data)
            self.outcomes["valid"] += 1
            self.existing_jobs.add(ct_key)
            alert = RoleCategorizer.get_terminal_alert(title)
            print(f"    {company[:50]}: ✓ Valid {alert} (ZipRecruiter)")
            self.source_stats[sender]["valid"] += 1
            logging.info(f"ACCEPTED | {company} | {title} | {location} | ZipRecruiter (email data)")

        except Exception as e:
            logging.error(f"ZipRecruiter processing failed for {url[:60]}: {e}")

    def _match_ziprecruiter_cache(self, url):
        if not hasattr(self, "_ziprecruiter_jobs_cache") or not self._ziprecruiter_jobs_cache:
            return None
        for job in self._ziprecruiter_jobs_cache:
            if job.get("url", "") == url:
                return job
        return None
    def _get_jobright_email_fallback(self, url):
        if not hasattr(self, "_jobright_email_map"):
            return None

        clean_url = re.sub(r"\?.*$", "", url).lower()
        if url in self._jobright_email_map:
            return self._jobright_email_map[url]
        if clean_url in self._jobright_email_map:
            return self._jobright_email_map[clean_url]

        for key, data in self._jobright_email_map.items():
            if clean_url in key.lower() or key.lower() in clean_url:
                return data

        return None

    def _extract_original_job_post_url(self, jobright_url):
        soup = None

        try:
            auth_response = self.jobright_auth.session.get(
                jobright_url,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                },
            )
            if auth_response and auth_response.status_code == 200:
                soup, _ = safe_parse_html(auth_response.content)
                logging.info(f"Jobright auth fetch OK: {jobright_url[:60]}")
        except Exception as e:
            logging.info(f"Jobright auth fetch failed: {e}")

        if soup:
            url = self._parse_original_url_from_soup(soup)
            if url:
                return url

        try:
            logging.info(f"Jobright trying Selenium: {jobright_url[:60]}")
            response, final_url, page_source = self.page_fetcher.fetch_page(
                jobright_url
            )
            if response:
                page_html = (
                    response.text if hasattr(response, "text") else str(response)
                )
                soup, _ = safe_parse_html(page_html)
                if soup:
                    url = self._parse_original_url_from_soup(soup)
                    if url:
                        return url
        except Exception as e:
            logging.info(f"Jobright Selenium fetch failed: {e}")

        logging.info(f"Jobright original URL not found: {jobright_url[:60]}")
        return None

    def _parse_original_url_from_soup(self, soup):
        try:
            origin_link = soup.find("a", class_=re.compile(r"index_origin"))
            if not origin_link:
                origin_link = soup.find(
                    "a", string=re.compile(r"original\s+job\s+post", re.I)
                )
            if not origin_link:
                for link in soup.find_all("a", href=True):
                    link_text = link.get_text(strip=True).lower()
                    if "original" in link_text and "job" in link_text:
                        href = link.get("href")
                        if href and "jobright.ai" not in href:
                            origin_link = link
                            break

            if origin_link:
                href = origin_link.get("href")
                if href and href.startswith("http") and "jobright.ai" not in href:
                    logging.info(f"Jobright original job post: {href[:80]}")
                    return href

            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if script_tag:
                data = json.loads(script_tag.string)
                job_result = (
                    data.get("props", {})
                    .get("pageProps", {})
                    .get("dataSource", {})
                    .get("jobResult", {})
                )
                actual_url = job_result.get("applyLink") or job_result.get(
                    "originalUrl"
                )
                if actual_url and "jobright.ai" not in actual_url:
                    logging.info(f"Jobright JSON data: {actual_url[:80]}")
                    return actual_url

        except Exception as e:
            logging.debug(f"Soup parsing failed: {e}")

        return None

    def _process_single_job_comprehensive(
        self,
        url,
        company_hint="",
        title_hint="",
        location_hint="",
        source="Unknown",
        email_html=None,
    ):
        try:
            # ── Pre-fetch dead URL check ──────────────────────────
            if self._is_dead_url(url):
                co = company_hint or "Unknown"
                ti = title_hint or "Unknown"
                self.outcomes["skipped_expired"] = self.outcomes.get("skipped_expired", 0) + 1
                self._print_rejected(co, "Job posting expired/unavailable")
                logging.info(f"REJECTED | {co} | {ti} | Job posting expired/unavailable (dead URL)")
                self._add_discarded(co, ti, "Unknown", "Unknown", url, "N/A", "Internship", source, "Job posting expired/unavailable")
                return None
            platform = PlatformDetector.detect(url)

            response, final_url, page_source = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                co = company_hint or "Unknown"
                ti = title_hint or "Unknown"
                self._print_rejected(co, "HTTP fetch failed")
                logging.info(f"REJECTED | {co} | {ti} | HTTP fetch failed | {url[:80]}")
                self._add_discarded(co, ti, location_hint or "Unknown", "Unknown", url, "N/A", "Internship", source, "HTTP fetch failed")
                return None

            # ── Post-fetch dead URL check ─────────────────────────
            if self._is_dead_url(final_url or ""):
                co = company_hint or "Unknown"
                ti = title_hint or "Unknown"
                self.outcomes["skipped_expired"] = self.outcomes.get("skipped_expired", 0) + 1
                self._print_rejected(co, "Job posting expired/unavailable")
                logging.info(f"REJECTED | {co} | {ti} | Job posting expired/unavailable (redirect)")
                self._add_discarded(co, ti, "Unknown", "Unknown", url, "N/A", "Internship", source, "Job posting expired/unavailable")
                return None

            soup, _ = safe_parse_html(
                response.text if hasattr(response, "text") else str(response)
            )
            if not soup:
                self.outcomes["failed_parse"] += 1
                co = company_hint or "Unknown"
                ti = title_hint or "Unknown"
                logging.info(f"REJECTED | {co} | {ti} | HTML parse failed | {url[:80]}")
                self._add_discarded(co, ti, location_hint or "Unknown", "Unknown", url, "N/A", "Internship", source, "HTML parse failed")
                return None

            # ── Post-parse dead page title check ──────────────────
            page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            if self._is_dead_page(page_title, final_url):
                co = company_hint or "Unknown"
                ti = title_hint or "Unknown"
                self.outcomes["skipped_expired"] = self.outcomes.get("skipped_expired", 0) + 1
                self._print_rejected(co, "Job posting expired/unavailable")
                logging.info(f"REJECTED | {co} | {ti} | Job posting expired/unavailable | Title: '{page_title[:60]}'")
                self._add_discarded(co, ti, "Unknown", "Unknown", url, "N/A", "Internship", source, "Job posting expired/unavailable")
                return None

            company = CompanyExtractor.extract_all_methods(final_url or url, soup)

            if self._is_garbage_company(company) and company_hint:
                company = company_hint
            elif not company or company == "Unknown":
                company = company_hint if company_hint else "Unknown"
            else:
                company_clean = CompanyExtractor.clean_company_name(company)
                if company_clean and not self._is_garbage_company(company_clean):
                    company = company_clean

            normalized = CompanyNormalizer.normalize(company, url)
            if normalized and not self._is_garbage_company(normalized):
                company = normalized
            # Auto-learn: save URL domain → company name for future runs
            try:
                from aggregator.processors import CompanyExtractor as _CE
                _CE.learn_company_name(final_url or url, company)
            except Exception:
                pass

            if self._is_garbage_company(company) and company_hint:
                company = company_hint

            # Apply company name normalization
            if company and company.lower().strip() in COMPANY_NAME_FIXES:
                fixed = COMPANY_NAME_FIXES[company.lower().strip()]
                if fixed != "Unknown":
                    logging.info(f"Company normalized: '{company}' → '{fixed}'")
                    company = fixed
                elif company_hint:
                    company = company_hint

            title = PageParser.extract_title(soup)
            if not title or title == "Unknown":
                title = title_hint if title_hint else "Unknown"
            title = TitleProcessor.clean_title_aggressive(title)
            # If extracted title looks like a page/company headline, trust hint
            if title_hint:
                _hint_clean = TitleProcessor.clean_title_aggressive(title_hint)
                _hint_intern, _ = TitleProcessor.is_internship_role(_hint_clean, github_category="Software Engineering Internship")
                _hint_valid, _ = TitleProcessor.is_valid_job_title(_hint_clean)
                _is_headline = "|" in title or len(title.split()) > 10 or any(
                    kw in title.lower() for kw in ["positions at", "careers at", "jobs at", "opportunities at", "join our", "work with us", "open roles"]
                )
                _is_intern, _ = TitleProcessor.is_internship_role(title, github_category="Software Engineering Internship")
                _is_valid, _ = TitleProcessor.is_valid_job_title(title)
                if (_is_headline or not _is_intern or not _is_valid) and _hint_intern and _hint_valid:
                    logging.info(f"Title override: page={title!r} hint={title_hint!r}")
                    title = _hint_clean

            # Duplicate check: only check existing_urls/jobs, NOT processing_lock
            # (processing_lock was already set by the caller for this URL)
            _clean = URLCleaner.clean_url(final_url or url)
            _norm = URLCleaner.normalize_text(f"{company}_{title}")
            with getattr(self, "_github_lock", _NOOP_LOCK):
                _url_dup = _clean in self.existing_urls
                _job_dup = _norm in self.existing_jobs
            if _url_dup:
                logging.info(f"DUPLICATE (url, post-fetch) | {company} | {title} | {_clean[:60]}")
                return None
            if _job_dup:
                logging.info(f"DUPLICATE (company+title, post-fetch) | {company} | {title}")
                return None

            is_valid_title, reason = TitleProcessor.is_valid_job_title(title)
            if not is_valid_title:
                self.outcomes["skipped_invalid_title"] += 1
                self._print_rejected(company, f"Invalid title: {reason}")
                logging.info(
                    f"REJECTED | {company} | {title} | Invalid title: {reason}"
                )
                return None

            is_internship, intern_reason = TitleProcessor.is_internship_role(
                title, page_text=soup.get_text()[:5000] if soup else ""
            )
            if not is_internship:
                self.outcomes["skipped_senior_role"] += 1
                self._add_discarded(
                    company,
                    title,
                    location_hint,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Full Time",
                    source,
                    intern_reason,
                )
                self._print_rejected(company, intern_reason)
                logging.info(f"REJECTED | {company} | {title} | {intern_reason}")
                return None

            season_ok, season_reason = TitleProcessor.check_season_requirement(
                title, page_text=soup.get_text()[:5000] if soup else ""
            )
            if not season_ok:
                self.outcomes["skipped_wrong_season"] += 1
                self._add_discarded(
                    company,
                    title,
                    location_hint,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    season_reason,
                )
                self._print_rejected(company, season_reason)
                logging.info(f"REJECTED | {company} | {title} | {season_reason}")
                return None

            is_tech = TitleProcessor.is_cs_engineering_role(
                title, description=soup.get_text()[:3000] if soup else ""
            )
            if not is_tech:
                self.outcomes["skipped_non_tech"] += 1
                self._add_discarded(
                    company,
                    title,
                    location_hint,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    "Not a CS/Engineering role",
                )
                self._print_rejected(company, "Not CS/Engineering")
                logging.info(
                    f"REJECTED | {company} | {title} | Not CS/Engineering role"
                )
                return None

            company_lower = company.lower().strip()
            if any(bl.lower() == company_lower for bl in COMPANY_BLACKLIST):
                reason = COMPANY_BLACKLIST_REASONS.get(company, "Blacklisted company")
                self.outcomes["skipped_blacklisted"] += 1
                self._add_discarded(
                    company,
                    title,
                    location_hint,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    reason,
                )
                self._print_rejected(company, "Blacklisted")
                logging.info(f"REJECTED | {company} | {title} | Blacklisted: {reason}")
                return None

            location = LocationExtractor.extract_all_methods(
                final_url or url,
                soup,
                title=title,
                platform=platform,
                page_source=page_source or "",
            )
            if (
                (not location or location == "Unknown")
                and location_hint
                and location_hint != "Unknown"
            ):
                location = location_hint
            # Normalize city-only locations to "City, ST" format
            if location and location != "Unknown":
                import re as _reloc
                if not _reloc.search(r',\s*[A-Z]{2}', location):
                    try:
                        from aggregator.config import CITY_TO_STATE_EXTRA
                        _llow = location.lower().strip()
                        _llow = _reloc.sub(r',?\s*(?:usa|united states)$', '', _llow).strip()
                        for _city, _st in CITY_TO_STATE_EXTRA.items():
                            if _city in _llow:
                                location = f"{_city.title()}, {_st}"
                                break
                    except Exception:
                        pass
            # Clean location: strip job type and remote words that leak into location
            if location and location != "Unknown":
                import re as _re
                location = _re.sub(r"(?i)^\s*(?:Internship|Full[- ]?Time|Part[- ]?Time|Co-?op|Contract|Temporary)\s*[,;]\s*", "", location)
                location = _re.sub(r"(?i)\s*[,;]\s*(?:Internship|Full[- ]?Time|Part[- ]?Time|Co-?op|Contract|Temporary)\s*$", "", location)
                # Strip remote status leaked into location
                location = _re.sub(r"(?i)\s*,?\s*(?:Hybrid|In Person|On Site|On-Site|Remote)\s*,?\s*(?:in-office.*)?$", "", location)
                location = _re.sub(r"(?i)^\s*(?:Hybrid|In Person|On Site|On-Site|Remote)\s*,?\s*", "", location)
                # Fix cases like "City, STHybrid" where no space between state and remote
                location = _re.sub(r"([A-Z]{2})(?:Hybrid|Remote|On Site|In Person).*$", r"\1", location)
                location = location.strip().strip(",").strip()

            international_check = LocationProcessor.check_if_international(
                location, soup=soup, url=final_url or url, title=title
            )
            if international_check:
                self.outcomes["skipped_international"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    international_check,
                )
                short_reason = international_check.replace("Location: ", "")
                self._print_rejected(company, short_reason)
                logging.info(f"REJECTED | {company} | {title} | {international_check}")
                return None

            company_intl = LocationProcessor.check_company_for_international(company)
            if company_intl:
                self.outcomes["skipped_international"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    company_intl,
                )
                self._print_rejected(company, "International (company name)")
                logging.info(f"REJECTED | {company} | {title} | {company_intl}")
                return None

            page_decision, page_reason, _ = ValidationHelper.check_page_restrictions(
                soup
            )
            if page_decision == "REJECT":
                self.outcomes["skipped_page_restriction"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    page_reason,
                )
                self._print_rejected(company, page_reason)
                logging.info(f"REJECTED | {company} | {title} | {page_reason}")
                return None

            page_age = ValidationHelper.extract_page_age(soup)
            if page_age is not None and page_age > PAGE_AGE_THRESHOLD_DAYS:
                self.outcomes["skipped_too_old"] += 1
                self._add_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    final_url or url,
                    "N/A",
                    "Internship",
                    source,
                    f"Posted {page_age} days ago (max {PAGE_AGE_THRESHOLD_DAYS})",
                )
                self._print_rejected(company, f"Posted {page_age}d ago")
                logging.info(f"REJECTED | {company} | {title} | Posted {page_age}d ago")
                return None

            # Salary check — reject if listed and under $25/hr
            sal_dec, sal_reason = ValidationHelper.check_salary_requirement(soup)
            if sal_dec == "REJECT":
                self.outcomes["skipped_low_salary"] = self.outcomes.get("skipped_low_salary", 0) + 1
                self._add_discarded(company, title, location, "Unknown",
                    final_url or url, "N/A", "Internship", source, sal_reason)
                self._print_rejected(company, sal_reason)
                logging.info(f"REJECTED | {company} | {title} | {sal_reason}")
                return None

            remote = LocationProcessor.extract_remote_status_enhanced(
                soup,
                location,
                final_url or url,
                description=soup.get_text()[:2000] if soup else "",
            )
            job_id = PageParser.extract_job_id(soup, final_url or url)
            sponsorship = ValidationHelper.check_sponsorship_status(soup)

            # Use original URL if redirect crossed to different domain (prevents company/URL mismatch)
            _store_url = final_url or url
            if final_url and url:
                try:
                    from urllib.parse import urlparse as _up
                    _orig_domain = _up(url).netloc.lower()
                    _final_domain = _up(final_url).netloc.lower()
                    # If domains differ significantly (not just www vs non-www), use original
                    _o = _orig_domain.replace("www.", "")
                    _f = _final_domain.replace("www.", "")
                    if _o != _f and not _f.endswith(_o) and not _o.endswith(_f):
                        _store_url = url
                        logging.debug(f"Domain mismatch: {_orig_domain} → {_final_domain}, using original URL")
                except Exception:
                    pass

            job_data = {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": _store_url,
                "job_id": "N/A" if _store_url and "greenhouse.io" in _store_url.lower() else (job_id if job_id else "N/A"),
                "job_type": "Internship",
                "sponsorship": sponsorship,
                "entry_date": self._format_date(),
                "source": source,
            }

            quality = QualityScorer.calculate_score(job_data)
            if quality < MIN_QUALITY_SCORE:
                self.outcomes["skipped_low_quality"] += 1
                self._print_rejected(company, f"Low quality ({quality})")
                logging.info(f"REJECTED | {company} | {title} | Low quality: {quality}")
                return None

            with getattr(self, "_github_lock", _NOOP_LOCK):
                self.valid_jobs.append(job_data)
                self.outcomes["valid"] += 1
                self.existing_urls.add(URLCleaner.clean_url(final_url or url))
                self.existing_jobs.add(URLCleaner.normalize_text(f"{company}_{title}"))
            if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
                self.existing_job_ids.add(job_id.lower())
                try:
                    from outreach.brain import Brain
                    Brain.get().register_job_id(job_id, company, title)
                except Exception:
                    pass

            logging.info(f"ACCEPTED | {company} | {title} | {location} | {source}")
            return job_data

        except Exception as e:
            logging.error(f"Processing failed for {url}: {e}", exc_info=True)
            return None

    def _is_garbage_company(self, name):
        if not name:
            return True
        return name.lower().strip() in GARBAGE_COMPANY_NAMES

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        with getattr(self, "_github_lock", _NOOP_LOCK):
            if job_id and job_id not in ("N/A", "") and not job_id.startswith("HASH_"):
                try:
                    from outreach.brain import Brain
                    if Brain.get().is_duplicate_job_id(job_id, company, title):
                        self.outcomes["skipped_duplicate_job_id"] += 1
                        logging.info(f"DUPLICATE (job_id) | {company} | {title}")
                        return True
                except Exception:
                    pass
            clean_url = URLCleaner.clean_url(url)
            if clean_url in self.existing_urls or clean_url in self.processing_lock:
                self.outcomes["skipped_duplicate_url"] += 1
                logging.info(f"DUPLICATE (url) | {company} | {title} | {url[:60]}")
                return True
            norm_key = URLCleaner.normalize_text(f"{company}_{title}")
            if norm_key in self.existing_jobs or norm_key in self.processing_lock:
                self.outcomes["skipped_duplicate_company_title"] += 1
                logging.info(f"DUPLICATE (company+title) | {company} | {title}")
                return True
            # TF-IDF fuzzy dedup: catch near-duplicates like
            # "Software Engineering Intern" vs "Software Engineer - Intern"
            try:
                if not hasattr(self, "_similarity_engine"):
                    from analytics.similarity import TitleSimilarity
                    self._similarity_engine = TitleSimilarity()
                    for existing in self.existing_jobs:
                        parts = existing.split("_", 1)
                        if len(parts) == 2:
                            self._similarity_engine.add(parts[1], company=parts[0])
                match = self._similarity_engine.is_near_duplicate(title, company=company, threshold=0.90)
                if match:
                    self.outcomes["skipped_duplicate_fuzzy"] = self.outcomes.get("skipped_duplicate_fuzzy", 0) + 1
                    logging.info(f"DUPLICATE (fuzzy) | {company} | {title} ≈ {match.title} ({match.score:.2f})")
                    return True
                self._similarity_engine.add(title, company=company)
            except Exception:
                pass
            # Add norm_key to processing_lock so parallel threads see it as duplicate
            self.processing_lock.add(norm_key)
            self.processing_lock.add(clean_url)
            if (
                job_id
                and job_id != "N/A"
                and not job_id.startswith("HASH_")
                and job_id.lower() in self.existing_job_ids
            ):
                self.outcomes["skipped_duplicate_job_id"] += 1
                logging.info(f"DUPLICATE (job_id2) | {company} | {title}")
                return True
            self.processing_lock.add(clean_url)
            return False

    def _is_duplicate_url(self, url):
        clean_url = URLCleaner.clean_url(url)
        return clean_url in self.existing_urls or clean_url in self.processing_lock

    def _add_discarded(
        self,
        company,
        title,
        location,
        remote,
        url,
        job_id,
        job_type,
        source,
        reason,
    ):
        with getattr(self, "_github_lock", _NOOP_LOCK):
            # Dedup: skip if same URL+reason already discarded
            _url_key = (url, reason)
            if not hasattr(self, "_discarded_url_seen"):
                self._discarded_url_seen = set()
            if _url_key in self._discarded_url_seen:
                return
            self._discarded_url_seen.add(_url_key)
            self.discarded_jobs.append(
                {
                    "company": company,
                    "title": title,
                    "location": location,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "job_type": job_type,
                    "source": source,
                    "reason": reason,
                    "entry_date": self._format_date(),
                    "sponsorship": "Unknown",
                }
            )
            self.outcomes["discarded"] += 1
        # Register discarded job_id in Brain to prevent re-processing
        if job_id and job_id not in ("N/A", "") and not job_id.startswith("HASH_"):
            try:
                from outreach.brain import Brain
                Brain.get().register_job_id(job_id, company, title)
            except Exception:
                pass
        # Soft-track company rejection in Brain (for weekly review — NOT auto-blacklist)
        if company and company not in ("Unknown", "N/A", ""):
            try:
                from outreach.brain import Brain
                Brain.get().record_company_rejection(company, reason)
            except Exception:
                pass

    def _print_rejected(self, company, reason):
        display = (company or "Unknown")
        logging.info(f"REJECTED | {display} | {reason}")
        if not getattr(self, "_github_mode", False):
            print(f"    {display}: ✗ {reason}")

    def _ensure_mutual_exclusion(self):
        if not self.valid_jobs or not self.discarded_jobs:
            return
        valid_keys = {
            (
                URLCleaner.normalize_text(j["company"]),
                URLCleaner.normalize_text(j["title"]),
            )
            for j in self.valid_jobs
        }
        discarded_keys = {
            (
                URLCleaner.normalize_text(j["company"]),
                URLCleaner.normalize_text(j["title"]),
            )
            for j in self.discarded_jobs
        }
        overlap = valid_keys & discarded_keys
        if overlap:
            self.valid_jobs = [
                j
                for j in self.valid_jobs
                if (
                    URLCleaner.normalize_text(j["company"]),
                    URLCleaner.normalize_text(j["title"]),
                )
                not in overlap
            ]
            self.outcomes["valid"] = len(self.valid_jobs)

    def _print_summary(self):
        print("\n" + "=" * 80)
        print("SUMMARY:")
        print("=" * 80)

        summary_items = [
            ("✓ Valid", self.outcomes["valid"]),
            ("✗ Discarded", self.outcomes["discarded"]),
            ("⊘ Duplicate URL", self.outcomes["skipped_duplicate_url"]),
            ("⊘ Duplicate job", self.outcomes["skipped_duplicate_company_title"]),
            ("⊘ Duplicate ID", self.outcomes["skipped_duplicate_job_id"]),
            ("⊘ Too old", self.outcomes.get("skipped_too_old", 0)),
            ("⊘ Wrong season", self.outcomes["skipped_wrong_season"]),
            ("⊘ Senior role", self.outcomes["skipped_senior_role"]),
            ("⊘ Non-tech", self.outcomes["skipped_non_tech"]),
            ("⊘ Invalid title", self.outcomes.get("skipped_invalid_title", 0)),
            ("⊘ International", self.outcomes.get("skipped_international", 0)),
            ("⊘ Blacklisted", self.outcomes["skipped_blacklisted"]),
            ("⊘ Page restriction", self.outcomes.get("skipped_page_restriction", 0)),
            ("⊘ Low quality", self.outcomes["skipped_low_quality"]),
            ("✗ HTTP failed", self.outcomes["failed_http"]),
            ("✗ Parse failed", self.outcomes["failed_parse"]),
            ("✗ Jobright unresolved", self.outcomes["failed_jobright_resolution"]),
            ("✗ ZipRecruiter unresolved", self.outcomes.get("failed_ziprecruiter_resolution", 0)),
            ("⊘ Low salary", self.outcomes.get("skipped_low_salary", 0)),
        ]
        for label, count in summary_items:
            if count > 0:
                print(f"  {label}: {count}")

        if self.source_stats:
            print("\n  BY SOURCE:")
            for source_name in sorted(self.source_stats.keys()):
                stats = self.source_stats[source_name]
                v = stats.get("valid", 0)
                r = stats.get("rejected", 0)
                f_count = stats.get("failed", 0)
                parts = []
                if v:
                    parts.append(f"{v} valid")
                if r:
                    parts.append(f"{r} rejected")
                if f_count:
                    parts.append(f"{f_count} failed")
                if parts:
                    print(f"    {source_name}: {', '.join(parts)}")

        rejection_reasons = defaultdict(int)
        for job in self.discarded_jobs:
            reason = job.get("reason", "Unknown")
            short = reason.split(":")[0].split("(")[0].strip()[:40]
            rejection_reasons[short] += 1

        if rejection_reasons:
            print("\n  TOP REJECTION REASONS:")
            sorted_reasons = sorted(
                rejection_reasons.items(), key=lambda x: x[1], reverse=True
            )
            for reason, count in sorted_reasons[:10]:
                print(f"    {reason}: {count}")

        print("=" * 80)

    @staticmethod
    def _parse_github_age(age_str):
        if not age_str:
            return None
        age_str = age_str.strip().lower()
        # Format: "1mo", "2mo" → months
        mo_match = re.match(r"^(\d+)mo$", age_str)
        if mo_match:
            return int(mo_match.group(1)) * 30
        # Format: "5d" → 5 days
        match = re.match(r"^(\d+)d$", age_str)
        if match:
            return int(match.group(1))
        # Format: "2mo" → 60 days
        match = re.match(r"^(\d+)mo$", age_str.lower())
        if match:
            return int(match.group(1)) * 30
        # Format: "Oct 15", "Feb 19" etc — vanshb03 calendar dates
        import datetime as _dt
        month_map = {
            "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
            "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12
        }
        cal_match = re.match(r"^([A-Za-z]{3})\s+(\d{1,2})$", age_str.strip())
        if cal_match:
            mon = cal_match.group(1).lower()
            day = int(cal_match.group(2))
            if mon in month_map:
                today = _dt.date.today()
                # Try current year first
                try:
                    candidate = _dt.date(today.year, month_map[mon], day)
                except ValueError:
                    return 999
                # If candidate is in the future, it must be from last year
                if candidate > today:
                    try:
                        candidate = _dt.date(today.year - 1, month_map[mon], day)
                    except ValueError:
                        return 999
                days_ago = (today - candidate).days
                return days_ago
        return DateParser.extract_days_ago(age_str)

    @staticmethod
    def _format_date():
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    @staticmethod
    def _looks_like_title(text):
        if not text:
            return False
        return (
            sum(
                1
                for kw in {"intern", "co-op", "engineer", "developer", "software"}
                if kw in text.lower()
            )
            >= 2
        )

    @staticmethod
    def _safe_scrape(url, source_name):
        try:
            return SimplifyGitHubScraper.scrape(url, source_name=source_name)
        except Exception as e:
            print(f"  ✗ {source_name} error: {e}")
            logging.error(f"{source_name} scraping failed: {e}")
            return []


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()

