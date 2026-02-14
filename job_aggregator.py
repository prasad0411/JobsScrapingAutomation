#!/usr/bin/env python3

import time
import datetime
import random
import re
import json
import os
import logging
from collections import defaultdict
from bs4 import BeautifulSoup

from config import (
    SIMPLIFY_URL,
    VANSHB03_URL,
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

from extractors import (
    EmailExtractor,
    PageFetcher,
    PageParser,
    SourceParsers,
    JobrightAuthenticator,
    JobrightRedirectResolver,
    SimplifyRedirectResolver,
    SimplifyGitHubScraper,
    safe_parse_html,
    retry_request,
)

from processors import (
    TitleProcessor,
    LocationExtractor,
    LocationProcessor,
    ValidationHelper,
    CompanyExtractor,
    QualityScorer,
    log_detailed_rejection,
)

from sheets_manager import SheetsManager

from utils import (
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
    filename="skipped_jobs.log",
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open("skipped_jobs.log", "w") as f:
    f.write("=" * 100 + "\n")
    f.write(
        f"JOB PROCESSING LOG - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    f.write("=" * 100 + "\n\n")


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

        print(
            f"Loaded: {len(self.existing_jobs)} jobs, {len(self.existing_urls)} URLs, {len(self.existing_job_ids)} IDs"
        )
        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs from sheets")

    def run(self):
        start_time = time.time()

        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()

        print("Scraping GitHub repositories...")
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

        rows = self.sheets.get_next_row_numbers()
        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )
        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )

        self._print_summary()
        elapsed = time.time() - start_time
        print(f"\n✓ DONE: {added_valid} valid, {added_discarded} discarded")
        print(f"Execution time: {elapsed / 60:.1f} minutes")
        print("=" * 80 + "\n")

        logging.info(f"SUMMARY: {added_valid} valid, {added_discarded} discarded")

    def _scrape_simplify_github(self):
        simplify_jobs = self._safe_scrape(SIMPLIFY_URL, "SimplifyJobs")
        vanshb03_jobs = self._safe_scrape(VANSHB03_URL, "vanshb03")

        print(
            f"  Total: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03\n"
        )
        logging.info(
            f"GitHub: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03"
        )

        for i, job in enumerate(simplify_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(simplify_jobs) - i
                    logging.info(
                        f"SimplifyJobs: Early exit - skipped {skipped} old jobs"
                    )
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"Failed to process SimplifyJobs job: {e}", exc_info=True)
                continue

        for i, job in enumerate(vanshb03_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(vanshb03_jobs) - i
                    logging.info(f"vanshb03: Early exit - skipped {skipped} old jobs")
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"Failed to process vanshb03 job: {e}", exc_info=True)
                continue

        github_valid = sum(
            1 for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]
        )
        print(f"  GitHub summary: {github_valid} valid jobs\n")
        logging.info(f"GitHub summary: {github_valid} valid jobs")

    def _process_single_github_job(self, job):
        title = TitleProcessor.clean_title_aggressive(job["title"])
        url = job["url"]
        source = job.get("source", "GitHub")
        company_from_github = job.get("company", "Unknown")
        location_from_github = job.get("location", "Unknown")

        if job.get("is_closed", False):
            return

        is_valid_title, reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            self.outcomes["skipped_invalid_title"] += 1
            logging.info(
                f"REJECTED | {company_from_github} | {title} | Invalid title: {reason}"
            )
            return

        resolved_url = url
        if "simplify.jobs" in url.lower():
            resolved_url, resolved = SimplifyRedirectResolver.resolve(url)
            if not resolved:
                resolved_url = url

        if self._is_duplicate(company_from_github, title, resolved_url):
            return

        is_internship, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_internship:
            self.outcomes["skipped_senior_role"] += 1
            logging.info(
                f"REJECTED | {company_from_github} | {title} | {intern_reason}"
            )
            return

        season_ok, season_reason = TitleProcessor.check_season_requirement(title)
        if not season_ok:
            self.outcomes["skipped_wrong_season"] += 1
            logging.info(
                f"REJECTED | {company_from_github} | {title} | {season_reason}"
            )
            return

        is_tech = TitleProcessor.is_cs_engineering_role(title)
        if not is_tech:
            self.outcomes["skipped_non_tech"] += 1
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
            logging.info(
                f"REJECTED | {company_from_github} | {title} | Blacklisted: {reason}"
            )
            return

        international_check = LocationProcessor.check_if_international(
            location_from_github, url=resolved_url, title=title
        )
        if international_check:
            self.outcomes["skipped_international"] += 1
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
        else:
            if not any(resolved_url in d.get("url", "") for d in self.discarded_jobs):
                pass

    def _process_emails_grouped(self, emails_data):
        processed_emails = ProcessedEmailTracker.load()
        email_counter = 0

        for email in emails_data:
            email_id = email["email_id"]
            sender = email["sender"]
            subject = email["subject"]
            html_content = email.get("html", "")
            urls = email["urls"]

            if email_id in processed_emails:
                logging.info(f"Skipping already processed email: {subject}")
                continue

            email_counter += 1
            print(
                f"\n  Email #{email_counter}: {subject} ({sender}) - {len(urls)} URLs"
            )

            for url in urls:
                try:
                    self._process_single_email_url(url, sender, html_content, subject)
                except Exception as e:
                    logging.error(f"Failed to process email URL {url}: {e}")
                    continue

            ProcessedEmailTracker.mark_email_processed(
                processed_emails, email_id, subject, len(urls)
            )

        ProcessedEmailTracker.save(processed_emails)

    def _process_single_email_url(self, url, sender, email_html, subject):
        if any(domain in url.lower() for domain in BLACKLIST_DOMAINS):
            return

        is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
        if not is_valid_url:
            return

        resolved_url = url
        is_company_site = False

        if "simplify.jobs" in url.lower():
            resolved_url, resolved = SimplifyRedirectResolver.resolve(url)
            if not resolved:
                resolved_url = url

        if "jobright.ai" in url.lower():
            resolved_url, is_company_site = JobrightRedirectResolver.resolve(
                url, email_html=email_html
            )

        if self._is_duplicate_url(resolved_url):
            self.outcomes["skipped_duplicate_url"] += 1
            return

        if "jobright.ai" in resolved_url.lower() and sender == "Jobright":
            jobright_data = None
            try:
                response = retry_request(resolved_url, max_retries=2)
                if response and response.status_code == 200:
                    soup, _ = safe_parse_html(response.content)
                    if soup:
                        jobright_data = PageParser.extract_jobright_data(
                            soup, resolved_url, self.jobright_auth
                        )
            except Exception:
                pass

            if jobright_data:
                actual_url = jobright_data.get("url", resolved_url)
                if "jobright.ai" not in actual_url and jobright_data.get(
                    "is_company_site", False
                ):
                    resolved_url = actual_url

                company = jobright_data.get("company", "Unknown")
                title = TitleProcessor.clean_title_aggressive(
                    jobright_data.get("title", "Unknown")
                )
                location = jobright_data.get("location", "Unknown")

                if self._is_duplicate(company, title, resolved_url):
                    return

                is_internship, intern_reason = TitleProcessor.is_internship_role(title)
                if not is_internship:
                    self.outcomes["skipped_senior_role"] += 1
                    logging.info(
                        f"REJECTED | {company} | {title} | {intern_reason} | Email: {subject}"
                    )
                    return

                result = self._process_single_job_comprehensive(
                    resolved_url,
                    company_hint=company,
                    title_hint=title,
                    location_hint=location,
                    source=sender,
                    email_html=email_html,
                )
                if result:
                    alert = RoleCategorizer.get_terminal_alert(result["title"])
                    print(f"    {result['company'][:50]}: ✓ Valid {alert}")
                return

        result = self._process_single_job_comprehensive(
            resolved_url,
            source=sender,
            email_html=email_html,
        )
        if result:
            alert = RoleCategorizer.get_terminal_alert(result["title"])
            print(f"    {result['company'][:50]}: ✓ Valid {alert}")

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
            platform = PlatformDetector.detect(url)

            response, final_url, page_source = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                logging.info(f"HTTP FAIL | {url[:80]}")
                return None

            soup, _ = safe_parse_html(
                response.text if hasattr(response, "text") else str(response)
            )
            if not soup:
                self.outcomes["failed_parse"] += 1
                return None

            company = CompanyExtractor.extract_all_methods(final_url or url, soup)
            if not company or company == "Unknown":
                company = company_hint if company_hint else "Unknown"

            company_clean = CompanyExtractor.clean_company_name(company)
            if company_clean and company_clean != "Unknown":
                company = company_clean

            normalized = CompanyNormalizer.normalize(company, url)
            if normalized:
                company = normalized

            title = PageParser.extract_title(soup)
            if not title or title == "Unknown":
                title = title_hint if title_hint else "Unknown"
            title = TitleProcessor.clean_title_aggressive(title)

            if self._is_duplicate(company, title, final_url or url):
                return None

            is_valid_title, reason = TitleProcessor.is_valid_job_title(title)
            if not is_valid_title:
                self.outcomes["skipped_invalid_title"] += 1
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
                logging.info(f"REJECTED | {company} | {title} | Posted {page_age}d ago")
                return None

            remote = LocationProcessor.extract_remote_status_enhanced(
                soup,
                location,
                final_url or url,
                description=soup.get_text()[:2000] if soup else "",
            )
            job_id = PageParser.extract_job_id(soup, final_url or url)
            sponsorship = ValidationHelper.check_sponsorship_status(soup)

            job_data = {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": final_url or url,
                "job_id": job_id if job_id else "N/A",
                "job_type": "Internship",
                "sponsorship": sponsorship,
                "entry_date": self._format_date(),
                "source": source,
            }

            quality = QualityScorer.calculate_score(job_data)
            if quality < MIN_QUALITY_SCORE:
                self.outcomes["skipped_low_quality"] += 1
                logging.info(f"REJECTED | {company} | {title} | Low quality: {quality}")
                return None

            self.valid_jobs.append(job_data)
            self.outcomes["valid"] += 1
            self.existing_urls.add(URLCleaner.clean_url(final_url or url))
            self.existing_jobs.add(URLCleaner.normalize_text(f"{company}_{title}"))
            if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
                self.existing_job_ids.add(job_id.lower())

            logging.info(f"ACCEPTED | {company} | {title} | {location} | {source}")
            return job_data

        except Exception as e:
            logging.error(f"Processing failed for {url}: {e}", exc_info=True)
            return None

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        clean_url = URLCleaner.clean_url(url)
        if clean_url in self.existing_urls or clean_url in self.processing_lock:
            self.outcomes["skipped_duplicate_url"] += 1
            return True

        norm_key = URLCleaner.normalize_text(f"{company}_{title}")
        if norm_key in self.existing_jobs:
            self.outcomes["skipped_duplicate_company_title"] += 1
            return True

        if (
            job_id
            and job_id != "N/A"
            and not job_id.startswith("HASH_")
            and job_id.lower() in self.existing_job_ids
        ):
            self.outcomes["skipped_duplicate_job_id"] += 1
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
            ("⊘ Too old", self.outcomes["skipped_too_old"]),
            ("⊘ Wrong season", self.outcomes["skipped_wrong_season"]),
            ("⊘ Senior role", self.outcomes["skipped_senior_role"]),
            ("⊘ Non-tech", self.outcomes["skipped_non_tech"]),
            ("⊘ Invalid title", self.outcomes["skipped_invalid_title"]),
            ("⊘ International", self.outcomes["skipped_international"]),
            ("⊘ Blacklisted", self.outcomes["skipped_blacklisted"]),
            ("⊘ Page restriction", self.outcomes["skipped_page_restriction"]),
            ("⊘ Low quality", self.outcomes["skipped_low_quality"]),
            ("✗ HTTP failed", self.outcomes["failed_http"]),
            ("✗ Parse failed", self.outcomes["failed_parse"]),
        ]
        for label, count in summary_items:
            if count > 0:
                print(f"  {label}: {count}")
        print("=" * 80)

    @staticmethod
    def _parse_github_age(age_str):
        if not age_str:
            return None
        match = re.match(r"^(\d+)d$", age_str.lower())
        if match:
            return int(match.group(1))
        match = re.match(r"^(\d+)mo$", age_str.lower())
        if match:
            return int(match.group(1)) * 30
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
