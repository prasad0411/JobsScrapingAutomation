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

from aggregator.config import (
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
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open(os.path.join(".local", "skipped_jobs.log"), "w") as f:
    f.write("=" * 100 + "\n")
    f.write(
        f"JOB PROCESSING LOG - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    f.write("=" * 100 + "\n\n")

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
        self.source_stats = defaultdict(lambda: defaultdict(int))

        print(
            # (loaded silently)
        )
        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs from sheets")

    def run(self):
        start_time = time.time()

        if not self.jobright_auth.cookies:
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
        self._github_mode = True

        logging.info(
            f"GitHub: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03"
        )

        print(f"\n  Processing Simplify repository...")
        consecutive_old = 0
        simplify_valid = 0
        simplify_rejected = 0
        for i, job in enumerate(simplify_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    consecutive_old += 1
                    continue
                else:
                    consecutive_old = 0
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"Failed to process SimplifyJobs job: {e}", exc_info=True)
                continue

        print(f"\n  Processing Vanshb repository...")
        consecutive_old = 0
        for i, job in enumerate(vanshb03_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    consecutive_old += 1
                    continue
                else:
                    consecutive_old = 0
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"Failed to process vanshb03 job: {e}", exc_info=True)
                continue

        self._github_mode = False

        github_valid = sum(
            1 for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]
        )
        print(f"\n  GitHub: {github_valid} valid jobs total")
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
                resolved_url = url

        if self._is_duplicate(company_from_github, title, resolved_url):
            return

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
            self.source_stats[source]["valid"] += 1
        else:
            self.source_stats[source]["rejected"] += 1

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
            for url in urls:
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

                deduped_urls.append(url)

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
            for idx, url in enumerate(deduped_urls):
                try:
                    result = self._process_single_email_url(
                        url,
                        sender,
                        html_content,
                        subject,
                        url_idx=idx + 1,
                        url_total=len(deduped_urls),
                    )
                    if result == "duplicate":
                        inline_dups += 1
                except Exception as e:
                    logging.error(f"Failed to process email URL {url}: {e}")
                    continue
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
                resolved_url = url

        if "jobright.ai" in url.lower():
            self._process_jobright_url(url, sender, email_html, subject)
            return "processed"

        if "ziprecruiter.com" in url.lower():
            self._process_ziprecruiter_url(url, sender, email_html, subject)
            return "processed"

        if self._is_duplicate_url(resolved_url):
            self.outcomes["skipped_duplicate_url"] += 1
            return "duplicate"

        result = self._process_single_job_comprehensive(
            resolved_url,
            source=sender,
            email_html=email_html,
        )
        if result:
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
        "this job is closed", "job has expired", "position has been closed",
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
            actual_url = None
            try:
                import requests as _req
                resp = _req.get(url, allow_redirects=True, timeout=10,
                               headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
                if resp and resp.status_code == 200 and resp.url != url and "ziprecruiter.com" not in resp.url:
                    actual_url = resp.url
                elif resp and resp.status_code == 200 and "ziprecruiter.com" in resp.url:
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
            # ── Full page validation on ZipRecruiter page itself ──────
            try:
                import requests as _req
                from aggregator.extractors import safe_parse_html as _sph
                from aggregator.processors import TitleProcessor as _TP, ValidationHelper as _VH
                zr_resp = _req.get(url, allow_redirects=True, timeout=10,
                                   headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
                if zr_resp and zr_resp.status_code == 200:
                    zr_soup, _ = _sph(zr_resp.text)
                    if zr_soup:
                        # Check CS/Engineering role using page description
                        zr_desc = zr_soup.get_text(separator=" ", strip=True)[:8000]
                        is_cs = _TP.is_cs_engineering_role(title, zr_desc)
                        if not is_cs:
                            self.outcomes["skipped_not_cs"] = self.outcomes.get("skipped_not_cs", 0) + 1
                            self._print_rejected(company, "Not CS/Engineering")
                            logging.info(f"REJECTED | {company} | {title} | Not a CS/Engineering role | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check undergraduate-only
                        ug_result, _ = _VH._check_undergraduate_only_requirements(zr_soup)
                        if ug_result:
                            self.outcomes["skipped_undergrad"] = self.outcomes.get("skipped_undergrad", 0) + 1
                            self._print_rejected(company, "Undergraduate students only")
                            logging.info(f"REJECTED | {company} | {title} | Undergraduate students only (MS not eligible) | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check PhD-only
                        phd_result, _ = _VH._check_phd_only_requirements(zr_soup)
                        if phd_result:
                            self.outcomes["skipped_phd"] = self.outcomes.get("skipped_phd", 0) + 1
                            self._print_rejected(company, "PhD students only")
                            logging.info(f"REJECTED | {company} | {title} | PhD students only | ZipRecruiter")
                            self.source_stats[sender]["rejected"] += 1
                            return
                        # Check page age (e.g. "Posted 29 days ago")
                        zr_age = _VH.extract_page_age(zr_soup)
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
                self._print_rejected(company_hint or "Unknown", "HTTP fetch failed")
                logging.info(f"HTTP FAIL | {company_hint} | {url[:80]}")
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
                logging.info(f"PARSE FAIL | {url[:80]}")
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

            if self._is_garbage_company(company) and company_hint:
                company = company_hint

            title = PageParser.extract_title(soup)
            if not title or title == "Unknown":
                title = title_hint if title_hint else "Unknown"
            title = TitleProcessor.clean_title_aggressive(title)

            if self._is_duplicate(company, title, final_url or url):
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
                "job_id": "N/A" if (final_url or url) and "greenhouse.io" in (final_url or url).lower() else (job_id if job_id else "N/A"),
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

    def _is_garbage_company(self, name):
        if not name:
            return True
        return name.lower().strip() in GARBAGE_COMPANY_NAMES

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

    def _print_rejected(self, company, reason):
        if getattr(self, "_github_mode", False):
            return
        display = (company or "Unknown")
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
            # ("⊘ Too old", self.outcomes["skipped_too_old"]),  # hidden — always large
            ("⊘ Wrong season", self.outcomes["skipped_wrong_season"]),
            ("⊘ Senior role", self.outcomes["skipped_senior_role"]),
            ("⊘ Non-tech", self.outcomes["skipped_non_tech"]),
            # ("⊘ Invalid title", self.outcomes["skipped_invalid_title"]),
            # ("⊘ International", self.outcomes["skipped_international"]),
            ("⊘ Blacklisted", self.outcomes["skipped_blacklisted"]),
            # ("⊘ Page restriction", self.outcomes["skipped_page_restriction"]),
            ("⊘ Low quality", self.outcomes["skipped_low_quality"]),
            ("✗ HTTP failed", self.outcomes["failed_http"]),
            ("✗ Parse failed", self.outcomes["failed_parse"]),
            ("✗ Jobright unresolved", self.outcomes["failed_jobright_resolution"]),
            ("✗ ZipRecruiter unresolved", self.outcomes.get("failed_ziprecruiter_resolution", 0)),
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
            # print("\n  TOP REJECTION REASONS:")
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
        age_str = age_str.strip()
        # Format: "5d" → 5 days
        match = re.match(r"^(\d+)d$", age_str.lower())
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
