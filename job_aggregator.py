#!/usr/bin/env python3
import time
import datetime
import random
import re
import logging
from bs4 import BeautifulSoup

from config import SIMPLIFY_URL, VANSHB03_URL, MAX_JOB_AGE_DAYS
from extractors import (
    EmailExtractor,
    PageFetcher,
    PageParser,
    SourceParsers,
    JobrightAuthenticator,
    SimplifyGitHubScraper,
)

from config import (
    SIMPLIFY_URL,
    VANSHB03_URL,
    MAX_JOB_AGE_DAYS,
    CANADA_PROVINCES,  # ADD THIS
)

from processors import (
    TitleProcessor,
    LocationProcessor,
    LocationExtractor,
    JobIDExtractor,
    ValidationHelper,
    QualityScorer,
)
from sheets_manager import SheetsManager
from utils import RoleCategorizer, URLCleaner, DateParser

# Setup logging
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


class UnifiedJobAggregator:
    """Main job aggregator with optimized multi-method extraction"""

    def __init__(self):
        print("=" * 80)
        self.sheets = SheetsManager()
        self.email_extractor = EmailExtractor()
        self.page_fetcher = PageFetcher()
        self.jobright_auth = JobrightAuthenticator()

        # Load existing jobs
        existing = self.sheets.load_existing_jobs()
        self.existing_jobs = existing["jobs"]
        self.existing_urls = existing["urls"]
        self.existing_job_ids = existing["job_ids"]
        self.processed_cache = existing["cache"]

        self.processing_lock = set()
        self.valid_jobs = []
        self.discarded_jobs = []

        # Outcome tracking
        self.outcomes = {
            "valid": 0,
            "discarded": 0,
            "skipped_duplicate_url": 0,
            "skipped_duplicate_company_title": 0,
            "skipped_too_old": 0,
            "skipped_wrong_season": 0,
            "skipped_senior_role": 0,
            "skipped_invalid_url": 0,
            "skipped_url_international": 0,
            "skipped_linkedin": 0,
            "skipped_dead_url": 0,  # NEW
            "failed_http": 0,
            "url_resolved": 0,
        }

        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs from sheets")

    def run(self):
        """Main execution flow"""
        # Authenticate Jobright if needed
        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()

        # Scrape GitHub
        print("Scraping GitHub repositories...")
        self._scrape_simplify_github()

        # Process emails
        print("\nProcessing email jobs...")
        try:
            email_data = self.email_extractor.fetch_job_emails()
            if email_data:
                self._process_email_jobs(email_data)
        except Exception as e:
            print(f"Email processing error: {e}")
            logging.error(f"Email processing error: {e}")

        # Ensure no duplicates between valid and discarded
        self._ensure_mutual_exclusion()

        # Write to sheets
        rows = self.sheets.get_next_row_numbers()
        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )
        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )

        # Print summary
        self._print_summary()
        print(f"\n✓ DONE: {added_valid} valid, {added_discarded} discarded")
        print("=" * 80 + "\n")

        logging.info(f"SUMMARY: {added_valid} valid, {added_discarded} discarded")
        logging.info("=" * 100 + "\n")

    def _scrape_simplify_github(self):
        """Scrape GitHub sources"""
        # Scrape SimplifyJobs
        try:
            simplify_jobs = SimplifyGitHubScraper.scrape(
                SIMPLIFY_URL, source_name="SimplifyJobs"
            )
        except Exception as e:
            print(f"  ✗ SimplifyJobs error: {e}")
            simplify_jobs = []

        # Scrape vanshb03
        try:
            vanshb03_jobs = SimplifyGitHubScraper.scrape(
                VANSHB03_URL, source_name="vanshb03"
            )
        except Exception as e:
            print(f"  ✗ vanshb03 error: {e}")
            vanshb03_jobs = []

        all_github_jobs = simplify_jobs + vanshb03_jobs
        print(
            f"  Total: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03\n"
        )
        logging.info(
            f"GitHub: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03"
        )

        # Process each job
        for job in all_github_jobs:
            company = job["company"]
            title_raw = job["title"]
            location = job["location"]
            url = job["url"]
            age = job["age"]
            is_closed = job["is_closed"]
            source = job["source"]

            # NEW: Check age from GitHub metadata (BEFORE scraping)
            age_days = self._parse_github_age(age)
            if age_days > MAX_JOB_AGE_DAYS:
                self.outcomes["skipped_too_old"] += 1
                logging.info(
                    f"REJECTED | {company} | {title_raw} | Posted {age_days}d ago (GitHub) | {url}"
                )
                continue

            # Clean title
            title = TitleProcessor.clean_title_aggressive(title_raw)

            # Check for duplicates
            if self._is_duplicate(company, title, url):
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            # Validate title
            is_valid, reason = TitleProcessor.is_valid_job_title(title)
            if not is_valid:
                continue

            # Check if internship
            is_intern, intern_reason = TitleProcessor.is_internship_role(title)
            if not is_intern:
                self.outcomes["skipped_senior_role"] += 1
                logging.info(
                    f"REJECTED | {company} | {title} | {intern_reason} | {url}"
                )
                continue

            # Check if closed
            if is_closed:
                print(f"  {company[:30]}: ✗ Closed")
                logging.info(
                    f"REJECTED | {company} | {title} | Position closed | {url}"
                )
                self._add_to_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Position closed",
                    source,
                    "Unknown",
                )
                continue

            # Check if CS role
            if not TitleProcessor.is_cs_engineering_role(title):
                print(f"  {company[:30]}: ✗ Non-CS")
                logging.info(f"REJECTED | {company} | {title} | Non-CS role | {url}")
                self._add_to_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Non-CS role",
                    source,
                    "Unknown",
                )
                continue

            # Check URL for international
            url_intl_check = ValidationHelper.check_url_for_international(url)
            if url_intl_check:
                print(
                    f"  {company[:30]}: ✗ {self._truncate(url_intl_check.split(':')[1].strip(), 50)}"
                )
                self.outcomes["skipped_url_international"] += 1
                logging.info(
                    f"REJECTED | {company} | {title} | {url_intl_check} | {url}"
                )
                country = (
                    url_intl_check.split(":")[-1]
                    .strip()
                    .replace("(from URL)", "")
                    .strip()
                )
                self._add_to_discarded(
                    company,
                    title,
                    country,
                    "Unknown",
                    url,
                    "N/A",
                    url_intl_check,
                    source,
                    "Unknown",
                )
                continue

            # Process the job (with scraping)
            self._process_github_job(company, title, location, url, source)

        github_valid = len(
            [j for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]]
        )
        print(f"\n  GitHub summary: {github_valid} valid jobs")
        logging.info(f"GitHub summary: {github_valid} valid jobs added")

    def _process_github_job(self, company, title, location, url, source="GitHub"):
        """Process a single GitHub job with enhanced extraction"""
        try:
            # NEW: Check URL health before scraping
            is_healthy, status_code = self.page_fetcher.check_url_health(url)
            if not is_healthy:
                self.outcomes["skipped_dead_url"] += 1
                reason = (
                    f"Dead URL ({status_code})" if status_code else "Connection failed"
                )
                print(f"  {company[:30]}: ✗ {reason}")
                logging.info(f"REJECTED | {company} | {title} | {reason} | {url}")
                return

            # Fetch page
            response, final_url = self.page_fetcher.fetch_page(url)
            if not response:
                self.outcomes["failed_http"] += 1
                print(f"  {company[:30]}: ✗ HTTP failed")
                logging.info(f"REJECTED | {company} | {title} | HTTP failed | {url}")
                return

            # Check for duplicate final URL
            clean_final = URLCleaner.clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                return

            self.processing_lock.add(clean_final)

            # Parse page
            soup = BeautifulSoup(response.text, "html.parser")

            # NEW: Check age from page (more accurate than GitHub)
            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                print(f"  {company[:30]}: ✗ Posted {job_age}d ago")
                self.outcomes["skipped_too_old"] += 1
                logging.info(
                    f"REJECTED | {company} | {title} | Posted {job_age}d ago | {final_url}"
                )
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    f"Posted {job_age} days ago (>3 days)",
                    source,
                    "Unknown",
                )
                return

            # Check season
            page_text_sample = soup.get_text()[:2000]
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, page_text_sample
            )
            if not is_valid_season:
                print(f"  {company[:30]}: ✗ {self._truncate(season_reason, 50)}")
                self.outcomes["skipped_wrong_season"] += 1
                logging.info(
                    f"REJECTED | {company} | {title} | {season_reason} | {final_url}"
                )
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    season_reason,
                    source,
                    "Unknown",
                )
                return

            # Check page restrictions
            review_flags = []
            decision, restriction, page_flags = (
                ValidationHelper.check_page_restrictions(soup)
            )
            if decision == "REJECT" and restriction:
                print(f"  {company[:30]}: ✗ {self._truncate(restriction, 50)}")
                logging.info(
                    f"REJECTED | {company} | {title} | {restriction} | {final_url}"
                )
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    restriction,
                    source,
                    "Unknown",
                )
                return

            if page_flags:
                review_flags.extend(page_flags)

            # Extract company
            extracted_company = PageParser.extract_company(soup, final_url)
            if not extracted_company or not extracted_company.strip():
                extracted_company = "Unknown"

            # NEW: Extract job_id using multi-method approach
            job_id = JobIDExtractor.extract_all_methods(final_url, soup)

            # NEW: Extract location using multi-method approach
            location_extracted = LocationExtractor.extract_all_methods(
                final_url, soup, title
            )
            location_formatted = LocationProcessor.format_location_clean(
                location_extracted
            )

            # Extract remote status
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url
            )

            # Check sponsorship
            sponsorship = ValidationHelper.check_sponsorship_status(soup)

            # Check if location is international
            if location_formatted == "Unknown" or "Unknown (Canada" in str(
                location_extracted
            ):
                canada_check = LocationProcessor.check_if_international("Unknown", soup)
                if canada_check and "Canada" in str(canada_check):
                    print(
                        f"  {company[:30]}: ✗ {self._truncate(canada_check.split(':')[1].strip() if ':' in str(canada_check) else 'Canada', 50)}"
                    )
                    logging.info(
                        f"REJECTED | {company} | {title} | {canada_check} | {final_url}"
                    )
                    self._add_to_discarded(
                        company,
                        title,
                        "Canada",
                        remote,
                        final_url,
                        job_id,
                        canada_check,
                        source,
                        sponsorship,
                    )
                    return
                review_flags.append("⚠️ Location extraction failed")
            else:
                intl_check = LocationProcessor.check_if_international(
                    location_formatted, soup
                )
                if intl_check and "Location:" in str(intl_check):
                    print(
                        f"  {company[:30]}: ✗ {self._truncate(intl_check.split(':')[1].strip(), 50)}"
                    )
                    logging.info(
                        f"REJECTED | {company} | {title} | {intl_check} | {final_url}"
                    )
                    country = self._detect_country_simple(location_formatted)
                    self._add_to_discarded(
                        company,
                        title,
                        country,
                        remote,
                        final_url,
                        job_id,
                        intl_check,
                        source,
                        sponsorship,
                    )
                    return

            # Check for location quality
            if location_formatted and any(
                kw in location_formatted
                for kw in ["Employment", "Type", "Details", "Program"]
            ):
                review_flags.append("⚠️ Location needs verification")

            # Calculate quality score
            quality = QualityScorer.calculate_score(
                {
                    "company": extracted_company,
                    "title": title,
                    "location": location_formatted,
                    "job_id": job_id,
                    "sponsorship": sponsorship,
                }
            )

            if not QualityScorer.is_acceptable_quality(quality):
                print(f"  {company[:30]}: ✗ Low quality")
                logging.info(
                    f"REJECTED | {company} | {title} | Low quality: {quality}/7 | {final_url}"
                )
                return

            # Get role alert
            role_alert = RoleCategorizer.get_terminal_alert(title)

            # Print result
            if review_flags:
                flags_str = ", ".join(review_flags)
                if role_alert:
                    print(f"  {company[:30]}: ✓ Valid [{flags_str}] {role_alert}")
                else:
                    print(f"  {company[:30]}: ✓ Valid [{flags_str}]")
                logging.info(
                    f"ACCEPTED (FLAGGED) | {company} | {title} | Flags: {flags_str} | {final_url}"
                )
            else:
                if role_alert:
                    print(f"  {company[:30]}: ✓ Valid {role_alert}")
                else:
                    print(f"  {company[:30]}: ✓ Valid")
                logging.info(
                    f"ACCEPTED | {company} | {title} | Location: {location_formatted} | {final_url}"
                )

            # Add to valid jobs
            self._add_to_valid(
                extracted_company,
                title,
                location_formatted,
                remote,
                final_url,
                job_id,
                sponsorship,
                source,
            )

        except Exception as e:
            logging.error(f"ERROR processing GitHub job | {company} | {title} | {e}")
            pass

    def _process_email_jobs(self, email_data_list):
        """Process jobs from emails"""
        simplify_skipped = []

        for idx, email_data in enumerate(email_data_list, 1):
            url = email_data["url"]
            email_html = email_data["email_html"]
            sender = email_data["sender"]

            # Skip Simplify redirect URLs
            if "simplify.jobs/p/" in url.lower():
                company_name = self._extract_company_from_email_html(email_html, url)
                simplify_skipped.append(company_name)
                continue

            # Handle LinkedIn from Jobright
            if "linkedin.com/jobs" in url.lower():
                if sender.lower() == "jobright":
                    soup_email = BeautifulSoup(email_html, "html.parser")
                    job_data = SourceParsers.parse_jobright_email(
                        soup_email, url, self.jobright_auth
                    )
                    if job_data:
                        logging.info(
                            f"Processing LinkedIn from email: {job_data.get('company')} - {job_data.get('title')}"
                        )
                        result = self._validate_parsed_job(job_data, sender)
                        if result and result.get("decision") == "valid":
                            role_alert = RoleCategorizer.get_terminal_alert(
                                result["title"]
                            )
                            flags = result.get("review_flags", "")
                            if flags:
                                if role_alert:
                                    print(
                                        f"  {result['company'][:30]} ({sender}/LinkedIn): ✓ Valid [{flags}] {role_alert}"
                                    )
                                else:
                                    print(
                                        f"  {result['company'][:30]} ({sender}/LinkedIn): ✓ Valid [{flags}]"
                                    )
                            else:
                                if role_alert:
                                    print(
                                        f"  {result['company'][:30]} ({sender}/LinkedIn): ✓ Valid {role_alert}"
                                    )
                                else:
                                    print(
                                        f"  {result['company'][:30]} ({sender}/LinkedIn): ✓ Valid"
                                    )

                            self.valid_jobs.append(
                                {
                                    "company": result["company"],
                                    "job_id": result["job_id"],
                                    "title": result["title"],
                                    "job_type": self._determine_job_type(
                                        result["title"]
                                    ),
                                    "location": result["location"],
                                    "remote": result["remote"],
                                    "entry_date": self._format_date(),
                                    "url": result["url"],
                                    "source": "Jobright/LinkedIn",
                                    "sponsorship": result["sponsorship"],
                                }
                            )
                            self._update_tracking(
                                result["company"],
                                result["title"],
                                result["url"],
                                result["job_id"],
                            )
                            self.outcomes["valid"] += 1
                        elif result and result.get("decision") == "discard":
                            print(
                                f"  {result['company'][:30]} ({sender}/LinkedIn): ✗ {self._truncate(result['reason'], 50)}"
                            )
                            self.discarded_jobs.append(
                                {
                                    "company": result["company"],
                                    "title": result["title"],
                                    "location": result["location"],
                                    "job_type": self._determine_job_type(
                                        result["title"]
                                    ),
                                    "remote": result["remote"],
                                    "url": result["url"],
                                    "job_id": result["job_id"],
                                    "reason": result["reason"],
                                    "source": "Jobright/LinkedIn",
                                    "sponsorship": result["sponsorship"],
                                    "entry_date": self._format_date(),
                                }
                            )
                            self._update_tracking(
                                result["company"],
                                result["title"],
                                result["url"],
                                result["job_id"],
                            )
                            self.outcomes["discarded"] += 1
                        continue
                else:
                    self.outcomes["skipped_linkedin"] += 1
                    logging.info(
                        f"SKIPPED | LinkedIn | Unknown | No email data available | {url}"
                    )
                    continue

            # Validate URL
            is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
            if not is_valid_url:
                self.outcomes["skipped_invalid_url"] += 1
                logging.info(
                    f"REJECTED | Unknown | Unknown | Invalid URL: {url_reason} | {url}"
                )
                continue

            # Check URL for international
            url_intl_check = ValidationHelper.check_url_for_international(url)
            if url_intl_check:
                self.outcomes["skipped_url_international"] += 1
                logging.info(f"REJECTED | Unknown | Unknown | {url_intl_check} | {url}")
                continue

            # Handle Jobright URLs
            if "jobright.ai/jobs/info/" in url.lower():
                original_url = url
                url, is_company_site = self.jobright_auth.resolve_jobright_url(url)

                if url != original_url:
                    self.outcomes["url_resolved"] += 1

                    # Re-check resolved URL for international
                    url_intl_check = ValidationHelper.check_url_for_international(url)
                    if url_intl_check:
                        self.outcomes["skipped_url_international"] += 1
                        logging.info(
                            f"REJECTED | Unknown | Unknown | {url_intl_check} (resolved) | {url}"
                        )
                        continue

                    # If resolved to LinkedIn
                    if "linkedin.com/jobs" in url.lower():
                        soup_email = BeautifulSoup(email_html, "html.parser")
                        job_data = SourceParsers.parse_jobright_email(
                            soup_email, original_url, self.jobright_auth
                        )
                        if job_data:
                            result = self._validate_parsed_job(job_data, sender)
                            if result and result.get("decision") == "valid":
                                role_alert = RoleCategorizer.get_terminal_alert(
                                    result["title"]
                                )
                                flags = result.get("review_flags", "")
                                if flags:
                                    if role_alert:
                                        print(
                                            f"  {result['company'][:30]} (Jobright/LinkedIn): ✓ Valid [{flags}] {role_alert}"
                                        )
                                    else:
                                        print(
                                            f"  {result['company'][:30]} (Jobright/LinkedIn): ✓ Valid [{flags}]"
                                        )
                                else:
                                    if role_alert:
                                        print(
                                            f"  {result['company'][:30]} (Jobright/LinkedIn): ✓ Valid {role_alert}"
                                        )
                                    else:
                                        print(
                                            f"  {result['company'][:30]} (Jobright/LinkedIn): ✓ Valid"
                                        )

                                self.valid_jobs.append(
                                    {
                                        "company": result["company"],
                                        "job_id": result["job_id"],
                                        "title": result["title"],
                                        "job_type": self._determine_job_type(
                                            result["title"]
                                        ),
                                        "location": result["location"],
                                        "remote": result["remote"],
                                        "entry_date": self._format_date(),
                                        "url": url,
                                        "source": "Jobright/LinkedIn",
                                        "sponsorship": result["sponsorship"],
                                    }
                                )
                                self._update_tracking(
                                    result["company"],
                                    result["title"],
                                    url,
                                    result["job_id"],
                                )
                                self.outcomes["valid"] += 1
                            elif result and result.get("decision") == "discard":
                                print(
                                    f"  {result['company'][:30]} (Jobright/LinkedIn): ✗ {self._truncate(result['reason'], 50)}"
                                )
                                self.discarded_jobs.append(
                                    {
                                        "company": result["company"],
                                        "title": result["title"],
                                        "location": result["location"],
                                        "job_type": self._determine_job_type(
                                            result["title"]
                                        ),
                                        "remote": result["remote"],
                                        "url": url,
                                        "job_id": result["job_id"],
                                        "reason": result["reason"],
                                        "source": "Jobright/LinkedIn",
                                        "sponsorship": result["sponsorship"],
                                        "entry_date": self._format_date(),
                                    }
                                )
                                self._update_tracking(
                                    result["company"],
                                    result["title"],
                                    url,
                                    result["job_id"],
                                )
                                self.outcomes["discarded"] += 1
                        continue

            # NEW: Check URL health before scraping
            clean_url = URLCleaner.clean_url(url)
            if clean_url in self.processing_lock or clean_url in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            is_healthy, status_code = self.page_fetcher.check_url_health(url)
            if not is_healthy:
                self.outcomes["skipped_dead_url"] += 1
                reason = (
                    f"Dead URL ({status_code})" if status_code else "Connection failed"
                )
                logging.info(f"REJECTED | Unknown | Unknown | {reason} | {url}")
                continue

            self.processing_lock.add(clean_url)

            # Process single email job
            result = self._process_single_email_job(
                url, email_html, sender, idx, len(email_data_list)
            )

            if not result:
                continue

            decision = result["decision"]

            if decision == "discard":
                print(
                    f"  {result['company'][:30]} ({sender}): ✗ {self._truncate(result['reason'], 50)}"
                )
                self.discarded_jobs.append(
                    {
                        "company": result["company"],
                        "title": result["title"],
                        "location": result["location"],
                        "job_type": self._determine_job_type(result["title"]),
                        "remote": result["remote"],
                        "url": result["url"],
                        "job_id": result["job_id"],
                        "reason": result["reason"],
                        "source": result["source"],
                        "sponsorship": result["sponsorship"],
                        "entry_date": self._format_date(),
                    }
                )
                self._update_tracking(
                    result["company"], result["title"], result["url"], result["job_id"]
                )
                self.outcomes["discarded"] += 1

            elif decision == "valid":
                role_alert = RoleCategorizer.get_terminal_alert(result["title"])
                flags_str = result.get("review_flags", "")

                if flags_str:
                    if role_alert:
                        print(
                            f"  {result['company'][:30]} ({sender}): ✓ Valid [{flags_str}] {role_alert}"
                        )
                    else:
                        print(
                            f"  {result['company'][:30]} ({sender}): ✓ Valid [{flags_str}]"
                        )
                else:
                    if role_alert:
                        print(
                            f"  {result['company'][:30]} ({sender}): ✓ Valid {role_alert}"
                        )
                    else:
                        print(f"  {result['company'][:30]} ({sender}): ✓ Valid")

                self.valid_jobs.append(
                    {
                        "company": result["company"],
                        "job_id": result["job_id"],
                        "title": result["title"],
                        "job_type": self._determine_job_type(result["title"]),
                        "location": result["location"],
                        "remote": result["remote"],
                        "entry_date": self._format_date(),
                        "url": result["url"],
                        "source": result["source"],
                        "sponsorship": result["sponsorship"],
                    }
                )

                normalized_key = URLCleaner.normalize_text(
                    f"{result['company']}_{result['title']}"
                )
                self.processed_cache[normalized_key] = {
                    "company": result["company"],
                    "title": result["title"],
                    "job_id": result["job_id"],
                    "url": result["url"],
                }

                self._update_tracking(
                    result["company"], result["title"], result["url"], result["job_id"]
                )
                self.outcomes["valid"] += 1

        # Print Simplify skipped summary
        if simplify_skipped:
            print()
            lines = []
            current_line = "  ⊘ SWE List: "
            for i, company in enumerate(simplify_skipped):
                if i == 0:
                    addition = company
                else:
                    addition = ", " + company

                if len(current_line + addition) > 120:
                    lines.append(current_line)
                    current_line = "    " + company
                else:
                    current_line += addition

            if current_line.strip():
                lines.append(current_line)

            for line in lines:
                print(line)
            print(f"  ⊘ Total: {len(simplify_skipped)} Simplify redirect URLs\n")

    def _process_single_email_job(self, url, email_html, sender, current_idx, total):
        """Process a single job from email"""
        try:
            time.sleep(random.uniform(1.5, 2.5))

            soup_email = BeautifulSoup(email_html, "html.parser")
            job_data = None

            # Parse based on sender
            if "ziprecruiter" in sender.lower():
                job_data = SourceParsers.parse_ziprecruiter_email(soup_email, url)
            elif "jobright" in sender.lower():
                job_data = SourceParsers.parse_jobright_email(
                    soup_email, url, self.jobright_auth
                )
            elif "adzuna" in sender.lower():
                job_data = SourceParsers.parse_adzuna_email(soup_email, url)

            # If parsed from email
            if job_data:
                # NEW: Check email age
                email_age = job_data.get("email_age_days")
                if email_age is not None and email_age > MAX_JOB_AGE_DAYS:
                    self.outcomes["skipped_too_old"] += 1
                    logging.info(
                        f"REJECTED | {job_data['company']} | {job_data['title']} | Posted {email_age}d ago (from email) | {job_data['url']}"
                    )
                    return {
                        "decision": "discard",
                        "company": job_data["company"],
                        "title": job_data["title"],
                        "location": job_data.get("location", "Unknown"),
                        "remote": job_data.get("remote", "Unknown"),
                        "url": job_data["url"],
                        "job_id": "N/A",
                        "reason": f"Posted {email_age} days ago (from email)",
                        "source": sender,
                        "sponsorship": job_data.get("sponsorship", "Unknown"),
                    }

                # If company site, fetch and enhance data
                if sender.lower() == "jobright" and job_data.get("is_company_site"):
                    actual_url = job_data["url"]
                    email_location = job_data.get("location", "Unknown")

                    url_lower = actual_url.lower() if actual_url else ""
                    if "careers.sig.com" in url_lower or "sig.com/job" in url_lower:
                        job_data["company"] = "Susquehanna International Group"

                    logging.info(f"Fetching company page: {actual_url[:80]}")

                    # NEW: Check URL health first
                    is_healthy, status_code = self.page_fetcher.check_url_health(
                        actual_url
                    )
                    if not is_healthy:
                        # Use email data as-is
                        return self._validate_parsed_job(job_data, sender)

                    response, final_url = self.page_fetcher.fetch_page(actual_url)
                    if response:
                        soup = BeautifulSoup(response.text, "html.parser")

                        # NEW: Enhanced extraction with multi-method
                        page_location = LocationExtractor.extract_all_methods(
                            final_url, soup, title=job_data.get("title", "")
                        )
                        page_location_formatted = (
                            LocationProcessor.format_location_clean(page_location)
                        )
                        page_remote = LocationProcessor.extract_remote_status_enhanced(
                            soup, page_location_formatted, final_url
                        )
                        page_job_id = JobIDExtractor.extract_all_methods(
                            final_url, soup
                        )
                        page_company = PageParser.extract_company(soup, final_url)

                        # Merge email and page data intelligently
                        if (
                            page_location_formatted != "Unknown"
                            and email_location != "Unknown"
                        ):
                            email_has_city_state = (
                                "," in email_location
                                and len(email_location.split(",")) >= 2
                            )
                            page_has_city_state = (
                                "," in page_location_formatted
                                and len(page_location_formatted.split(",")) >= 2
                            )

                            if page_has_city_state:
                                job_data["location"] = page_location_formatted
                        elif page_location_formatted != "Unknown":
                            job_data["location"] = page_location_formatted

                        if page_remote != "Unknown":
                            job_data["remote"] = page_remote

                        if page_job_id != "N/A":
                            job_data["job_id"] = page_job_id

                        if "sig.com" not in url_lower:
                            if (
                                page_company
                                and page_company != "Unknown"
                                and page_company.strip()
                            ):
                                if not self._looks_like_title(page_company):
                                    job_data["company"] = page_company

                        # NEW: Check page age
                        page_age = PageParser.extract_job_age_days(soup)
                        if page_age is not None and page_age > MAX_JOB_AGE_DAYS:
                            self.outcomes["skipped_too_old"] += 1
                            return {
                                "decision": "discard",
                                "company": job_data["company"],
                                "title": job_data["title"],
                                "location": job_data["location"],
                                "remote": job_data["remote"],
                                "url": final_url,
                                "job_id": job_data.get("job_id", "N/A"),
                                "reason": f"Posted {page_age} days ago (>3 days)",
                                "source": sender,
                                "sponsorship": job_data.get("sponsorship", "Unknown"),
                            }

                        # Check season
                        page_text = soup.get_text()[:2000]
                        is_valid_season, season_reason = (
                            TitleProcessor.check_season_requirement(
                                job_data["title"], page_text
                            )
                        )
                        if not is_valid_season:
                            self.outcomes["skipped_wrong_season"] += 1
                            return {
                                "decision": "discard",
                                "company": job_data["company"],
                                "title": job_data["title"],
                                "location": job_data["location"],
                                "remote": job_data["remote"],
                                "url": final_url,
                                "job_id": job_data.get("job_id", "N/A"),
                                "reason": season_reason,
                                "source": sender,
                                "sponsorship": job_data.get("sponsorship", "Unknown"),
                            }

                        # Check restrictions
                        decision, restriction, review_flags = (
                            ValidationHelper.check_page_restrictions(soup)
                        )
                        if decision == "REJECT" and restriction:
                            return {
                                "decision": "discard",
                                "company": job_data["company"],
                                "title": job_data["title"],
                                "location": job_data["location"],
                                "remote": job_data["remote"],
                                "url": final_url,
                                "job_id": job_data.get("job_id", "N/A"),
                                "reason": restriction,
                                "source": sender,
                                "sponsorship": job_data.get("sponsorship", "Unknown"),
                            }

                        if review_flags:
                            job_data["review_flags"] = review_flags

                        # Check international
                        if job_data["location"] != "Unknown":
                            intl_check = LocationProcessor.check_if_international(
                                job_data["location"], soup
                            )
                            if intl_check and "Location:" in str(intl_check):
                                country = self._detect_country_simple(
                                    job_data["location"]
                                )
                                return {
                                    "decision": "discard",
                                    "company": job_data["company"],
                                    "title": job_data["title"],
                                    "location": country,
                                    "remote": job_data["remote"],
                                    "url": final_url,
                                    "job_id": job_data.get("job_id", "N/A"),
                                    "reason": intl_check,
                                    "source": sender,
                                    "sponsorship": job_data.get(
                                        "sponsorship", "Unknown"
                                    ),
                                }

                return self._validate_parsed_job(job_data, sender)

            # Not parsed from email - fetch and scrape
            response, final_url = self.page_fetcher.fetch_page(url)
            if not response:
                self.outcomes["failed_http"] += 1
                logging.info(f"REJECTED | Unknown | Unknown | HTTP failed | {url}")
                return None

            clean_final = URLCleaner.clean_url(final_url)
            clean_original = URLCleaner.clean_url(url)

            if clean_final in self.processing_lock and clean_final != clean_original:
                self.outcomes["skipped_duplicate_url"] += 1
                return None

            if clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                self.existing_urls.add(clean_original)
                return None

            self.processing_lock.add(clean_final)

            soup = BeautifulSoup(response.text, "html.parser")

            # NEW: Check age from page
            job_age_days = PageParser.extract_job_age_days(soup)
            if job_age_days is not None and job_age_days > MAX_JOB_AGE_DAYS:
                self.outcomes["skipped_too_old"] += 1
                company = PageParser.extract_company(soup, final_url)
                title_ext = PageParser.extract_title(soup)
                logging.info(
                    f"REJECTED | {company} | {title_ext} | Posted {job_age_days}d ago | {final_url}"
                )
                return {
                    "decision": "discard",
                    "company": company if company else "Unknown",
                    "title": title_ext if title_ext else "Unknown",
                    "location": "Unknown",
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": "N/A",
                    "reason": f"Posted {job_age_days} days ago (>3 days)",
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            # Check season
            page_text_sample = soup.get_text()[:2000]
            title_from_page = PageParser.extract_title(soup)
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title_from_page if title_from_page != "Unknown" else "",
                page_text_sample,
            )
            if not is_valid_season:
                self.outcomes["skipped_wrong_season"] += 1
                company = PageParser.extract_company(soup, final_url)
                logging.info(
                    f"REJECTED | {company} | {title_from_page} | {season_reason} | {final_url}"
                )
                return {
                    "decision": "discard",
                    "company": company if company else "Unknown",
                    "title": title_from_page if title_from_page else "Unknown",
                    "location": "Unknown",
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": "N/A",
                    "reason": season_reason,
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            return self._process_scraped_page(soup, final_url, url, sender)

        except Exception as e:
            logging.error(f"ERROR processing email job | {sender} | {url} | {e}")
            return None

    def _looks_like_title(self, text):
        """Check if text looks like a job title"""
        if not text:
            return False

        text_lower = text.lower()
        title_keywords = [
            "intern",
            "co-op",
            "engineer",
            "developer",
            "software",
            "full stack",
            "junior",
            "senior",
        ]
        keyword_count = sum(1 for kw in title_keywords if kw in text_lower)
        return keyword_count >= 2

    def _validate_parsed_job(self, job_data, sender):
        """Validate job data parsed from email"""
        company = job_data["company"]
        title_raw = job_data["title"]
        location_raw = job_data.get("location", "Unknown")
        url = job_data["url"]
        remote = job_data.get("remote", "Unknown")
        job_id = job_data.get("job_id", "N/A")
        review_flags = job_data.get("review_flags", [])

        if not isinstance(review_flags, list):
            review_flags = []

        # Clean title
        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Check if internship
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
            logging.info(f"REJECTED | {company} | {title} | {intern_reason} | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": intern_reason,
                "source": sender,
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }

        # Validate title
        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            logging.info(
                f"REJECTED | {company} | {title} | Invalid title: {title_reason} | {url}"
            )
            return {"decision": "skip"}

        # Check if CS role
        if not TitleProcessor.is_cs_engineering_role(title):
            logging.info(f"REJECTED | {company} | {title} | Non-CS role | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": "Non-CS role",
                "source": sender,
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }

        # Validate company
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            company, title, url
        )
        if not is_valid_co:
            logging.info(f"REJECTED | {company} | {title} | {co_reason} | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": co_reason,
                "source": sender,
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }

        company = fixed_co

        # Check for duplicates
        normalized_key = URLCleaner.normalize_text(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            existing_job = self.processed_cache.get(normalized_key)
            new_job_data = {
                "company": company,
                "title": title,
                "job_id": job_id,
                "url": url,
            }
            if not (
                existing_job and self._should_keep_both_jobs(new_job_data, existing_job)
            ):
                self.outcomes["skipped_duplicate_company_title"] += 1
                print(
                    f"  {company[:30]}: ⊘ Duplicate ({job_id if job_id != 'N/A' else 'company+title'})"
                )
                logging.info(f"REJECTED | {company} | {title} | Duplicate | {url}")
                return None

        # Check if international
        if location_raw and location_raw != "Unknown":
            intl_check = LocationProcessor.check_if_international(location_raw, None)
            if intl_check and "Location:" in str(intl_check):
                logging.info(f"REJECTED | {company} | {title} | {intl_check} | {url}")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": location_raw,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "reason": intl_check,
                    "source": sender,
                    "sponsorship": job_data.get("sponsorship", "Unknown"),
                }

        # Format location
        location_formatted = LocationProcessor.format_location_clean(location_raw)

        # Check location quality
        if location_formatted and any(
            kw in location_formatted for kw in ["Employment", "Type", "Details"]
        ):
            review_flags.append("⚠️ Location needs verification")

        if location_formatted == "Unknown":
            review_flags.append("⚠️ Location extraction failed")

        # Calculate quality score
        quality_score = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": job_id,
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }
        )

        if not QualityScorer.is_acceptable_quality(quality_score):
            logging.info(
                f"REJECTED | {company} | {title} | Low quality: {quality_score}/7 | {url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_formatted,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": f"Low quality: {quality_score}/7",
                "source": sender,
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }

        # Cache for duplicate checking
        self.processed_cache[normalized_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": url,
        }

        logging.info(
            f"ACCEPTED | {company} | {title} | Location: {location_formatted} | {url}"
        )

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location_formatted,
            "remote": remote,
            "url": url,
            "job_id": job_id,
            "source": sender,
            "sponsorship": job_data.get("sponsorship", "Unknown"),
            "review_flags": ", ".join(review_flags) if review_flags else "",
        }

    def _process_scraped_page(self, soup, final_url, original_url, sender):
        """Process a scraped job page"""
        review_flags = []

        # Handle Jobright pages
        if "jobright.ai/jobs/info/" in final_url.lower():
            jobright_data = PageParser.extract_jobright_data(
                soup, final_url, self.jobright_auth
            )
            if jobright_data:
                company = jobright_data["company"]
                title_raw = jobright_data["title"]
                location = jobright_data["location"]
                sponsorship = jobright_data["sponsorship"]
                remote = jobright_data["remote"]
                actual_url = jobright_data["url"]

                title = TitleProcessor.clean_title_aggressive(title_raw)

                is_intern, intern_reason = TitleProcessor.is_internship_role(title)
                if not is_intern:
                    self.outcomes["skipped_senior_role"] += 1
                    logging.info(
                        f"REJECTED | {company} | {title} | {intern_reason} | {actual_url}"
                    )
                    return {
                        "decision": "discard",
                        "company": company,
                        "title": title,
                        "location": location,
                        "remote": remote,
                        "url": actual_url,
                        "job_id": "N/A",
                        "reason": intern_reason,
                        "source": sender,
                        "sponsorship": sponsorship,
                    }

                location_formatted = LocationProcessor.format_location_clean(location)

                return self._validate_and_decide(
                    company,
                    title,
                    location_formatted,
                    remote,
                    sponsorship,
                    actual_url,
                    "N/A",
                    sender,
                    soup,
                    review_flags=review_flags,
                )
            else:
                return None

        # Extract from regular page
        company = PageParser.extract_company(soup, final_url)
        if not company or not company.strip():
            company = "Unknown"

        title_raw = PageParser.extract_title(soup)
        if not title_raw or title_raw == "Unknown":
            logging.info(
                f"REJECTED | {company} | Unknown | Missing title | {final_url}"
            )
            return None

        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Check if internship
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
            logging.info(
                f"REJECTED | {company} | {title} | {intern_reason} | {final_url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": final_url,
                "job_id": "N/A",
                "reason": intern_reason,
                "source": sender,
                "sponsorship": "Unknown",
            }

        # Validate title
        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            logging.info(
                f"REJECTED | {company} | {title} | Invalid title: {title_reason} | {final_url}"
            )
            return {"decision": "skip"}

        # Check if CS role
        if not TitleProcessor.is_cs_engineering_role(title):
            logging.info(f"REJECTED | {company} | {title} | Non-CS role | {final_url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": final_url,
                "job_id": "N/A",
                "reason": "Non-CS role",
                "source": sender,
                "sponsorship": "Unknown",
            }

        # Validate company
        is_valid_company, fixed_company, company_reason = (
            ValidationHelper.validate_company_field(company, title, final_url)
        )
        if not is_valid_company:
            logging.info(
                f"REJECTED | {company} | {title} | {company_reason} | {final_url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": final_url,
                "job_id": "N/A",
                "reason": company_reason,
                "source": sender,
                "sponsorship": "Unknown",
            }

        company = fixed_company
        if not company or not company.strip():
            company = "Unknown"

        # Check for duplicates
        normalized_key = URLCleaner.normalize_text(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            existing_job = self.processed_cache.get(normalized_key)
            job_id = JobIDExtractor.extract_all_methods(final_url, soup)
            new_job_data = {
                "company": company,
                "title": title,
                "job_id": job_id,
                "url": final_url,
            }
            if not (
                existing_job and self._should_keep_both_jobs(new_job_data, existing_job)
            ):
                self.outcomes["skipped_duplicate_company_title"] += 1
                print(
                    f"  {company[:30]}: ⊘ Duplicate ({job_id if job_id != 'N/A' else 'company+title'})"
                )
                logging.info(
                    f"REJECTED | {company} | {title} | Duplicate job | {final_url}"
                )
                return None

        # Check season
        page_text_sample = soup.get_text()[:2000]
        is_valid_season, season_reason = TitleProcessor.check_season_requirement(
            title, page_text_sample
        )
        if not is_valid_season:
            self.outcomes["skipped_wrong_season"] += 1
            logging.info(
                f"REJECTED | {company} | {title} | {season_reason} | {final_url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": "Unknown",
                "remote": "Unknown",
                "url": final_url,
                "job_id": "N/A",
                "reason": season_reason,
                "source": sender,
                "sponsorship": "Unknown",
            }

        # Check page restrictions
        decision, restriction, page_flags = ValidationHelper.check_page_restrictions(
            soup
        )
        if decision == "REJECT" and restriction:
            # NEW: Extract data even for rejected jobs
            job_id = JobIDExtractor.extract_all_methods(final_url, soup)
            location_extracted = LocationExtractor.extract_all_methods(
                final_url, soup, title
            )
            location_formatted = LocationProcessor.format_location_clean(
                location_extracted
            )
            sponsorship = ValidationHelper.check_sponsorship_status(soup)
            country_only = self._detect_country_simple(location_formatted)

            logging.info(
                f"REJECTED | {company} | {title} | {restriction} | {final_url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": country_only,
                "remote": "Unknown",
                "url": final_url,
                "job_id": job_id,
                "reason": restriction,
                "source": sender,
                "sponsorship": sponsorship,
            }

        if page_flags:
            review_flags.extend(page_flags)

        # NEW: Multi-method extraction
        job_id = JobIDExtractor.extract_all_methods(final_url, soup)
        location_extracted = LocationExtractor.extract_all_methods(
            final_url, soup, title
        )
        location_formatted = LocationProcessor.format_location_clean(location_extracted)
        remote = LocationProcessor.extract_remote_status_enhanced(
            soup, location_formatted, final_url
        )
        sponsorship = ValidationHelper.check_sponsorship_status(soup)

        # Check if international
        if location_formatted == "Unknown" or "Unknown (Canada" in str(
            location_extracted
        ):
            canada_check = LocationProcessor.check_if_international("Unknown", soup)
            if canada_check and "Canada" in str(canada_check):
                country = self._detect_country_simple(location_formatted)
                logging.info(
                    f"REJECTED | {company} | {title} | {canada_check} | {final_url}"
                )
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": "Canada",
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": canada_check,
                    "source": sender,
                    "sponsorship": sponsorship,
                }
            review_flags.append("⚠️ Location extraction failed")
        else:
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup
            )
            if intl_check and "Location:" in str(intl_check):
                country = self._detect_country_simple(location_formatted)
                logging.info(
                    f"REJECTED | {company} | {title} | {intl_check} | {final_url}"
                )
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country,
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": intl_check,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

        # Check location quality
        if location_formatted and any(
            kw in location_formatted for kw in ["Employment", "Type", "Details"]
        ):
            review_flags.append("⚠️ Location needs verification")

        # Calculate quality
        quality = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        if not QualityScorer.is_acceptable_quality(quality):
            logging.info(
                f"REJECTED | {company} | {title} | Low quality: {quality}/7 | {final_url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_formatted,
                "remote": remote,
                "url": final_url,
                "job_id": job_id,
                "reason": f"Low quality: {quality}/7",
                "source": sender,
                "sponsorship": sponsorship,
            }

        # Cache
        self.processed_cache[normalized_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": final_url,
        }

        logging.info(
            f"ACCEPTED | {company} | {title} | Location: {location_formatted} | {final_url}"
        )

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location_formatted,
            "remote": remote,
            "url": final_url,
            "job_id": job_id,
            "source": sender,
            "sponsorship": sponsorship,
            "review_flags": ", ".join(review_flags) if review_flags else "",
        }

    def _validate_and_decide(
        self,
        company,
        title_raw,
        location,
        remote,
        sponsorship,
        url,
        job_id,
        sender,
        soup,
        review_flags=None,
    ):
        """Validate and decide on job"""
        if review_flags is None:
            review_flags = []

        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Check if internship
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            logging.info(f"REJECTED | {company} | {title} | {intern_reason} | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": intern_reason,
                "source": sender,
                "sponsorship": sponsorship,
            }

        # Validate title
        is_valid, reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid:
            logging.info(
                f"REJECTED | {company} | {title} | Invalid title: {reason} | {url}"
            )
            return {"decision": "skip"}

        # Check if CS role
        if not TitleProcessor.is_cs_engineering_role(title):
            logging.info(f"REJECTED | {company} | {title} | Non-CS role | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": "Non-CS",
                "source": sender,
                "sponsorship": sponsorship,
            }

        # Validate company
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            company, title, url
        )
        if not is_valid_co:
            logging.info(f"REJECTED | {company} | {title} | {co_reason} | {url}")
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": co_reason,
                "source": sender,
                "sponsorship": sponsorship,
            }

        company = fixed_co
        if not company or not company.strip():
            company = "Unknown"

        # Check for duplicates
        norm_key = URLCleaner.normalize_text(f"{company}_{title}")
        if norm_key in self.existing_jobs:
            existing = self.processed_cache.get(norm_key)
            if not (
                existing
                and self._should_keep_both_jobs(
                    {"company": company, "title": title, "job_id": job_id, "url": url},
                    existing,
                )
            ):
                self.outcomes["skipped_duplicate_company_title"] += 1
                print(
                    f"  {company[:30]}: ⊘ Duplicate ({job_id if job_id != 'N/A' else 'company+title'})"
                )
                logging.info(f"REJECTED | {company} | {title} | Duplicate | {url}")
                return None

        # Check restrictions
        if soup:
            decision, restriction, page_flags = (
                ValidationHelper.check_page_restrictions(soup)
            )
            if decision == "REJECT" and restriction:
                country = self._detect_country_simple(location)
                logging.info(f"REJECTED | {company} | {title} | {restriction} | {url}")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "reason": restriction,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

            if page_flags:
                review_flags.extend(page_flags)

        # Check if international
        if location == "Unknown":
            if soup:
                canada_check = LocationProcessor.check_if_international("Unknown", soup)
                if canada_check and "Canada" in str(canada_check):
                    logging.info(
                        f"REJECTED | {company} | {title} | {canada_check} | {url}"
                    )
                    return {
                        "decision": "discard",
                        "company": company,
                        "title": title,
                        "location": "Canada",
                        "remote": remote,
                        "url": url,
                        "job_id": job_id,
                        "reason": canada_check,
                        "source": sender,
                        "sponsorship": sponsorship,
                    }
                review_flags.append("⚠️ Location extraction failed")
        else:
            intl_check = LocationProcessor.check_if_international(location, soup)
            if intl_check and "Location:" in str(intl_check):
                country = self._detect_country_simple(location)
                logging.info(f"REJECTED | {company} | {title} | {intl_check} | {url}")
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "reason": intl_check,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

        # Check location quality
        if location and any(kw in location for kw in ["Employment", "Type"]):
            review_flags.append("⚠️ Location needs verification")

        # Calculate quality
        quality = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        if not QualityScorer.is_acceptable_quality(quality):
            logging.info(
                f"REJECTED | {company} | {title} | Low quality: {quality}/7 | {url}"
            )
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": f"Low quality: {quality}/7",
                "source": sender,
                "sponsorship": sponsorship,
            }

        # Cache
        self.processed_cache[norm_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": url,
        }

        logging.info(f"ACCEPTED | {company} | {title} | Location: {location} | {url}")

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location,
            "remote": remote,
            "url": url,
            "job_id": job_id,
            "source": sender,
            "sponsorship": sponsorship,
            "review_flags": ", ".join(review_flags) if review_flags else "",
        }

    def _add_to_valid(
        self, company, title, location, remote, url, job_id, sponsorship, source
    ):
        """Add job to valid list"""
        self.valid_jobs.append(
            {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "job_type": "Co-op" if "co-op" in title.lower() else "Internship",
                "entry_date": self._format_date(),
                "source": source,
                "sponsorship": sponsorship,
            }
        )
        self._update_tracking(company, title, url, job_id)
        self.outcomes["valid"] += 1

    def _add_to_discarded(
        self, company, title, location, remote, url, job_id, reason, source, sponsorship
    ):
        """Add job to discarded list"""
        self.discarded_jobs.append(
            {
                "company": company,
                "title": title,
                "location": location,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "job_type": "Co-op" if "co-op" in title.lower() else "Internship",
                "entry_date": self._format_date(),
                "reason": reason,
                "source": source,
                "sponsorship": sponsorship,
            }
        )
        self._update_tracking(company, title, url, job_id)
        self.outcomes["discarded"] += 1

    def _update_tracking(self, company, title, url, job_id):
        """Update tracking sets"""
        key = URLCleaner.normalize_text(f"{company}_{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(URLCleaner.clean_url(url))
        if job_id != "N/A" and not job_id.startswith("HASH_"):
            self.existing_job_ids.add(job_id.lower())

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        """Check if job is duplicate"""
        key = URLCleaner.normalize_text(f"{company}_{title}")
        return (
            key in self.existing_jobs
            or URLCleaner.clean_url(url) in self.existing_urls
            or (
                job_id != "N/A"
                and not job_id.startswith("HASH_")
                and job_id.lower() in self.existing_job_ids
            )
        )

    def _should_keep_both_jobs(self, new_job, existing_job):
        """Check if both jobs should be kept (different job IDs)"""
        new_id = new_job.get("job_id", "N/A")
        existing_id = existing_job.get("job_id", "N/A")

        # Both have real IDs - compare them
        if new_id != "N/A" and existing_id != "N/A":
            if not new_id.startswith("HASH_") and not existing_id.startswith("HASH_"):
                return new_id.lower() != existing_id.lower()

        # Different companies
        new_company_norm = URLCleaner.normalize_text(new_job.get("company", ""))
        existing_company_norm = URLCleaner.normalize_text(
            existing_job.get("company", "")
        )
        if new_company_norm != existing_company_norm:
            return True

        # Different titles
        new_title_norm = URLCleaner.normalize_text(new_job.get("title", ""))
        existing_title_norm = URLCleaner.normalize_text(existing_job.get("title", ""))
        return new_title_norm != existing_title_norm

    def _ensure_mutual_exclusion(self):
        """Ensure no job appears in both valid and discarded"""
        if not self.valid_jobs or not self.discarded_jobs:
            return

        valid_keys = {
            (
                URLCleaner.normalize_text(j["company"]),
                URLCleaner.normalize_text(j["title"]),
                URLCleaner.clean_url(j["url"]),
            )
            for j in self.valid_jobs
        }

        discarded_keys = {
            (
                URLCleaner.normalize_text(j["company"]),
                URLCleaner.normalize_text(j["title"]),
                URLCleaner.clean_url(j["url"]),
            )
            for j in self.discarded_jobs
        }

        overlap = valid_keys & discarded_keys

        if overlap:
            overlap_simple = {(c, t) for c, t, u in overlap}
            self.valid_jobs = [
                j
                for j in self.valid_jobs
                if (
                    URLCleaner.normalize_text(j["company"]),
                    URLCleaner.normalize_text(j["title"]),
                )
                not in overlap_simple
            ]
            self.outcomes["valid"] = len(self.valid_jobs)
            logging.info(f"Removed {len(overlap)} overlapping jobs")

    def _detect_country_simple(self, location):
        """Detect country from location string"""
        if not location or location == "Unknown":
            return "Unknown"

        location_lower = location.lower()

        country_map = {
            "canada": "Canada",
            "uk": "UK",
            "india": "India",
            "china": "China",
        }

        for key, value in country_map.items():
            if key in location_lower:
                return value

        # Check for Canadian provinces
        if any(f", {p}" in location for p in CANADA_PROVINCES):
            return "Canada"

        return location

    def _print_summary(self):
        """Print processing summary"""
        print("\n" + "=" * 80)
        print("SUMMARY:")
        print("=" * 80)

        summary_items = [
            ("✓ Valid", self.outcomes["valid"]),
            ("✗ Discarded", self.outcomes["discarded"]),
            ("⊘ Duplicate URL", self.outcomes["skipped_duplicate_url"]),
            ("⊘ Duplicate job", self.outcomes["skipped_duplicate_company_title"]),
            ("⊘ Wrong season", self.outcomes["skipped_wrong_season"]),
            ("⊘ Senior role", self.outcomes["skipped_senior_role"]),
            ("⊘ Invalid URL", self.outcomes["skipped_invalid_url"]),
            ("⊘ International (URL)", self.outcomes["skipped_url_international"]),
            ("⊘ Dead URL", self.outcomes["skipped_dead_url"]),
            ("ℹ️  LinkedIn (from email)", self.outcomes["skipped_linkedin"]),
            ("⊘ Too old (>3 days)", self.outcomes["skipped_too_old"]),
            ("⚠ HTTP failed", self.outcomes["failed_http"]),
            ("🔄 URLs resolved", self.outcomes["url_resolved"]),
        ]

        for label, count in summary_items:
            if count > 0:
                print(f"  {label}: {count}")

        print("=" * 80)

    def _parse_github_age(self, age_str):
        """Parse GitHub age string to days"""
        if not age_str:
            return 999

        age_lower = age_str.lower().strip()

        # SimplifyJobs format: "0d", "1d", "2d", "3d"
        match = re.match(r"^(\d+)d$", age_lower)
        if match:
            return int(match.group(1))

        # Month format: "1mo"
        match = re.match(r"^(\d+)mo$", age_lower)
        if match:
            return int(match.group(1)) * 30

        # "3 days ago" or "3d ago" format
        match = re.search(r"(\d+)\s*d(?:ays?)?\s*ago", age_lower)
        if match:
            return int(match.group(1))

        # Date format: "Jan 14"
        match = re.search(r"([A-Z][a-z]{2})\s+(\d{1,2})", age_str)
        if match:
            try:
                month_str, day = match.group(1), int(match.group(2))
                months = {
                    "Jan": 1,
                    "Feb": 2,
                    "Mar": 3,
                    "Apr": 4,
                    "May": 5,
                    "Jun": 6,
                    "Jul": 7,
                    "Aug": 8,
                    "Sep": 9,
                    "Oct": 10,
                    "Nov": 11,
                    "Dec": 12,
                }
                month = months.get(month_str)
                if month:
                    import datetime

                    posted_date = datetime.datetime(2026, month, day)
                    today = datetime.datetime.now()
                    if posted_date > today:
                        posted_date = datetime.datetime(2025, month, day)
                    return (today - posted_date).dayss
            except:
                pass

        return 999

    @staticmethod
    def _truncate(text, max_length=50):
        """Truncate text to max length"""
        return text if len(text) <= max_length else text[: max_length - 3] + "..."

    @staticmethod
    def _extract_company_from_email_html(email_html, url):
        """Extract company from Simplify email HTML"""
        try:
            soup = BeautifulSoup(email_html, "html.parser")
            link = soup.find("a", href=lambda h: h and url in h)
            if link:
                parent = link.find_parent(["p", "div", "td"])
                if parent:
                    strong_tag = parent.find("strong")
                    if strong_tag:
                        company = strong_tag.get_text().strip()
                        company = company.rstrip(":").strip()
                        if company and len(company) < 100:
                            return company

            # Extract from URL slug
            parts = url.split("/")
            if len(parts) >= 5:
                slug = parts[4].split("?")[0]
                words = slug.split("-")
                if words and not any(char.isdigit() for char in words[0]):
                    return words[0].title()

            return "Simplify Jobs"
        except:
            return "Simplify Jobs"

    @staticmethod
    def _format_date():
        """Format current date"""
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    @staticmethod
    def _determine_job_type(title):
        """Determine if job is internship or co-op"""
        return "Co-op" if "co-op" in title.lower() else "Internship"


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
