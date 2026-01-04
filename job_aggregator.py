#!/usr/bin/env python3
# cSpell:disable
"""
Main job aggregation pipeline - Production-Optimized
Processes jobs from GitHub, email, and Handshake with comprehensive validation.
"""

import time
import datetime
import random
import re
from bs4 import BeautifulSoup

from config import SIMPLIFY_URL, VANSHB03_URL, MAX_JOB_AGE_DAYS
from extractors import (
    EmailExtractor,
    PageFetcher,
    PageParser,
    SourceParsers,
    JobrightAuthenticator,
    SimplifyGitHubScraper,
    HandshakeExtractor,
)
from processors import (
    TitleProcessor,
    LocationProcessor,
    ValidationHelper,
    QualityScorer,
)
from sheets_manager import SheetsManager


class UnifiedJobAggregator:
    """Main orchestrator for job aggregation pipeline."""

    # ✅ OPTIMIZATION: Pre-compile regex patterns
    URL_CLEAN_PATTERN = re.compile(r"\?.*$|#.*$")
    NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]")

    def __init__(self):
        print("=" * 80)

        # Initialize components
        self.sheets = SheetsManager()
        self.email_extractor = EmailExtractor()
        self.page_fetcher = PageFetcher()
        self.jobright_auth = JobrightAuthenticator()
        self.handshake_extractor = HandshakeExtractor()

        # Load existing data
        existing = self.sheets.load_existing_jobs()
        self.existing_jobs = existing["jobs"]
        self.existing_urls = existing["urls"]
        self.existing_job_ids = existing["job_ids"]
        self.processed_cache = existing["cache"]

        self.processing_lock = set()

        # Job lists
        self.valid_jobs = []
        self.discarded_jobs = []

        # Statistics
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
            "failed_http": 0,
            "url_resolved": 0,
        }

        print(f"Loaded: {len(self.existing_jobs)} existing jobs\n")

    def run(self):
        """Execute the complete job aggregation pipeline."""
        # Step 1: Authenticate if needed
        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()

        # ✅ NEW: Authenticate Handshake if needed
        if not self.handshake_extractor.cookies:
            print("\nHandshake cookies not found - attempting login...")
            self.handshake_extractor.login_interactive()

        # Step 2: Scrape GitHub repos
        print("Scraping GitHub repositories...")
        self._scrape_simplify_github()

        # Step 3: Scrape Handshake
        try:
            self._scrape_handshake()
        except Exception as e:
            print(f"Handshake error: {e}")

        # Step 4: Process email jobs
        print("\nProcessing email jobs...")
        try:
            email_data = self.email_extractor.fetch_job_emails()
            if email_data:
                self._process_email_jobs(email_data)
        except Exception as e:
            print(f"Email processing error: {e}")

        # Step 5: Ensure mutual exclusion
        self._ensure_mutual_exclusion()

        # Step 6: Write to sheets
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

    def _scrape_simplify_github(self):
        """Scrape both SimplifyJobs and vanshb03 GitHub READMEs."""
        # Scrape SimplifyJobs repo
        try:
            simplify_jobs = SimplifyGitHubScraper.scrape(
                SIMPLIFY_URL, source_name="SimplifyJobs"
            )
        except Exception as e:
            print(f"  ✗ SimplifyJobs error: {e}")
            simplify_jobs = []

        # Scrape vanshb03 repo
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

        for job in all_github_jobs:
            company = job["company"]
            title_raw = job["title"]
            location = job["location"]
            url = job["url"]
            age = job["age"]
            is_closed = job["is_closed"]
            source = job["source"]

            # Age filter
            age_days = self._parse_github_age(age)
            if age_days > MAX_JOB_AGE_DAYS:
                continue

            # Clean title
            title = TitleProcessor.clean_title_aggressive(title_raw)

            # Check duplicates
            if self._is_duplicate(company, title, url):
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            # Validate title
            is_valid, reason = TitleProcessor.is_valid_job_title(title)
            if not is_valid:
                continue

            # Check internship
            is_intern, intern_reason = TitleProcessor.is_internship_role(title)
            if not is_intern:
                self.outcomes["skipped_senior_role"] += 1
                continue

            # Check if closed
            if is_closed:
                print(f"  {company[:30]}: ✗ Closed")
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

            # Check CS role
            if not TitleProcessor.is_cs_engineering_role(title):
                print(f"  {company[:30]}: ✗ Non-CS")
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

            # URL pre-check for international
            url_intl_check = ValidationHelper.check_url_for_international(url)
            if url_intl_check:
                print(
                    f"  {company[:30]}: ✗ {self._truncate(url_intl_check.split(':')[1].strip(), 50)}"
                )
                self.outcomes["skipped_url_international"] += 1
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

            # Process the job
            self._process_github_job(company, title, location, url, source)

        github_valid = len(
            [j for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]]
        )
        print(f"\n  GitHub summary: {github_valid} valid jobs")

    def _process_github_job(self, company, title, location, url, source="GitHub"):
        """✅ OPTIMIZED: Process single GitHub job with early returns."""
        try:
            # Fetch page
            response, final_url = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                print(f"  {company[:30]}: ✗ HTTP failed")
                return

            # Check duplicates after redirect
            clean_final = self._clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                return

            self.processing_lock.add(clean_final)
            soup = BeautifulSoup(response.text, "html.parser")

            # ✅ CRITICAL: Only reject if age is KNOWN and >5 days
            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                print(f"  {company[:30]}: ✗ Posted {job_age}d ago")
                self.outcomes["skipped_too_old"] += 1
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    f"Posted {job_age} days ago (>5 days)",
                    source,
                    "Unknown",
                )
                return

            # Season check
            page_text_sample = soup.get_text()[:2000]
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, page_text_sample
            )
            if not is_valid_season:
                print(f"  {company[:30]}: ✗ {self._truncate(season_reason, 50)}")
                self.outcomes["skipped_wrong_season"] += 1
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

            # Restrictions check
            restriction = ValidationHelper.check_page_restrictions(soup)
            if restriction:
                print(f"  {company[:30]}: ✗ {self._truncate(restriction, 50)}")
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

            # Extract comprehensive data
            job_id = PageParser.extract_job_id(soup, final_url)
            location_extracted = LocationProcessor.extract_location_enhanced(
                soup, final_url
            )
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_extracted, final_url
            )
            sponsorship = ValidationHelper.check_sponsorship_status(soup)

            # International check
            if location_extracted == "Unknown":
                country_found = LocationProcessor._aggressive_country_scan(soup)
                if country_found and country_found not in [
                    "USA",
                    "United States",
                    "US",
                ]:
                    print(f"  {company[:30]}: ✗ {country_found}")
                    self._add_to_discarded(
                        company,
                        title,
                        country_found,
                        remote,
                        final_url,
                        job_id,
                        f"Location: {country_found}",
                        source,
                        sponsorship,
                    )
                    return
            else:
                intl_check = LocationProcessor.check_if_international(
                    location_extracted, soup
                )
                if intl_check:
                    print(
                        f"  {company[:30]}: ✗ {self._truncate(intl_check.split(':')[1].strip(), 50)}"
                    )
                    country = self._detect_country_simple(location_extracted)
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

            # Format location
            location_clean = LocationProcessor.format_location_clean(location_extracted)

            # Quality check
            quality = QualityScorer.calculate_score(
                {
                    "company": company,
                    "title": title,
                    "location": location_clean,
                    "job_id": job_id,
                    "sponsorship": sponsorship,
                }
            )

            if not QualityScorer.is_acceptable_quality(quality):
                print(f"  {company[:30]}: ✗ Low quality")
                return

            # Add to valid
            print(f"  {company[:30]}: ✓ Valid")
            self._add_to_valid(
                company,
                title,
                location_clean,
                remote,
                final_url,
                job_id,
                sponsorship,
                source,
            )

        except Exception as e:
            pass

    def _scrape_handshake(self):
        """Scrape Handshake jobs."""
        jobs = self.handshake_extractor.scrape_jobs()
        if not jobs:
            return

        for job in jobs:
            company = job.get("company", "Unknown")
            title_raw = job.get("title", "Unknown")
            location_raw = job.get("location", "Unknown")
            url = job["url"]
            job_id = job.get("job_id", "N/A")
            remote = job.get("remote", "Unknown")
            work_auth = job.get("work_authorization_required", "Unknown")
            spons = job.get("sponsorship", "Unknown")

            # Skip if requires US work auth or duplicate
            if work_auth == "Yes" or self._clean_url(url) in self.existing_urls:
                continue

            self.processing_lock.add(self._clean_url(url))

            title = TitleProcessor.clean_title_aggressive(title_raw)

            # Validate
            if (
                not TitleProcessor.is_valid_job_title(title)[0]
                or not TitleProcessor.is_internship_role(title)[0]
                or not TitleProcessor.is_cs_engineering_role(title)
            ):
                continue

            is_valid_co, company, _ = ValidationHelper.validate_company_field(
                company, title, url
            )
            if (
                not is_valid_co
                or self._normalize(f"{company}_{title}") in self.existing_jobs
                or LocationProcessor.check_if_international(location_raw, None)
            ):
                continue

            location_clean = LocationProcessor.format_location_clean(location_raw)
            quality = QualityScorer.calculate_score(
                {
                    "company": company,
                    "title": title,
                    "location": location_clean,
                    "job_id": job_id,
                    "sponsorship": spons,
                }
            )

            if QualityScorer.is_acceptable_quality(quality):
                self._add_to_valid(
                    company,
                    title,
                    location_clean,
                    remote,
                    url,
                    job_id,
                    spons,
                    "Handshake",
                )

    def _process_email_jobs(self, email_data_list):
        """✅ OPTIMIZED: Process email jobs with early age filtering."""
        for idx, email_data in enumerate(email_data_list, 1):
            url = email_data["url"]
            email_html = email_data["email_html"]
            sender = email_data["sender"]

            # Validate URL
            is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
            if not is_valid_url:
                self.outcomes["skipped_invalid_url"] += 1
                continue

            # URL international pre-check
            url_intl_check = ValidationHelper.check_url_for_international(url)
            if url_intl_check:
                self.outcomes["skipped_url_international"] += 1
                continue

            # Resolve Jobright URLs
            if "jobright.ai/jobs/info/" in url.lower():
                original_url = url
                url, is_company_site = self.jobright_auth.resolve_jobright_url(url)
                if url != original_url:
                    self.outcomes["url_resolved"] += 1

                    # Re-check resolved URL
                    url_intl_check = ValidationHelper.check_url_for_international(url)
                    if url_intl_check:
                        self.outcomes["skipped_url_international"] += 1
                        continue

            # Check duplicates
            clean_url = self._clean_url(url)
            if clean_url in self.processing_lock or clean_url in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                continue

            self.processing_lock.add(clean_url)

            # Process the email job
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

                normalized_key = self._normalize(
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

    def _process_single_email_job(self, url, email_html, sender, current_idx, total):
        """✅ OPTIMIZED: Process single email job with email age prioritization."""
        try:
            time.sleep(random.uniform(1.5, 2.5))

            # Try to parse from email first
            soup_email = BeautifulSoup(email_html, "html.parser")
            job_data = None

            if "ziprecruiter" in sender.lower():
                job_data = SourceParsers.parse_ziprecruiter_email(soup_email, url)
            elif "jobright" in sender.lower():
                job_data = SourceParsers.parse_jobright_email(
                    soup_email, url, self.jobright_auth
                )
            elif "adzuna" in sender.lower():
                job_data = SourceParsers.parse_adzuna_email(soup_email, url)

            # ✅ CRITICAL: Check email age FIRST (before fetching page)
            if job_data:
                email_age = job_data.get("email_age_days")

                # If email has age and it's too old, reject immediately
                if email_age is not None and email_age > MAX_JOB_AGE_DAYS:
                    self.outcomes["skipped_too_old"] += 1
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

                # Validate parsed job (email age is fresh or unknown)
                return self._validate_parsed_job(job_data, sender)

            # Fallback: Fetch the actual page
            response, final_url = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                return None

            # Check duplicates after redirect
            clean_final = self._clean_url(final_url)
            clean_original = self._clean_url(url)

            if clean_final in self.processing_lock and clean_final != clean_original:
                self.outcomes["skipped_duplicate_url"] += 1
                return None

            if clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                self.existing_urls.add(clean_original)
                return None

            self.processing_lock.add(clean_final)
            soup = BeautifulSoup(response.text, "html.parser")

            # ✅ CRITICAL: Only reject if age is KNOWN and >5 days
            job_age_days = PageParser.extract_job_age_days(soup)
            if job_age_days is not None and job_age_days > MAX_JOB_AGE_DAYS:
                self.outcomes["skipped_too_old"] += 1
                company = PageParser.extract_company(soup, final_url)
                title_ext = PageParser.extract_title(soup)

                return {
                    "decision": "discard",
                    "company": company if company else "Unknown",
                    "title": title_ext if title_ext else "Unknown",
                    "location": "Unknown",
                    "remote": "Unknown",
                    "url": final_url,
                    "job_id": "N/A",
                    "reason": f"Posted {job_age_days} days ago (>5 days)",
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            # Season check
            page_text_sample = soup.get_text()[:2000]
            title_from_page = PageParser.extract_title(soup)
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title_from_page if title_from_page != "Unknown" else "",
                page_text_sample,
            )
            if not is_valid_season:
                self.outcomes["skipped_wrong_season"] += 1
                company = PageParser.extract_company(soup, final_url)
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

            # Process scraped page
            return self._process_scraped_page(soup, final_url, url, sender)

        except Exception as e:
            return None

    def _validate_parsed_job(self, job_data, sender):
        """✅ OPTIMIZED: Validate email-parsed job with consolidated checks."""
        company = job_data["company"]
        title_raw = job_data["title"]
        location_raw = job_data.get("location", "Unknown")
        url = job_data["url"]
        remote = job_data.get("remote", "Unknown")

        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Consolidated validation checks
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": "N/A",
                "reason": intern_reason,
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            return {"decision": "skip"}

        if not TitleProcessor.is_cs_engineering_role(title):
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": "N/A",
                "reason": "Non-CS role",
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        # Company validation
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            company, title, url
        )
        if not is_valid_co:
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_raw,
                "remote": remote,
                "url": url,
                "job_id": "N/A",
                "reason": co_reason,
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        company = fixed_co

        # Duplicate check
        normalized_key = self._normalize(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            existing_job = self.processed_cache.get(normalized_key)
            new_job_data = {
                "company": company,
                "title": title,
                "job_id": "N/A",
                "url": url,
            }

            if not (
                existing_job and self._should_keep_both_jobs(new_job_data, existing_job)
            ):
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        # International check
        if location_raw and location_raw != "Unknown":
            intl_check = LocationProcessor.check_if_international(location_raw, None)
            if intl_check:
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": location_raw,
                    "remote": remote,
                    "url": url,
                    "job_id": "N/A",
                    "reason": intl_check,
                    "source": sender,
                    "sponsorship": "Unknown (Email)",
                }

        # Format location
        location_formatted = LocationProcessor.format_location_clean(location_raw)

        # Quality check
        quality_score = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": "N/A",
                "sponsorship": "Unknown (Email)",
            }
        )

        if not QualityScorer.is_acceptable_quality(quality_score):
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_formatted,
                "remote": remote,
                "url": url,
                "job_id": "N/A",
                "reason": f"Low quality: {quality_score}/7",
                "source": sender,
                "sponsorship": "Unknown (Email)",
            }

        self.processed_cache[normalized_key] = {
            "company": company,
            "title": title,
            "job_id": "N/A",
            "url": url,
        }

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location_formatted,
            "remote": remote,
            "url": url,
            "job_id": "N/A",
            "source": sender,
            "sponsorship": "Unknown (Email)",
        }

    def _process_scraped_page(self, soup, final_url, original_url, sender):
        """Process job by analyzing scraped page."""
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

                return self._validate_and_decide(
                    company,
                    title,
                    location,
                    remote,
                    sponsorship,
                    actual_url,
                    "N/A",
                    sender,
                    soup,
                )
            else:
                return None

        # Extract basic data
        company = PageParser.extract_company(soup, final_url)
        title_raw = PageParser.extract_title(soup)

        if not company or not title_raw:
            return None

        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Internship check
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
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

        # Title validation
        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            return {"decision": "skip"}

        # CS role check
        if not TitleProcessor.is_cs_engineering_role(title):
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

        # Company validation
        is_valid_company, fixed_company, company_reason = (
            ValidationHelper.validate_company_field(company, title, final_url)
        )

        if not is_valid_company:
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

        # Duplicate check
        normalized_key = self._normalize(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            existing_job = self.processed_cache.get(normalized_key)
            job_id = PageParser.extract_job_id(soup, final_url)

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
                return None

        # Season check
        page_text_sample = soup.get_text()[:2000]
        is_valid_season, season_reason = TitleProcessor.check_season_requirement(
            title, page_text_sample
        )
        if not is_valid_season:
            self.outcomes["skipped_wrong_season"] += 1
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

        # Restrictions check
        restriction = ValidationHelper.check_page_restrictions(soup)
        if restriction:
            job_id = PageParser.extract_job_id(soup, final_url)
            location = LocationProcessor.extract_location_enhanced(soup, final_url)
            sponsorship = ValidationHelper.check_sponsorship_status(soup)
            country_only = self._detect_country_simple(location)

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

        # Extract comprehensive data
        job_id = PageParser.extract_job_id(soup, final_url)
        location_extracted = LocationProcessor.extract_location_enhanced(
            soup, final_url
        )
        remote = LocationProcessor.extract_remote_status_enhanced(
            soup, location_extracted, final_url
        )
        sponsorship = ValidationHelper.check_sponsorship_status(soup)

        # International check
        if location_extracted == "Unknown":
            country_found = LocationProcessor._aggressive_country_scan(soup)
            if country_found and country_found not in ["USA", "United States", "US"]:
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country_found,
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": f"Location: {country_found}",
                    "source": sender,
                    "sponsorship": sponsorship,
                }
        else:
            location_intl_check = LocationProcessor.check_if_international(
                location_extracted, soup
            )
            if location_intl_check:
                country_only = self._detect_country_simple(location_extracted)
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country_only,
                    "remote": remote,
                    "url": final_url,
                    "job_id": job_id,
                    "reason": location_intl_check,
                    "source": sender,
                    "sponsorship": sponsorship,
                }

        # Format location
        location_formatted = LocationProcessor.format_location_clean(location_extracted)

        # Quality check
        quality_score = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        if not QualityScorer.is_acceptable_quality(quality_score):
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_formatted,
                "remote": remote,
                "url": final_url,
                "job_id": job_id,
                "reason": f"Low quality: {quality_score}/7",
                "source": sender,
                "sponsorship": sponsorship,
            }

        self.processed_cache[normalized_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": final_url,
        }

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
    ):
        """Final validation for Jobright pages."""
        title = TitleProcessor.clean_title_aggressive(title_raw)

        # Internship check
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
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

        # Title validation
        is_valid, reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid:
            return {"decision": "skip"}

        # CS check
        if not TitleProcessor.is_cs_engineering_role(title):
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

        # Company validation
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            company, title, url
        )
        if not is_valid_co:
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

        # Duplicate check
        norm_key = self._normalize(f"{company}_{title}")
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
                return None

        # Restrictions check
        if soup:
            restriction = ValidationHelper.check_page_restrictions(soup)
            if restriction:
                country = self._detect_country_simple(location)
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

        # International check
        if location == "Unknown":
            country_found = LocationProcessor._aggressive_country_scan(soup)
            if country_found and country_found not in ["USA", "United States", "US"]:
                return {
                    "decision": "discard",
                    "company": company,
                    "title": title,
                    "location": country_found,
                    "remote": remote,
                    "url": url,
                    "job_id": job_id,
                    "reason": f"Location: {country_found}",
                    "source": sender,
                    "sponsorship": sponsorship,
                }
        else:
            intl_check = LocationProcessor.check_if_international(location, soup)
            if intl_check:
                country = self._detect_country_simple(location)
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

        # Format location
        location_fmt = LocationProcessor.format_location_clean(location)

        # Quality check
        quality = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_fmt,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        if not QualityScorer.is_acceptable_quality(quality):
            return {
                "decision": "discard",
                "company": company,
                "title": title,
                "location": location_fmt,
                "remote": remote,
                "url": url,
                "job_id": job_id,
                "reason": f"Low quality: {quality}/7",
                "source": sender,
                "sponsorship": sponsorship,
            }

        self.processed_cache[norm_key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": url,
        }

        return {
            "decision": "valid",
            "company": company,
            "title": title,
            "location": location_fmt,
            "remote": remote,
            "url": url,
            "job_id": job_id,
            "source": sender,
            "sponsorship": sponsorship,
        }

    def _add_to_valid(
        self, company, title, location, remote, url, job_id, sponsorship, source
    ):
        """Add job to valid list."""
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
        """Add job to discarded list."""
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
        """✅ DRY: Update all tracking sets."""
        key = self._normalize(f"{company}_{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(self._clean_url(url))
        if job_id != "N/A":
            self.existing_job_ids.add(job_id.lower())

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        """Check if job is duplicate."""
        key = self._normalize(f"{company}_{title}")
        return (
            key in self.existing_jobs
            or self._clean_url(url) in self.existing_urls
            or (job_id != "N/A" and job_id.lower() in self.existing_job_ids)
        )

    def _should_keep_both_jobs(self, new_job, existing_job):
        """Determine if both job variants should be kept."""
        new_id = new_job.get("job_id", "N/A")
        existing_id = existing_job.get("job_id", "N/A")

        if new_id != "N/A" and existing_id != "N/A":
            return new_id.lower() != existing_id.lower()

        new_company_norm = self._normalize(new_job.get("company", ""))
        existing_company_norm = self._normalize(existing_job.get("company", ""))

        if new_company_norm != existing_company_norm:
            return True

        new_title_norm = self._normalize(new_job.get("title", ""))
        existing_title_norm = self._normalize(existing_job.get("title", ""))

        return new_title_norm != existing_title_norm

    def _ensure_mutual_exclusion(self):
        """Remove jobs in both valid and discarded lists."""
        if not self.valid_jobs or not self.discarded_jobs:
            return

        valid_keys = {
            (
                self._normalize(j["company"]),
                self._normalize(j["title"]),
                self._clean_url(j["url"]),
            )
            for j in self.valid_jobs
        }

        discarded_keys = {
            (
                self._normalize(j["company"]),
                self._normalize(j["title"]),
                self._clean_url(j["url"]),
            )
            for j in self.discarded_jobs
        }

        overlap = valid_keys & discarded_keys

        if overlap:
            overlap_simple = {(c, t) for c, t, u in overlap}
            self.valid_jobs = [
                j
                for j in self.valid_jobs
                if (self._normalize(j["company"]), self._normalize(j["title"]))
                not in overlap_simple
            ]
            self.outcomes["valid"] = len(self.valid_jobs)

    def _detect_country_simple(self, location):
        """Simple country detection for display."""
        if not location or location == "Unknown":
            return "Unknown"

        location_lower = location.lower()

        # Check common countries
        country_map = {
            "canada": "Canada",
            "uk": "UK",
            "united kingdom": "UK",
            "india": "India",
            "china": "China",
            "australia": "Australia",
            "singapore": "Singapore",
        }

        for key, value in country_map.items():
            if key in location_lower:
                return value

        from config import CANADA_PROVINCES

        if any(f", {p}" in location for p in CANADA_PROVINCES):
            return "Canada"

        return location

    def _print_summary(self):
        """Print minimal summary - only non-zero counts."""
        print("\n" + "=" * 80)
        print("SUMMARY:")
        print("=" * 80)

        # Only show non-zero counts
        summary_items = [
            ("✓ Valid", self.outcomes["valid"]),
            ("✗ Discarded", self.outcomes["discarded"]),
            ("⊘ Duplicate URL", self.outcomes["skipped_duplicate_url"]),
            ("⊘ Duplicate job", self.outcomes["skipped_duplicate_company_title"]),
            ("⊘ Wrong season", self.outcomes["skipped_wrong_season"]),
            ("⊘ Senior role", self.outcomes["skipped_senior_role"]),
            ("⊘ Invalid URL", self.outcomes["skipped_invalid_url"]),
            ("⊘ International (URL)", self.outcomes["skipped_url_international"]),
            ("⊘ Too old", self.outcomes["skipped_too_old"]),
            ("⚠ HTTP failed", self.outcomes["failed_http"]),
            ("🔄 URLs resolved", self.outcomes["url_resolved"]),
        ]

        for label, count in summary_items:
            if count > 0:
                print(f"  {label}: {count}")

        print("=" * 80)

    def _parse_github_age(self, age_str):
        """Parse age from GitHub tables."""
        if not age_str:
            return 999

        # Format 1: "1d" or "2d"
        match = re.search(r"(\d+)d", age_str.lower())
        if match:
            return int(match.group(1))

        # Format 2: "Jan 01", "Dec 12"
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
                if not month:
                    return 999

                posted_date = datetime.datetime(2026, month, day)
                today = datetime.datetime.now()

                if posted_date > today:
                    posted_date = datetime.datetime(2025, month, day)

                return (today - posted_date).days
            except:
                return 999

        return 999

    @staticmethod
    def _truncate(text, max_length=50):
        """Truncate text to max length."""
        return text if len(text) <= max_length else text[: max_length - 3] + "..."

    def _normalize(self, text):
        """✅ OPTIMIZED: Normalize with compiled pattern."""
        return self.NORMALIZE_PATTERN.sub("", text.lower()) if text else ""

    def _clean_url(self, url):
        """✅ OPTIMIZED: Clean URL with compiled pattern."""
        if not url:
            return ""

        if "jobright.ai/jobs/info/" in url.lower():
            match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
            if match:
                return match.group(1).lower()

        url = self.URL_CLEAN_PATTERN.sub("", url)
        return url.lower().rstrip("/")

    @staticmethod
    def _format_date():
        """Format current date/time."""
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    @staticmethod
    def _determine_job_type(title):
        """Determine job type."""
        return "Co-op" if "co-op" in title.lower() else "Internship"


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
