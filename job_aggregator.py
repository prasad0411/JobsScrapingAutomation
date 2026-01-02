#!/usr/bin/env python3
# cSpell:disable
"""
Main job aggregation pipeline - FINAL VERSION
Orchestrates extraction, processing, validation, and sheet updates.
All validation checks properly integrated with comprehensive logging.
"""

import time
import datetime
import random
import re
from bs4 import BeautifulSoup

from config import SIMPLIFY_URL, MAX_JOB_AGE_DAYS
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

    def __init__(self):
        print("=" * 80)
        print("INITIALIZING JOB AGGREGATOR - FINAL VERSION")
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
            "skipped_non_job": 0,
            "skipped_marketing": 0,
            "skipped_too_old": 0,
            "skipped_wrong_season": 0,
            "skipped_senior_role": 0,
            "skipped_invalid_url": 0,
            "failed_http": 0,
            "failed_extraction": 0,
            "low_quality": 0,
            "kept_both_variants": 0,
            "method_email_parsed": 0,
            "method_handshake": 0,
            "url_resolved": 0,
        }

        print(f"Loaded {len(self.existing_jobs)} existing jobs\n")

    def run(self):
        """Execute the complete job aggregation pipeline."""
        start_time = time.time()
        print("\nStarting job aggregation\n")

        # Step 1: Authenticate Jobright if needed
        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()

        # Step 2: Scrape SimplifyJobs GitHub
        self._scrape_simplify_github()
        try:
            self._scrape_handshake()
        except Exception as e:
            print(f"Handshake error: {e}")

        # Step 3: Process email jobs
        try:
            email_data = self.email_extractor.fetch_job_emails()
            if email_data:
                self._process_email_jobs(email_data)
        except Exception as e:
            print(f"Email processing error: {e}")

        # Step 4: Ensure mutual exclusion
        print("\nRunning mutual exclusion check")
        self._ensure_mutual_exclusion()

        # Step 5: Write to sheets
        print(
            f"\nFinal: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} GitHub, "
            f"{len([j for j in self.valid_jobs if j['source'] != 'GitHub'])} Email\n"
        )

        rows = self.sheets.get_next_row_numbers()
        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )
        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )

        # Print summary
        elapsed = time.time() - start_time
        self._print_summary(elapsed)
        print(f"\nDONE: {added_valid} valid, {added_discarded} discarded")
        print("=" * 80 + "\n")

    def _scrape_simplify_github(self):
        """Scrape SimplifyJobs GitHub README."""
        print("Scraping SimplifyJobs GitHub")

        github_jobs = SimplifyGitHubScraper.scrape()

        for job in github_jobs:
            company = job["company"]
            title_raw = job["title"]
            location = job["location"]
            url = job["url"]
            age = job["age"]
            is_closed = job["is_closed"]

            # Age filter
            if self._parse_age(age) > 1:
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
                self.outcomes["skipped_non_job"] += 1
                continue

            # CHECK 1: Internship role (not senior)
            is_intern, intern_reason = TitleProcessor.is_internship_role(title)
            if not is_intern:
                self.outcomes["skipped_senior_role"] += 1
                continue

            # Check if closed
            if is_closed:
                self._add_to_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Position closed",
                    "GitHub",
                    "Unknown",
                )
                continue

            # Check if CS role
            if not TitleProcessor.is_cs_engineering_role(title):
                self._add_to_discarded(
                    company,
                    title,
                    location,
                    "Unknown",
                    url,
                    "N/A",
                    "Non-CS role",
                    "GitHub",
                    "Unknown",
                )
                continue

            # Process the job by fetching the actual page
            self._process_github_job(company, title, location, url)

        print(
            f"GitHub: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} valid\n"
        )

    def _process_github_job(self, company, title, location, url):
        """Process a single GitHub job by fetching and analyzing the page."""
        try:
            print(f"\n[GITHUB] Processing: {company} - {title[:60]}")
            print(f"         URL: {url[:100]}")

            # URL pre-check for Canada
            url_canada = ValidationHelper.check_url_for_canada(url)
            if url_canada:
                self._add_to_discarded(
                    company,
                    title,
                    "Canada",
                    "Unknown",
                    url,
                    "N/A",
                    url_canada,
                    "GitHub",
                    "Unknown",
                )
                return

            # Fetch page
            response, final_url = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                print(f"  ✗ HTTP FAILED")
                return

            # Check for duplicates after redirect
            clean_final = self._clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                self.existing_urls.add(self._clean_url(url))
                print(f"  ✗ DUPLICATE (after redirect)")
                return

            self.processing_lock.add(clean_final)

            soup = BeautifulSoup(response.text, "html.parser")

            # CHECK 2: Season requirement
            page_text_sample = soup.get_text()[:2000]
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, page_text_sample
            )
            if not is_valid_season:
                print(f"  ✗ WRONG SEASON: {season_reason}")
                self.outcomes["skipped_wrong_season"] += 1
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    season_reason,
                    "GitHub",
                    "Unknown",
                )
                return

            # CHECK 3: Job age
            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                print(f"  ✗ TOO OLD: {job_age} days")
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    f"Posted {job_age} days ago (>3 days)",
                    "GitHub",
                    "Unknown",
                )
                return

            # CHECK 4: Restrictions
            restriction = ValidationHelper.check_page_restrictions(soup)
            if restriction:
                print(f"  ✗ RESTRICTED: {restriction}")
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    restriction,
                    "GitHub",
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

            print(f"  → Location extracted: '{location_extracted}'")
            print(f"  → Remote: {remote}")
            print(f"  → Job ID: {job_id}")

            # CHECK 5: International location
            intl_check = LocationProcessor.check_if_international(
                location_extracted, soup
            )
            if intl_check:
                print(f"  ✗ INTERNATIONAL: {intl_check}")
                country = self._detect_country_simple(location_extracted)
                self._add_to_discarded(
                    company,
                    title,
                    country,
                    remote,
                    final_url,
                    job_id,
                    intl_check,
                    "GitHub",
                    sponsorship,
                )
                return

            # Format location
            location_clean = LocationProcessor.format_location_clean(location_extracted)
            print(f"  → Location formatted: '{location_clean}'")

            # CHECK 6: Quality
            quality = QualityScorer.calculate_score(
                {
                    "company": company,
                    "title": title,
                    "location": location_clean,
                    "job_id": job_id,
                    "sponsorship": sponsorship,
                }
            )
            print(f"  → Quality score: {quality}/7")

            if not QualityScorer.is_acceptable_quality(quality):
                print(f"  ✗ LOW QUALITY")
                self.outcomes["low_quality"] += 1
                return

            # Add to valid
            print(f"  ✓ VALID - Adding to sheet")
            self._add_to_valid(
                company,
                title,
                location_clean,
                remote,
                final_url,
                job_id,
                sponsorship,
                "GitHub",
            )

        except Exception as e:
            self.outcomes["failed_extraction"] += 1
            print(f"  ✗ EXCEPTION: {str(e)[:150]}")

    def _scrape_handshake(self):
        jobs = self.handshake_extractor.scrape_jobs()
        if not jobs:
            return
        for job in jobs:
            company, title_raw, location_raw, url = (
                job.get("company", "Unknown"),
                job.get("title", "Unknown"),
                job.get("location", "Unknown"),
                job["url"],
            )
            job_id, remote, work_auth, spons = (
                job.get("job_id", "N/A"),
                job.get("remote", "Unknown"),
                job.get("work_authorization_required", "Unknown"),
                job.get("sponsorship", "Unknown"),
            )
            if work_auth == "Yes" or self._clean_url(url) in self.existing_urls:
                continue
            self.processing_lock.add(self._clean_url(url))
            from processors import (
                TitleProcessor,
                ValidationHelper,
                LocationProcessor,
                QualityScorer,
            )

            title = TitleProcessor.clean_title_aggressive(title_raw)
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
            if QualityScorer.is_acceptable_quality(
                QualityScorer.calculate_score(
                    {
                        "company": company,
                        "title": title,
                        "location": location_clean,
                        "job_id": job_id,
                        "sponsorship": spons,
                    }
                )
            ):
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
                self.outcomes["method_handshake"] += 1

    def _process_email_jobs(self, email_data_list):
        """Process jobs extracted from emails."""
        print("=" * 80)
        print(f"PROCESSING {len(email_data_list)} EMAIL URLS")
        print("=" * 80 + "\n")

        for idx, email_data in enumerate(email_data_list, 1):
            url = email_data["url"]
            email_html = email_data["email_html"]
            sender = email_data["sender"]

            print(f"\n[{idx}/{len(email_data_list)}] {url[:100]}")
            print(f"  Sender: {sender}")

            # CHECK 0: URL Validation (CRITICAL - was missing!)
            is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
            if not is_valid_url:
                print(f"  ✗ INVALID URL: {url_reason}")
                self.outcomes["skipped_invalid_url"] += 1
                continue

            # Check URL for Canada
            url_canada = ValidationHelper.check_url_for_canada(url)
            if url_canada:
                print(f"  ✗ {url_canada}")
                self.outcomes["skipped_invalid_url"] += 1
                continue

            # Resolve Jobright URLs
            if "jobright.ai/jobs/info/" in url.lower():
                print(f"  Resolving Jobright URL...")
                original_url = url
                url, is_company_site = self.jobright_auth.resolve_jobright_url(url)
                if url != original_url:
                    print(f"  → Resolved to: {url[:100]}")
                    self.outcomes["url_resolved"] += 1

            # Check duplicates
            clean_url = self._clean_url(url)
            if clean_url in self.processing_lock or clean_url in self.existing_urls:
                print(f"  ✗ DUPLICATE URL")
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

            if decision == "skip":
                reason_type = result.get("reason_type", "non_job")
                self.outcomes[f"skipped_{reason_type}"] += 1
                print(f"  ⊘ SKIP: {result.get('reason')}")

            elif decision == "discard":
                print(f"  ✗ DISCARD: {result['reason']}")
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

                normalized_key = self._normalize(
                    f"{result['company']}_{result['title']}"
                )
                self.existing_jobs.add(normalized_key)
                self.existing_urls.add(self._clean_url(result["url"]))

                if result["job_id"] != "N/A":
                    self.existing_job_ids.add(result["job_id"].lower())

                self.outcomes["discarded"] += 1

            elif decision == "valid":
                print(f"  ✓ VALID - Adding to sheet")
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
                self.existing_jobs.add(normalized_key)
                self.processed_cache[normalized_key] = {
                    "company": result["company"],
                    "title": result["title"],
                    "job_id": result["job_id"],
                    "url": result["url"],
                }
                self.existing_urls.add(self._clean_url(result["url"]))

                if result["job_id"] != "N/A":
                    self.existing_job_ids.add(result["job_id"].lower())

                self.outcomes["valid"] += 1

        print("\n" + "=" * 80)
        print("EMAIL PROCESSING COMPLETE")
        print("=" * 80 + "\n")

    def _process_single_email_job(self, url, email_html, sender, current_idx, total):
        """Process a single job from email with comprehensive validation."""
        try:
            # URL pre-check for Canada
            url_canada = ValidationHelper.check_url_for_canada(url)
            if url_canada:
                return {
                    "decision": "discard",
                    "company": "Unknown",
                    "title": "Unknown",
                    "location": "Canada",
                    "remote": "Unknown",
                    "url": url,
                    "job_id": "N/A",
                    "reason": url_canada,
                    "source": sender,
                    "sponsorship": "Unknown",
                }

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

            # If successfully parsed from email
            if job_data:
                print(f"  → Parsed from email")
                self.outcomes["method_email_parsed"] += 1
                return self._validate_parsed_job(job_data, sender)

            # Fallback: Fetch the actual page
            print(f"  → Fetching page...")
            response, final_url = self.page_fetcher.fetch_page(url)

            if not response:
                self.outcomes["failed_http"] += 1
                print(f"  ✗ HTTP FAILED")
                return None

            # Check final URL for duplicates
            clean_final = self._clean_url(final_url)
            clean_original = self._clean_url(url)

            if clean_final in self.processing_lock and clean_final != clean_original:
                self.outcomes["skipped_duplicate_url"] += 1
                print(f"  ✗ DUPLICATE (final URL)")
                return None

            if clean_final in self.existing_urls:
                self.outcomes["skipped_duplicate_url"] += 1
                print(f"  ✗ DUPLICATE (existing)")
                self.existing_urls.add(clean_original)
                return None

            self.processing_lock.add(clean_final)
            print(f"  → Final URL: {final_url[:100]}")

            soup = BeautifulSoup(response.text, "html.parser")

            # CHECK 1: Season
            page_text_sample = soup.get_text()[:2000]
            title_from_page = PageParser.extract_title(soup)
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title_from_page if title_from_page != "Unknown" else "",
                page_text_sample,
            )
            if not is_valid_season:
                print(f"  ✗ WRONG SEASON: {season_reason}")
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

            # CHECK 2: Job age
            job_age_days = PageParser.extract_job_age_days(soup)
            if job_age_days is not None and job_age_days > MAX_JOB_AGE_DAYS:
                print(f"  ✗ TOO OLD: {job_age_days} days")
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
                    "reason": f"Posted {job_age_days} days ago (>3 days)",
                    "source": sender,
                    "sponsorship": "Unknown",
                }

            # Process scraped page
            return self._process_scraped_page(soup, final_url, url, sender)

        except Exception as e:
            self.outcomes["failed_extraction"] += 1
            print(f"  ✗ EXCEPTION: {str(e)[:150]}")
            return None

    def _validate_parsed_job(self, job_data, sender):
        """Validate job data parsed from email - WITH ALL CHECKS."""
        company = job_data["company"]
        title_raw = job_data["title"]
        location_raw = job_data.get("location", "Unknown")
        url = job_data["url"]
        remote = job_data.get("remote", "Unknown")

        print(f"  Email-parsed: Company={company}, Title={title_raw[:50]}")
        print(f"  Location (raw): '{location_raw}'")

        # Clean title
        title = TitleProcessor.clean_title_aggressive(title_raw)

        # CHECK 1: Is internship? (CRITICAL - WAS MISSING!)
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            print(f"  ✗ NOT INTERNSHIP: {intern_reason}")
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

        # CHECK 2: Valid title
        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            print(f"  ✗ INVALID TITLE: {title_reason}")
            return {
                "decision": "skip",
                "reason": title_reason,
                "reason_type": (
                    "marketing" if "Marketing" in title_reason else "non_job"
                ),
            }

        # CHECK 3: CS role
        if not TitleProcessor.is_cs_engineering_role(title):
            print(f"  ✗ NON-CS ROLE")
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

        # CHECK 4: Company validation
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            company, title, url
        )

        if not is_valid_co:
            print(f"  ✗ BAD COMPANY: {co_reason}")
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

        # CHECK 5: Duplicates
        normalized_key = self._normalize(f"{company}_{title}")

        if normalized_key in self.existing_jobs:
            existing_job = self.processed_cache.get(normalized_key)
            new_job_data = {
                "company": company,
                "title": title,
                "job_id": "N/A",
                "url": url,
            }

            if existing_job and self._should_keep_both_jobs(new_job_data, existing_job):
                self.outcomes["kept_both_variants"] += 1
            else:
                print(f"  ✗ DUPLICATE (company+title)")
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        # CHECK 6: International (CRITICAL - WAS MISSING FOR EMAILS!)
        if location_raw and location_raw != "Unknown":
            intl_check = LocationProcessor.check_if_international(location_raw, None)
            if intl_check:
                print(f"  ✗ INTERNATIONAL: {intl_check}")
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

        # CHECK 7: Quality
        quality_score = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": "N/A",
                "sponsorship": "Unknown (Email)",
            }
        )

        print(f"  → Quality: {quality_score}/7")

        if not QualityScorer.is_acceptable_quality(quality_score):
            print(f"  ✗ LOW QUALITY")
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

        print(f"  ✓ VALID (email-parsed)")

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
        """Process job by analyzing scraped page - WITH ALL CHECKS."""
        print(f"  → Extracting from page...")

        # Handle Jobright pages specially
        if "jobright.ai/jobs/info/" in final_url.lower():
            print(f"  Platform: JOBRIGHT")

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

                print(f"  Jobright data: {company} - {title_raw[:50]}")
                print(f"  Location: '{location}'")

                title = TitleProcessor.clean_title_aggressive(title_raw)

                # CHECK: Is internship?
                is_intern, intern_reason = TitleProcessor.is_internship_role(title)
                if not is_intern:
                    print(f"  ✗ NOT INTERNSHIP: {intern_reason}")
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
                print(f"  ✗ Jobright extraction failed")
                return None

        # Extract basic data
        company = PageParser.extract_company(soup, final_url)
        title_raw = PageParser.extract_title(soup)

        if not company or not title_raw:
            self.outcomes["failed_extraction"] += 1
            print(f"  ✗ No company or title found")
            return None

        title = TitleProcessor.clean_title_aggressive(title_raw)

        print(f"  Extracted: {company} - {title[:60]}")

        # CHECK 1: Is internship? (CRITICAL!)
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            print(f"  ✗ NOT INTERNSHIP: {intern_reason}")
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

        # CHECK 2: Valid title
        is_valid_title, title_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid_title:
            print(f"  ✗ INVALID TITLE: {title_reason}")
            return {
                "decision": "skip",
                "reason": title_reason,
                "reason_type": (
                    "marketing" if "Marketing" in title_reason else "non_job"
                ),
            }

        # CHECK 3: CS role
        if not TitleProcessor.is_cs_engineering_role(title):
            print(f"  ✗ NON-CS")
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

        # CHECK 4: Company validation
        is_valid_company, fixed_company, company_reason = (
            ValidationHelper.validate_company_field(company, title, final_url)
        )

        if not is_valid_company:
            print(f"  ✗ BAD COMPANY: {company_reason}")
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

        # CHECK 5: Duplicates
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

            if existing_job and self._should_keep_both_jobs(new_job_data, existing_job):
                self.outcomes["kept_both_variants"] += 1
            else:
                print(f"  ✗ DUPLICATE (company+title)")
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        # CHECK 6: Season requirement
        page_text_sample = soup.get_text()[:2000]
        is_valid_season, season_reason = TitleProcessor.check_season_requirement(
            title, page_text_sample
        )
        if not is_valid_season:
            print(f"  ✗ WRONG SEASON: {season_reason}")
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

        # CHECK 7: Restrictions
        restriction = ValidationHelper.check_page_restrictions(soup)

        if restriction:
            print(f"  ✗ RESTRICTED: {restriction}")
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

        print(f"  → Location: '{location_extracted}'")
        print(f"  → Remote: {remote}")

        # CHECK 8: International (CRITICAL!)
        location_intl_check = LocationProcessor.check_if_international(
            location_extracted, soup
        )

        if location_intl_check:
            print(f"  ✗ INTERNATIONAL: {location_intl_check}")
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
        print(f"  → Location formatted: '{location_formatted}'")

        # CHECK 9: Quality
        quality_score = QualityScorer.calculate_score(
            {
                "company": company,
                "title": title,
                "location": location_formatted,
                "job_id": job_id,
                "sponsorship": sponsorship,
            }
        )

        print(f"  → Quality: {quality_score}/7")

        if not QualityScorer.is_acceptable_quality(quality_score):
            print(f"  ✗ LOW QUALITY")
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

        print(f"  ✓ ALL CHECKS PASSED")

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
        """Final validation for special cases like Jobright."""
        title = TitleProcessor.clean_title_aggressive(title_raw)

        print(f"  Final validation: {company} - {title[:50]}")
        print(f"  Location: '{location}'")

        # CHECK 1: Is internship?
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            print(f"  ✗ NOT INTERNSHIP: {intern_reason}")
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

        # CHECK 2: Valid title
        is_valid, reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid:
            return {"decision": "skip", "reason": reason, "reason_type": "non_job"}

        # CHECK 3: CS role
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

        # CHECK 4: Company
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

        # CHECK 5: Duplicates
        norm_key = self._normalize(f"{company}_{title}")
        if norm_key in self.existing_jobs:
            existing = self.processed_cache.get(norm_key)
            if existing and self._should_keep_both_jobs(
                {"company": company, "title": title, "job_id": job_id, "url": url},
                existing,
            ):
                self.outcomes["kept_both_variants"] += 1
            else:
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None

        # CHECK 6: Restrictions
        if soup:
            restriction = ValidationHelper.check_page_restrictions(soup)
            if restriction:
                print(f"  ✗ RESTRICTED: {restriction}")
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

        # CHECK 7: International (CRITICAL!)
        intl_check = LocationProcessor.check_if_international(location, soup)
        if intl_check:
            print(f"  ✗ INTERNATIONAL: {intl_check}")
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

        # CHECK 8: Quality
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
            print(f"  ✗ LOW QUALITY: {quality}/7")
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

        print(f"  ✓ ALL CHECKS PASSED")

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

        # Update tracking
        key = self._normalize(f"{company}_{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(self._clean_url(url))
        self.processed_cache[key] = {
            "company": company,
            "title": title,
            "job_id": job_id,
            "url": url,
        }

        if job_id != "N/A":
            self.existing_job_ids.add(job_id.lower())

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

        key = self._normalize(f"{company}_{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(self._clean_url(url))

        if job_id != "N/A":
            self.existing_job_ids.add(job_id.lower())

        self.outcomes["discarded"] += 1

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        """Check if job is duplicate."""
        key = self._normalize(f"{company}_{title}")
        if key in self.existing_jobs:
            return True
        if self._clean_url(url) in self.existing_urls:
            return True
        if job_id != "N/A" and job_id.lower() in self.existing_job_ids:
            return True
        return False

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

        if new_title_norm != existing_title_norm:
            return True

        return False

    def _ensure_mutual_exclusion(self):
        """Remove jobs that appear in both valid and discarded lists."""
        if not self.valid_jobs or not self.discarded_jobs:
            print("Mutual exclusion: No overlap possible")
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
            print(f"⚠ MUTUAL EXCLUSION: {len(overlap)} jobs in BOTH lists")

            overlap_simple = {(c, t) for c, t, u in overlap}

            removed = [
                j
                for j in self.valid_jobs
                if (self._normalize(j["company"]), self._normalize(j["title"]))
                in overlap_simple
            ]

            for job in removed:
                print(f"  Removed: {job['company']} - {job['title'][:60]}")

            self.valid_jobs = [
                j
                for j in self.valid_jobs
                if (self._normalize(j["company"]), self._normalize(j["title"]))
                not in overlap_simple
            ]

            self.outcomes["valid"] = len(self.valid_jobs)
            print(f"  After exclusion: {len(self.valid_jobs)} valid remain")
        else:
            print("Mutual exclusion: No overlap - clean!")

    def _detect_country_simple(self, location):
        """Simple country detection for display purposes."""
        if not location or location == "Unknown":
            return "Unknown"

        location_lower = location.lower()

        from config import CANADA_PROVINCES

        if "canada" in location_lower or any(
            f", {p}" in location for p in CANADA_PROVINCES
        ):
            return "Canada"
        if "uk" in location_lower or "united kingdom" in location_lower:
            return "UK"
        if "india" in location_lower:
            return "India"
        if "china" in location_lower:
            return "China"
        if "australia" in location_lower:
            return "Australia"
        if "singapore" in location_lower:
            return "Singapore"

        return location

    def _print_summary(self, elapsed):
        """Print comprehensive processing summary."""
        print("\n" + "=" * 80)
        print("PROCESSING SUMMARY:")
        print("=" * 80)
        print(f"  ✓ Valid jobs: {self.outcomes['valid']}")
        print(f"  ✗ Discarded: {self.outcomes['discarded']}")
        print(f"  ⊘ Skipped (duplicate URL): {self.outcomes['skipped_duplicate_url']}")
        print(
            f"  ⊘ Skipped (duplicate company+title): {self.outcomes['skipped_duplicate_company_title']}"
        )
        print(f"  ⊘ Skipped (non-job): {self.outcomes['skipped_non_job']}")
        print(f"  ⊘ Skipped (wrong season): {self.outcomes['skipped_wrong_season']}")
        print(f"  ⊘ Skipped (senior role): {self.outcomes['skipped_senior_role']}")
        print(f"  ⊘ Skipped (invalid URL): {self.outcomes['skipped_invalid_url']}")
        print(f"  ⊘ Skipped (too old): {self.outcomes['skipped_too_old']}")
        print(f"  ⚠ Failed (HTTP): {self.outcomes['failed_http']}")
        print(f"  ⚠ Failed (extraction): {self.outcomes['failed_extraction']}")
        print(f"  ⚠ Low quality: {self.outcomes['low_quality']}")
        print(f"  ✓ Kept both variants: {self.outcomes['kept_both_variants']}")
        print(f"  🔄 URLs resolved: {self.outcomes['url_resolved']}")
        print("")
        print("EXTRACTION METHODS USED:")
        print(f"  Standard requests: {self.page_fetcher.outcomes['method_standard']}")
        print(f"  Rotating UA: {self.page_fetcher.outcomes['method_rotating_agent']}")
        print(f"  Selenium: {self.page_fetcher.outcomes['method_selenium']}")
        print(f"  Email parsing: {self.outcomes['method_email_parsed']}")
        print(f"  Handshake: {self.outcomes['method_handshake']}")
        print("=" * 80)
        print(f"\nExecution time: {elapsed/60:.1f} minutes")

    @staticmethod
    def _normalize(text):
        """Normalize text for deduplication."""
        if not text:
            return ""
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @staticmethod
    def _clean_url(url):
        """Clean URL for comparison."""
        if not url:
            return ""

        # Handle Jobright URLs specially
        if "jobright.ai/jobs/info/" in url.lower():
            match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
            if match:
                return match.group(1).lower()

        url = re.sub(r"\?.*$", "", url)
        url = re.sub(r"#.*$", "", url)
        return url.lower().rstrip("/")

    @staticmethod
    def _remove_emojis(text):
        """Remove emojis from text."""
        if not text:
            return text
        emoji_pattern = re.compile(
            "["
            "\U0001f600-\U0001f64f"
            "\U0001f300-\U0001f5ff"
            "\U0001f680-\U0001f6ff"
            "\U0001f1e0-\U0001f1ff"
            "\U00002500-\U00002bef"
            "]+",
            flags=re.UNICODE,
        )
        text = emoji_pattern.sub("", text)
        text = re.sub(r"[↳🇺🇸🛂\*🔒❌✅]+", "", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _parse_age(age_str):
        """Parse age string like '1d' to days."""
        match = re.search(r"(\d+)d", age_str.lower()) if age_str else None
        return int(match.group(1)) if match else 999

    @staticmethod
    def _format_date():
        """Format current date/time."""
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    @staticmethod
    def _determine_job_type(title):
        """Determine if job is Co-op or Internship."""
        return "Co-op" if "co-op" in title.lower() else "Internship"


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
