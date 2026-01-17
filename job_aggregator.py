#!/usr/bin/env python3

import time
import datetime
import random
import re
import logging
from collections import defaultdict

from config import (
    SIMPLIFY_URL,
    VANSHB03_URL,
    MAX_JOB_AGE_DAYS,
)

from processors import (
    TitleProcessor,
    LocationProcessor,
    LocationExtractor,
    JobIDExtractor,
    ValidationHelper,
    QualityScorer,
    CompanyExtractor,
)

from extractors import (
    EmailExtractor,
    PageFetcher,
    PageParser,
    SourceParsers,
    JobrightAuthenticator,
    SimplifyGitHubScraper,
    SimplifyRedirectResolver,
    safe_parse_html,
)

from sheets_manager import SheetsManager
from utils import RoleCategorizer, URLCleaner, DateParser, PlatformDetector

# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    filename="skipped_jobs.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open("skipped_jobs.log", "w") as f:
    f.write("=" * 100 + "\n")
    f.write(
        f"JOB PROCESSING LOG - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    f.write("=" * 100 + "\n\n")


# ============================================================================
# Main Job Aggregator
# ============================================================================


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

        # Single print - no duplicates
        print(f"Loaded {len(self.existing_jobs)} existing jobs from sheets")
        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs")

    def run(self):
        """Main execution flow"""
        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()

        print("Scraping GitHub repositories...")
        self._scrape_simplify_github()

        print("\nProcessing email jobs...")
        try:
            email_data = self.email_extractor.fetch_job_emails()

            if email_data:
                print(f"Processing {len(email_data)} email URLs...")
                self._process_email_jobs(email_data)
            else:
                print("No email jobs found")

        except Exception as e:
            print(f"✗ Email processing error: {e}")
            logging.error(f"Email error: {e}", exc_info=True)

        self._ensure_mutual_exclusion()

        rows = self.sheets.get_next_row_numbers()
        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )
        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )

        # Clean summary only
        self._print_summary()
        print(f"\n✓ DONE: {added_valid} valid, {added_discarded} discarded")
        print("=" * 80 + "\n")

        logging.info(f"SUMMARY: {added_valid} valid, {added_discarded} discarded")

    def _scrape_simplify_github(self):
        """Scrape GitHub repositories"""
        simplify_jobs = self._safe_scrape(SIMPLIFY_URL, "SimplifyJobs")
        vanshb03_jobs = self._safe_scrape(VANSHB03_URL, "vanshb03")

        print(
            f"  Total: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03\n"
        )

        for i, job in enumerate(simplify_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(simplify_jobs) - i
                    print(
                        f"\n  SimplifyJobs: Early exit - skipping {skipped} old jobs\n"
                    )
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"GitHub job processing failed: {e}")
                continue

        for i, job in enumerate(vanshb03_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(vanshb03_jobs) - i
                    print(f"\n  vanshb03: Early exit - skipping {skipped} old jobs\n")
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"GitHub job processing failed: {e}")
                continue

        github_valid = sum(
            1 for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]
        )
        print(f"  GitHub summary: {github_valid} valid jobs")

    def _process_single_github_job(self, job):
        """Process individual GitHub job listing"""
        age_days = self._parse_github_age(job["age"])

        if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
            self.outcomes["skipped_too_old"] += 1
            return

        title = TitleProcessor.clean_title_aggressive(job["title"])

        if self._is_duplicate(job["company"], title, job["url"]):
            self.duplicate_jobs.append(
                {
                    "company": job["company"],
                    "title": title,
                    "url": job["url"],
                    "source": job["source"],
                }
            )
            self.outcomes["skipped_duplicate_url"] += 1
            return

        is_valid, invalid_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid:
            return

        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self._log_rejection(
                job["company"], title, job["url"], intern_reason, "skipped_senior_role"
            )
            return

        if job["is_closed"]:
            self._add_to_discarded(
                job["company"],
                title,
                job["location"],
                "Unknown",
                job["url"],
                "N/A",
                "Position closed",
                job["source"],
                "Unknown",
            )
            return

        if not TitleProcessor.is_cs_engineering_role(title):
            self._add_to_discarded(
                job["company"],
                title,
                job["location"],
                "Unknown",
                job["url"],
                "N/A",
                "Non-CS role",
                job["source"],
                "Unknown",
            )
            return

        url_intl_check = ValidationHelper.check_url_for_international(job["url"])
        if url_intl_check:
            self._log_rejection(
                job["company"],
                title,
                job["url"],
                url_intl_check,
                "skipped_url_international",
            )
            self._add_to_discarded(
                job["company"],
                title,
                "Canada",
                "Unknown",
                job["url"],
                "N/A",
                url_intl_check,
                job["source"],
                "Unknown",
            )
            return

        self._process_github_job(
            job["company"], title, job["location"], job["url"], job["source"]
        )

    def _process_github_job(self, company, title, location, url, source="GitHub"):
        """Process GitHub job with full extraction"""
        try:
            is_healthy, status_code = self.page_fetcher.check_url_health(url)
            if not is_healthy:
                reason = (
                    f"Dead URL ({status_code})" if status_code else "Connection failed"
                )
                self._log_rejection(company, title, url, reason, "skipped_dead_url")
                return

            response, final_url = self.page_fetcher.fetch_page(url)
            if not response:
                self._log_rejection(company, title, url, "HTTP failed", "failed_http")
                return

            clean_final = URLCleaner.clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.duplicate_jobs.append(
                    {
                        "company": company,
                        "title": title,
                        "url": final_url,
                        "source": source,
                    }
                )
                self.outcomes["skipped_duplicate_url"] += 1
                return

            self.processing_lock.add(clean_final)

            soup, parser = safe_parse_html(response.text)
            if not soup:
                self.outcomes["failed_parse"] += 1
                return

            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                self._log_rejection(
                    company,
                    title,
                    final_url,
                    f"Posted {job_age}d ago",
                    "skipped_too_old",
                )
                self._add_to_discarded(
                    company,
                    title,
                    "Unknown",
                    "Unknown",
                    final_url,
                    "N/A",
                    f"Posted {job_age} days ago",
                    source,
                    "Unknown",
                )
                return

            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, soup.get_text()[:2000]
            )
            if not is_valid_season:
                self._log_rejection(
                    company, title, final_url, season_reason, "skipped_wrong_season"
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

            review_flags = []
            decision, restriction, page_flags = (
                ValidationHelper.check_page_restrictions(soup)
            )
            if decision == "REJECT" and restriction:
                self._log_rejection(company, title, final_url, restriction, "discarded")
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

            if job_age is None:
                review_flags.append("⚠️ Age unknown")

            platform = PlatformDetector.detect(final_url)
            extracted_company = CompanyExtractor.extract_all_methods(final_url, soup)
            job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
            location_extracted = LocationExtractor.extract_all_methods(
                final_url, soup, title, platform
            )
            location_formatted = LocationProcessor.format_location_clean(
                location_extracted
            )
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url
            )
            sponsorship = ValidationHelper.check_sponsorship_status(soup)

            # CRITICAL: Canadian detection ALWAYS runs (even if location = "Unknown")
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup, final_url
            )
            if intl_check and "Canada" in str(intl_check):
                self._log_rejection(company, title, final_url, intl_check, "discarded")
                self._add_to_discarded(
                    company,
                    title,
                    "Canada",
                    remote,
                    final_url,
                    job_id,
                    intl_check,
                    source,
                    sponsorship,
                )
                return

            if location_formatted == "Unknown":
                review_flags.append("⚠️ Location extraction failed")

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
                self._log_rejection(
                    company, title, final_url, f"Low quality: {quality}/7", "discarded"
                )
                return

            role_alert = RoleCategorizer.get_terminal_alert(title)

            if review_flags:
                flags_str = ", ".join(review_flags)
                print(f"  {company[:30]}: ✓ Valid [{flags_str}] {role_alert}")
            else:
                print(f"  {company[:30]}: ✓ Valid {role_alert}")

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
            logging.error(f"GitHub job processing error: {e}", exc_info=True)

    def _process_email_jobs(self, email_data_list):
        """
        ENHANCED: Process email jobs with Simplify redirect resolution
        """
        simplify_resolved = 0
        simplify_failed = 0

        for email_data in email_data_list:
            try:
                # NEW: Resolve Simplify redirects
                if "simplify.jobs/p/" in email_data["url"].lower():
                    actual_url, resolved = SimplifyRedirectResolver.resolve(
                        email_data["url"]
                    )
                    if resolved:
                        simplify_resolved += 1
                        email_data["url"] = actual_url
                        self._process_resolved_email_job(email_data)
                    else:
                        simplify_failed += 1
                else:
                    self._process_resolved_email_job(email_data)

            except Exception as e:
                logging.error(f"Email job processing failed: {e}")
                continue

        # Report Simplify redirect stats (only if non-zero)
        if simplify_resolved > 0:
            print(f"\n  ✓ Simplify redirects resolved: {simplify_resolved}")
        if simplify_failed > 0:
            print(f"  ⊘ Simplify redirects failed: {simplify_failed}")
        if simplify_resolved > 0 or simplify_failed > 0:
            print()

    def _process_resolved_email_job(self, email_data):
        """Process email job after Simplify redirect resolution"""
        url = email_data["url"]
        email_html = email_data["email_html"]
        sender = email_data["sender"]

        # LinkedIn jobs (Jobright specific)
        if "linkedin.com/jobs" in url.lower():
            if sender.lower() == "jobright":
                soup, _ = safe_parse_html(email_html)
                if soup:
                    job_data = SourceParsers.parse_jobright_email(
                        soup, url, self.jobright_auth
                    )
                    if job_data:
                        result = self._validate_parsed_job(job_data, sender)
                        if result:
                            self._handle_validation_result(result, sender)
                return
            else:
                self.outcomes["skipped_linkedin"] += 1
                return

        # Validate URL
        is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
        if not is_valid_url:
            self.outcomes["skipped_invalid_url"] += 1
            return

        # Check URL for international markers
        url_intl_check = ValidationHelper.check_url_for_international(url)
        if url_intl_check:
            self.outcomes["skipped_url_international"] += 1
            return

        # Jobright resolution
        if "jobright.ai/jobs/info/" in url.lower():
            original_url = url
            url, is_company_site = self.jobright_auth.resolve_jobright_url(url)

            if url != original_url:
                self.outcomes["url_resolved"] += 1

                if "linkedin.com/jobs" in url.lower():
                    soup, _ = safe_parse_html(email_html)
                    if soup:
                        job_data = SourceParsers.parse_jobright_email(
                            soup, original_url, self.jobright_auth
                        )
                        if job_data:
                            result = self._validate_parsed_job(job_data, sender)
                            if result:
                                self._handle_validation_result(result, sender)
                        return

        # Check for duplicates
        clean_url = URLCleaner.clean_url(url)
        if clean_url in self.processing_lock or clean_url in self.existing_urls:
            self.duplicate_jobs.append(
                {"company": "Unknown", "title": "Unknown", "url": url, "source": sender}
            )
            self.outcomes["skipped_duplicate_url"] += 1
            return

        # Check URL health
        is_healthy, status_code = self.page_fetcher.check_url_health(url)
        if not is_healthy:
            self.outcomes["skipped_dead_url"] += 1
            return

        self.processing_lock.add(clean_url)

        # Process the job page
        result = self._process_single_email_job(url, email_html, sender)
        if result:
            self._handle_validation_result(result, sender)

    def _process_single_email_job(self, url, email_html, sender):
        """Process single email job with full extraction"""
        try:
            time.sleep(random.uniform(1.5, 2.5))

            soup, _ = safe_parse_html(email_html)
            if not soup:
                return None

            job_data = None

            # Try platform-specific parsers
            parser_map = {
                "ziprecruiter": SourceParsers.parse_ziprecruiter_email,
                "jobright": SourceParsers.parse_jobright_email,
                "adzuna": SourceParsers.parse_adzuna_email,
            }

            for key, parser_func in parser_map.items():
                if key in sender.lower():
                    job_data = (
                        parser_func(soup, url, self.jobright_auth)
                        if key == "jobright"
                        else parser_func(soup, url)
                    )
                    break

            # If email parser worked, validate and return
            if job_data:
                email_age = job_data.get("email_age_days")
                if email_age is not None and email_age > MAX_JOB_AGE_DAYS:
                    self.outcomes["skipped_too_old"] += 1
                    return self._create_discard_result(
                        job_data, f"Posted {email_age} days ago", sender
                    )

                # Jobright: fetch actual page if it's a company site
                if sender.lower() == "jobright" and job_data.get("is_company_site"):
                    actual_url = job_data["url"]

                    is_healthy, _ = self.page_fetcher.check_url_health(actual_url)
                    if not is_healthy:
                        return self._validate_parsed_job(job_data, sender)

                    response, final_url = self.page_fetcher.fetch_page(actual_url)
                    if response:
                        soup, _ = safe_parse_html(response.text)
                        if soup:
                            from utils import PlatformDetector

                            platform = PlatformDetector.detect(final_url)
                            self._enhance_job_data_from_page(
                                job_data, soup, final_url, platform
                            )

                            page_age = PageParser.extract_job_age_days(soup)
                            if page_age is not None and page_age > MAX_JOB_AGE_DAYS:
                                self.outcomes["skipped_too_old"] += 1
                                return self._create_discard_result(
                                    job_data, f"Posted {page_age} days ago", sender
                                )

                            is_valid_season, season_reason = (
                                TitleProcessor.check_season_requirement(
                                    job_data["title"], soup.get_text()[:2000]
                                )
                            )
                            if not is_valid_season:
                                self.outcomes["skipped_wrong_season"] += 1
                                return self._create_discard_result(
                                    job_data, season_reason, sender
                                )

                            decision, restriction, review_flags = (
                                ValidationHelper.check_page_restrictions(soup)
                            )
                            if decision == "REJECT" and restriction:
                                return self._create_discard_result(
                                    job_data, restriction, sender
                                )

                            # CRITICAL: Canadian detection with all methods
                            intl_check = LocationProcessor.check_if_international(
                                job_data["location"], soup, final_url
                            )
                            if intl_check and "Canada" in str(intl_check):
                                return self._create_discard_result(
                                    job_data, intl_check, sender
                                )

                return self._validate_parsed_job(job_data, sender)

            # Email parser didn't work - fetch actual page
            response, final_url = self.page_fetcher.fetch_page(url)
            if not response:
                self.outcomes["failed_http"] += 1
                return None

            clean_final = URLCleaner.clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.duplicate_jobs.append(
                    {
                        "company": "Unknown",
                        "title": "Unknown",
                        "url": final_url,
                        "source": sender,
                    }
                )
                self.outcomes["skipped_duplicate_url"] += 1
                return None

            self.processing_lock.add(clean_final)

            soup, _ = safe_parse_html(response.text)
            if not soup:
                self.outcomes["failed_parse"] += 1
                return None

            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                self.outcomes["skipped_too_old"] += 1
                return None

            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                "", soup.get_text()[:2000]
            )
            if not is_valid_season:
                self.outcomes["skipped_wrong_season"] += 1
                return None

            return self._process_scraped_page(soup, final_url, url, sender)

        except Exception as e:
            logging.error(f"Email job processing error: {e}")
            return None

    def _validate_parsed_job(self, job_data, sender):
        """Validate parsed job data"""
        title = TitleProcessor.clean_title_aggressive(job_data["title"])

        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
            return self._create_discard_result(job_data, intern_reason, sender)

        if not TitleProcessor.is_cs_engineering_role(title):
            return self._create_discard_result(job_data, "Non-CS role", sender)

        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            job_data["company"], title, job_data["url"]
        )
        if not is_valid_co:
            return self._create_discard_result(job_data, co_reason, sender)

        job_data["company"] = fixed_co

        normalized_key = URLCleaner.normalize_text(f"{fixed_co}_{title}")
        if normalized_key in self.existing_jobs:
            self.duplicate_jobs.append(
                {
                    "company": fixed_co,
                    "title": title,
                    "url": job_data["url"],
                    "source": sender,
                }
            )
            self.outcomes["skipped_duplicate_company_title"] += 1
            return None

        # CRITICAL: Canadian detection with URL
        intl_check = LocationProcessor.check_if_international(
            job_data.get("location", "Unknown"), None, job_data.get("url")
        )
        if intl_check and "Canada" in str(intl_check):
            return self._create_discard_result(job_data, intl_check, sender)

        location_formatted = LocationProcessor.format_location_clean(
            job_data.get("location", "Unknown")
        )
        review_flags = (
            job_data.get("review_flags", [])
            if isinstance(job_data.get("review_flags"), list)
            else []
        )

        if location_formatted == "Unknown":
            review_flags.append("⚠️ Location extraction failed")

        quality = QualityScorer.calculate_score(
            {
                "company": job_data["company"],
                "title": title,
                "location": location_formatted,
                "job_id": job_data.get("job_id", "N/A"),
                "sponsorship": job_data.get("sponsorship", "Unknown"),
            }
        )

        if not QualityScorer.is_acceptable_quality(quality):
            return self._create_discard_result(
                job_data, f"Low quality: {quality}/7", sender
            )

        self.processed_cache[normalized_key] = {
            "company": job_data["company"],
            "title": title,
            "job_id": job_data.get("job_id", "N/A"),
            "url": job_data["url"],
        }

        return {
            "decision": "valid",
            "company": job_data["company"],
            "title": title,
            "location": location_formatted,
            "remote": job_data.get("remote", "Unknown"),
            "url": job_data["url"],
            "job_id": job_data.get("job_id", "N/A"),
            "source": sender,
            "sponsorship": job_data.get("sponsorship", "Unknown"),
            "review_flags": ", ".join(review_flags) if review_flags else "",
        }

    def _process_scraped_page(self, soup, final_url, original_url, sender):
        """Process scraped job page"""
        if "jobright.ai/jobs/info/" in final_url.lower():
            jobright_data = PageParser.extract_jobright_data(
                soup, final_url, self.jobright_auth
            )
            if jobright_data:
                return self._validate_parsed_job(jobright_data, sender)
            return None

        from utils import PlatformDetector

        platform = PlatformDetector.detect(final_url)
        company = CompanyExtractor.extract_all_methods(final_url, soup)
        title_raw = PageParser.extract_title(soup)

        if not title_raw or title_raw == "Unknown":
            return None

        title = TitleProcessor.clean_title_aggressive(title_raw)

        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            self.outcomes["skipped_senior_role"] += 1
            return self._create_discard_result(
                {"company": company, "title": title, "url": final_url},
                intern_reason,
                sender,
            )

        if not TitleProcessor.is_cs_engineering_role(title):
            return self._create_discard_result(
                {"company": company, "title": title, "url": final_url},
                "Non-CS role",
                sender,
            )

        normalized_key = URLCleaner.normalize_text(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            self.duplicate_jobs.append(
                {"company": company, "title": title, "url": final_url, "source": sender}
            )
            self.outcomes["skipped_duplicate_company_title"] += 1
            return None

        decision, restriction, page_flags = ValidationHelper.check_page_restrictions(
            soup
        )
        if decision == "REJECT" and restriction:
            return self._create_discard_result(
                {"company": company, "title": title, "url": final_url},
                restriction,
                sender,
            )

        job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
        location_extracted = LocationExtractor.extract_all_methods(
            final_url, soup, title, platform
        )
        location_formatted = LocationProcessor.format_location_clean(location_extracted)
        remote = LocationProcessor.extract_remote_status_enhanced(
            soup, location_formatted, final_url
        )
        sponsorship = ValidationHelper.check_sponsorship_status(soup)

        review_flags = []

        # CRITICAL: Canadian detection with all methods
        intl_check = LocationProcessor.check_if_international(
            location_formatted, soup, final_url
        )
        if intl_check and "Canada" in str(intl_check):
            return self._create_discard_result(
                {
                    "company": company,
                    "title": title,
                    "location": "Canada",
                    "url": final_url,
                    "job_id": job_id,
                },
                intl_check,
                sender,
            )

        if location_formatted == "Unknown":
            review_flags.append("⚠️ Location extraction failed")

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
            return self._create_discard_result(
                {
                    "company": company,
                    "title": title,
                    "location": location_formatted,
                    "url": final_url,
                    "job_id": job_id,
                },
                f"Low quality: {quality}/7",
                sender,
            )

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
            "review_flags": ", ".join(review_flags) if review_flags else "",
        }

    def _enhance_job_data_from_page(self, job_data, soup, final_url, platform):
        """Enhance job data by extracting from actual page"""
        page_location = LocationExtractor.extract_all_methods(
            final_url, soup, job_data.get("title", ""), platform
        )
        page_location_formatted = LocationProcessor.format_location_clean(page_location)
        page_remote = LocationProcessor.extract_remote_status_enhanced(
            soup, page_location_formatted, final_url
        )
        page_job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
        page_company = CompanyExtractor.extract_all_methods(final_url, soup)

        if page_location_formatted != "Unknown":
            job_data["location"] = page_location_formatted

        if page_remote != "Unknown":
            job_data["remote"] = page_remote

        if page_job_id != "N/A":
            job_data["job_id"] = page_job_id

        if page_company and page_company != "Unknown":
            job_data["company"] = page_company

    def _create_discard_result(self, job_data, reason, sender):
        """Create discard result object"""
        return {
            "decision": "discard",
            "company": job_data.get("company", "Unknown"),
            "title": job_data.get("title", "Unknown"),
            "location": job_data.get("location", "Unknown"),
            "remote": job_data.get("remote", "Unknown"),
            "url": job_data.get("url", ""),
            "job_id": job_data.get("job_id", "N/A"),
            "reason": reason,
            "source": sender,
            "sponsorship": job_data.get("sponsorship", "Unknown"),
        }

    def _handle_validation_result(self, result, sender):
        """Handle validation result (valid or discard)"""
        if not result:
            return

        if result.get("decision") == "valid":
            role_alert = RoleCategorizer.get_terminal_alert(result["title"])
            flags = result.get("review_flags", "")

            status_msg = f"  {result['company'][:30]} ({sender}): ✓ Valid"
            if flags:
                status_msg += f" [{flags}]"
            if role_alert:
                status_msg += f" {role_alert}"
            print(status_msg)

            self.valid_jobs.append(
                {
                    "company": result["company"],
                    "job_id": result["job_id"],
                    "title": result["title"],
                    "job_type": (
                        "Co-op" if "co-op" in result["title"].lower() else "Internship"
                    ),
                    "location": result["location"],
                    "remote": result["remote"],
                    "entry_date": self._format_date(),
                    "url": result["url"],
                    "source": result["source"],
                    "sponsorship": result["sponsorship"],
                }
            )

            self._update_tracking(
                result["company"], result["title"], result["url"], result["job_id"]
            )
            self.outcomes["valid"] += 1

        elif result.get("decision") == "discard":
            print(
                f"  {result['company'][:30]} ({sender}): ✗ {self._truncate(result['reason'], 50)}"
            )

            self.discarded_jobs.append(
                {
                    "company": result["company"],
                    "title": result["title"],
                    "location": result["location"],
                    "job_type": (
                        "Co-op" if "co-op" in result["title"].lower() else "Internship"
                    ),
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

    def _log_rejection(self, company, title, url, reason, outcome_key):
        """Log rejection with clean formatting"""
        company_display = (company[:30] if company else "Unknown").ljust(30)
        reason_display = self._truncate(reason, 60)
        print(f"  {company_display}: ✗ {reason_display}")
        logging.info(f"REJECTED | {company} | {title} | {reason} | {url}")
        self.outcomes[outcome_key] += 1

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

    def _ensure_mutual_exclusion(self):
        """Ensure no job appears in both valid and discarded"""
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
        """CLEANED: Simple summary only - no duplicate section"""
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
            ("⊘ Dead URL", self.outcomes["skipped_dead_url"]),
            ("⊘ Too old", self.outcomes["skipped_too_old"]),
        ]

        for label, count in summary_items:
            if count > 0:
                print(f"  {label}: {count}")

        print("=" * 80)

    @staticmethod
    def _parse_github_age(age_str):
        """Parse GitHub age string to days"""
        if not age_str:
            return None

        match = re.match(r"^(\d+)d$", age_str.lower())
        if match:
            return int(match.group(1))

        match = re.match(r"^(\d+)mo$", age_str.lower())
        if match:
            return int(match.group(1)) * 30

        days_ago = DateParser.extract_days_ago(age_str)
        return days_ago

    @staticmethod
    def _truncate(text, max_length=50):
        """Truncate text to max length"""
        return text if len(text) <= max_length else text[: max_length - 3] + "..."

    @staticmethod
    def _format_date():
        """Format current date/time"""
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")

    @staticmethod
    def _safe_scrape(url, source_name):
        """Safe scraping with error handling"""
        try:
            return SimplifyGitHubScraper.scrape(url, source_name=source_name)
        except Exception as e:
            print(f"  ✗ {source_name} error: {e}")
            logging.error(f"{source_name} scraping failed: {e}")
            return []


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
