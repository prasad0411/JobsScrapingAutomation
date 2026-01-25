#!/usr/bin/env python3

import time
import datetime
import random
import re
import logging
import json
import os
from collections import defaultdict

from config import (
    SIMPLIFY_URL,
    VANSHB03_URL,
    MAX_JOB_AGE_DAYS,
    CANADA_PROVINCES,
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
        print(f"Loaded {len(self.existing_jobs)} jobs from sheets")
        logging.info(f"Loaded {len(self.existing_jobs)} existing jobs")

    def run(self):
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
        except Exception as e:
            print(f"Email processing error: {e}")
            logging.error(f"Email error: {e}", exc_info=True)
        self._ensure_mutual_exclusion()
        rows = self.sheets.get_next_row_numbers()
        added_valid = self.sheets.add_valid_jobs(
            self.valid_jobs, rows["valid"], rows["valid_sr_no"]
        )
        added_discarded = self.sheets.add_discarded_jobs(
            self.discarded_jobs, rows["discarded"], rows["discarded_sr_no"]
        )
        self._print_summary()
        print(f"\n✓ DONE: {added_valid} valid, {added_discarded} discarded")
        print("=" * 80 + "\n")
        logging.info(f"SUMMARY: {added_valid} valid, {added_discarded} discarded")

    def _scrape_simplify_github(self):
        simplify_jobs = self._safe_scrape(SIMPLIFY_URL, "SimplifyJobs")
        vanshb03_jobs = self._safe_scrape(VANSHB03_URL, "vanshb03")
        print(
            f"  Total: {len(simplify_jobs)} SimplifyJobs + {len(vanshb03_jobs)} vanshb03\n"
        )
        logging.info(f"GitHub: {len(simplify_jobs)} + {len(vanshb03_jobs)}")
        for i, job in enumerate(simplify_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(simplify_jobs) - i
                    logging.info(f"SimplifyJobs: Early exit - {skipped} old")
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"GitHub error: {e}")
                continue
        for i, job in enumerate(vanshb03_jobs):
            try:
                age_days = self._parse_github_age(job["age"])
                if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
                    skipped = len(vanshb03_jobs) - i
                    logging.info(f"vanshb03: Early exit - {skipped} old")
                    self.outcomes["skipped_too_old"] += skipped
                    break
                self._process_single_github_job(job)
            except Exception as e:
                logging.error(f"GitHub error: {e}")
                continue
        github_valid = sum(
            1 for j in self.valid_jobs if j["source"] in ["SimplifyJobs", "vanshb03"]
        )
        print(f"  GitHub summary: {github_valid} valid jobs\n")

    def _process_single_github_job(self, job):
        age_days = self._parse_github_age(job["age"])
        if age_days is not None and age_days > MAX_JOB_AGE_DAYS:
            self.outcomes["skipped_too_old"] += 1
            return
        title = TitleProcessor.clean_title_aggressive(job["title"])
        if self._is_duplicate(job["company"], title, job["url"]):
            self.duplicate_jobs.append(
                {"company": job["company"], "title": title, "url": job["url"]}
            )
            self.outcomes["skipped_duplicate_url"] += 1
            return
        is_valid, invalid_reason = TitleProcessor.is_valid_job_title(title)
        if not is_valid:
            print(f"  {job['company'][:30]}: ✗ {invalid_reason}")
            logging.info(f"REJECTED | {job['company']} | {invalid_reason}")
            return
        is_intern, intern_reason = TitleProcessor.is_internship_role(title)
        if not is_intern:
            print(f"  {job['company'][:30]}: ✗ {intern_reason}")
            self.outcomes["skipped_senior_role"] += 1
            return
        if job["is_closed"]:
            print(f"  {job['company'][:30]}: ✗ Closed")
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
            print(f"  {job['company'][:30]}: ✗ Non-CS")
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
            print(f"  {job['company'][:30]}: ✗ {url_intl_check}")
            country = (
                url_intl_check.split(":")[-1].strip().replace("(from URL)", "").strip()
            )
            self._add_to_discarded(
                job["company"],
                title,
                country,
                "Unknown",
                job["url"],
                "N/A",
                url_intl_check,
                job["source"],
                "Unknown",
            )
            self.outcomes["skipped_url_international"] += 1
            return
        self._process_github_job(
            job["company"], title, job["location"], job["url"], job["source"]
        )

    def _process_github_job(self, company, title, location, url, source="GitHub"):
        try:
            is_healthy, status_code = self.page_fetcher.check_url_health(url)
            if not is_healthy:
                reason = (
                    f"Dead URL ({status_code})" if status_code else "Connection failed"
                )
                print(f"  {company[:30]}: ✗ {reason}")
                self.outcomes["skipped_dead_url"] += 1
                return
            response, final_url, page_source = self.page_fetcher.fetch_page(url)
            if not response:
                print(f"  {company[:30]}: ✗ HTTP failed")
                self.outcomes["failed_http"] += 1
                return
            clean_final = URLCleaner.clean_url(final_url)
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.duplicate_jobs.append(
                    {"company": company, "title": title, "url": final_url}
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
                print(f"  {company[:30]}: ✗ Posted {job_age}d ago")
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
                self.outcomes["skipped_too_old"] += 1
                return
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, soup.get_text()[:2000]
            )
            if not is_valid_season:
                print(f"  {company[:30]}: ✗ {season_reason}")
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
                self.outcomes["skipped_wrong_season"] += 1
                return
            review_flags = []
            decision, restriction, page_flags = (
                ValidationHelper.check_page_restrictions(soup)
            )
            if decision == "REJECT" and restriction:
                print(f"  {company[:30]}: ✗ {restriction}")
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
                self.outcomes["discarded"] += 1
                return
            if page_flags:
                review_flags.extend(page_flags)
            if job_age is None:
                review_flags.append("⚠️ Age unknown")
            platform = PlatformDetector.detect(final_url)
            extracted_company = CompanyExtractor.extract_all_methods(final_url, soup)
            job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
            location_extracted = LocationExtractor.extract_all_methods(
                final_url, soup, title, platform, page_source
            )
            location_formatted = LocationProcessor.format_location_clean(
                location_extracted
            )
            from extractors import DescriptionExtractor

            description = DescriptionExtractor.extract(soup, platform)
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url, description
            )
            sponsorship = self._check_sponsorship_tracking_only(soup)
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup, final_url, title
            )
            if intl_check:
                if "Canada" in str(intl_check) or "International" in str(intl_check):
                    print(f"  {company[:30]}: ✗ {intl_check}")
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
                self.outcomes["discarded"] += 1
                return
            if location_formatted == "Unknown":
                review_flags.append("⚠️ Location failed")
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
                print(f"  {company[:30]}: ✗ Quality ({quality}/7)")
                self.outcomes["discarded"] += 1
                return
            role_alert = RoleCategorizer.get_terminal_alert(title)
            if review_flags:
                print(f"  {company[:30]}: ✓ [{', '.join(review_flags)}] {role_alert}")
            else:
                print(f"  {company[:30]}: ✓ {role_alert}")
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
            logging.error(f"ERROR | {company} | {e}")

    def _process_emails_grouped(self, emails_data):
        total_resolved = 0
        total_failed = 0
        consecutive_failures = 0
        for email_idx, email in enumerate(emails_data, 1):
            email_id = email["email_id"]
            subject = email["subject"][:70]
            sender = email["sender"]
            url_count = len(email["urls"])
            print(f'\nEmail {email_idx}/{len(emails_data)}: "{subject}" ({sender})')
            print(f"  {url_count} URLs from this email...\n")
            for url in email["urls"]:
                try:
                    if "simplify.jobs/p/" in url.lower():
                        actual_url, resolved = SimplifyRedirectResolver.resolve(
                            url, self.page_fetcher
                        )
                        if resolved:
                            total_resolved += 1
                            consecutive_failures = 0
                            self._process_single_url_with_extraction(
                                actual_url, email["html"], sender
                            )
                        else:
                            total_failed += 1
                            consecutive_failures += 1
                            self.outcomes["skipped_no_canonical"] += 1
                            if consecutive_failures >= 20:
                                print(
                                    f"  ⚠️  WARNING: 20 Simplify URLs failed consecutively - continuing\n"
                                )
                                consecutive_failures = 0
                    else:
                        self._process_single_url_with_extraction(
                            url, email["html"], sender
                        )
                except Exception as e:
                    print(f"    ERROR: {str(e)[:50]}")
                    logging.error(f"Exception: {e}")
                    continue
        if total_resolved > 0 or total_failed > 0:
            print(
                f"\n  Simplify totals: {total_resolved} resolved, {total_failed} failed\n"
            )

    def _process_single_url_with_extraction(self, url, email_html, sender):
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
                        else:
                            print(
                                f"    {job_data.get('company', 'Unknown')[:28]}: ⊘ Duplicate"
                            )
                        return
            else:
                self.outcomes["skipped_linkedin"] += 1
                return
        is_valid_url, url_reason = ValidationHelper.is_valid_job_url(url)
        if not is_valid_url:
            self.outcomes["skipped_invalid_url"] += 1
            return
        url_intl_check = ValidationHelper.check_url_for_international(url)
        if url_intl_check:
            self.outcomes["skipped_url_international"] += 1
            return
        if "jobright.ai/jobs/info/" in url.lower():
            soup, _ = safe_parse_html(email_html)
            if not soup:
                logging.warning("Jobright: No email HTML to parse")
                return

            email_data = SourceParsers.parse_jobright_email(
                soup, url, self.jobright_auth
            )

            if not email_data:
                logging.warning("Jobright: Email parsing returned None")
                return

            email_company = email_data.get("company", "Unknown")
            email_title = email_data.get("title", "Unknown")
            email_location = email_data.get("location", "Unknown")
            email_age = email_data.get("email_age_days")

            logging.info(
                f"Jobright: Email data - {email_company} | {email_title} | {email_location}"
            )

            if email_age is not None and email_age > MAX_JOB_AGE_DAYS:
                print(f"    {email_company[:28]}: ✗ Posted {email_age}d ago")
                self.outcomes["skipped_too_old"] += 1
                return

            canonical_url = self.page_fetcher.extract_jobright_canonical(url)

            if (
                not canonical_url
                or "simplify" in canonical_url
                or "linkedin" in canonical_url
            ):
                logging.info("Jobright: No canonical URL found, skipping job")
                print(f"    {email_company[:28]}: ✗ No canonical URL")
                self.outcomes["skipped_no_canonical"] += 1
                return

            logging.info(f"Jobright: ✓ Canonical URL: {canonical_url[:60]}")
            response, final_url, page_source = self.page_fetcher.fetch_page(
                canonical_url
            )

            if not response or not final_url:
                logging.warning("Jobright: Page fetch failed, skipping")
                print(f"    {email_company[:28]}: ✗ Page fetch failed")
                self.outcomes["skipped_fetch_failed"] += 1
                return

            soup_page, _ = safe_parse_html(response.text)
            if not soup_page:
                logging.warning("Jobright: Page parse failed, skipping")
                print(f"    {email_company[:28]}: ✗ Parse failed")
                self.outcomes["skipped_parse_failed"] += 1
                return

            platform = PlatformDetector.detect(final_url)
            self._enhance_job_data_from_page(
                email_data, soup_page, final_url, platform, page_source
            )

            if email_data.get("company") == "Unknown" and email_company != "Unknown":
                email_data["company"] = email_company
            if email_data.get("location") == "Unknown" and email_location != "Unknown":
                email_data["location"] = email_location
            if email_data.get("title") == "Unknown" and email_title != "Unknown":
                email_data["title"] = email_title

            email_data["url"] = final_url

            page_age = PageParser.extract_job_age_days(soup_page)
            if page_age is not None and page_age > MAX_JOB_AGE_DAYS:
                print(f"    {email_data['company'][:28]}: ✗ Posted {page_age}d ago")
                self.outcomes["skipped_too_old"] += 1
                return

            decision, restriction, review_flags = (
                self._check_page_restrictions_no_sponsorship(soup_page)
            )
            if decision == "REJECT" and restriction:
                print(f"    {email_data['company'][:28]}: ✗ {restriction}")
                return self._create_discard_result(email_data, restriction, sender)

            result = self._validate_parsed_job(email_data, sender)
            if result:
                self._handle_validation_result(result, sender)
            else:
                print(f"    {email_data.get('company', 'Unknown')[:28]}: ⊘ Duplicate")
            return
        clean_url = URLCleaner.clean_url(url)
        if clean_url in self.processing_lock:
            self.outcomes["skipped_duplicate_url"] += 1
            return
        self.processing_lock.add(clean_url)
        result = self._process_single_email_job(url, email_html, sender)
        if result:
            self._handle_validation_result(result, sender)

    def _process_single_email_job(self, url, email_html, sender):
        try:
            time.sleep(random.uniform(1.0, 1.5))
            response, final_url, page_source = self.page_fetcher.fetch_page(url)
            if not response:
                self.outcomes["failed_http"] += 1
                return None
            soup, _ = safe_parse_html(response.text)
            if not soup:
                self.outcomes["failed_parse"] += 1
                return None
            platform = PlatformDetector.detect(final_url)
            company = CompanyExtractor.extract_all_methods(final_url, soup)
            title_raw = PageParser.extract_title(soup)
            if not title_raw or title_raw == "Unknown":
                return None
            title = TitleProcessor.clean_title_aggressive(title_raw)
            clean_final = URLCleaner.clean_url(final_url)
            if clean_final in self.existing_urls:
                print(f"    {company[:28]}: ⊘ Duplicate")
                self.duplicate_jobs.append(
                    {"company": company, "title": title, "url": final_url}
                )
                self.outcomes["skipped_duplicate_url"] += 1
                return None
            normalized_key = URLCleaner.normalize_text(f"{company}_{title}")
            if normalized_key in self.existing_jobs:
                print(f"    {company[:28]}: ⊘ Duplicate job")
                self.duplicate_jobs.append(
                    {"company": company, "title": title, "url": final_url}
                )
                self.outcomes["skipped_duplicate_company_title"] += 1
                return None
            job_age = PageParser.extract_job_age_days(soup)
            if job_age is not None and job_age > MAX_JOB_AGE_DAYS:
                print(f"    {company[:28]}: ✗ Posted {job_age}d ago")
                self.outcomes["skipped_too_old"] += 1
                return None
            is_valid_season, season_reason = TitleProcessor.check_season_requirement(
                title, soup.get_text()[:2000]
            )
            if not is_valid_season:
                print(f"    {company[:28]}: ✗ {season_reason}")
                self.outcomes["skipped_wrong_season"] += 1
                return None
            is_intern, intern_reason = TitleProcessor.is_internship_role(title)
            if not is_intern:
                print(f"    {company[:28]}: ✗ {intern_reason}")
                self.outcomes["skipped_senior_role"] += 1
                return self._create_discard_result(
                    {"company": company, "title": title, "url": final_url},
                    intern_reason,
                    sender,
                )
            if not TitleProcessor.is_cs_engineering_role(title):
                print(f"    {company[:28]}: ✗ Non-CS")
                return self._create_discard_result(
                    {"company": company, "title": title, "url": final_url},
                    "Non-CS role",
                    sender,
                )
            decision, restriction, page_flags = (
                self._check_page_restrictions_no_sponsorship(soup)
            )
            if decision == "REJECT" and restriction:
                print(f"    {company[:28]}: ✗ {restriction}")
                return self._create_discard_result(
                    {"company": company, "title": title, "url": final_url},
                    restriction,
                    sender,
                )
            job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
            location_extracted = LocationExtractor.extract_all_methods(
                final_url, soup, title, platform, page_source
            )
            location_formatted = LocationProcessor.format_location_clean(
                location_extracted
            )
            from extractors import DescriptionExtractor

            description = DescriptionExtractor.extract(soup, platform)
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url, description
            )
            sponsorship = self._check_sponsorship_tracking_only(soup)
            review_flags = []
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup, final_url, title
            )
            if intl_check and (
                "Canada" in str(intl_check) or "International" in str(intl_check)
            ):
                print(f"    {company[:28]}: ✗ {intl_check}")
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
                review_flags.append("⚠️ Location failed")
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
                print(f"    {company[:28]}: ✗ Quality {quality}/7")
                return self._create_discard_result(
                    {
                        "company": company,
                        "title": title,
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
        except Exception as e:
            logging.error(f"ERROR | {url} | {e}")
            return None

    def _validate_parsed_job(self, job_data, sender):
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
                {"company": fixed_co, "title": title, "url": job_data["url"]}
            )
            self.outcomes["skipped_duplicate_company_title"] += 1
            return None
        intl_check = LocationProcessor.check_if_international(
            job_data.get("location", "Unknown"),
            None,
            job_data.get("url"),
            job_data.get("title", ""),
        )
        if intl_check:
            if "Canada" in str(intl_check) or "International" in str(intl_check):
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
            review_flags.append("⚠️ Location failed")
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

    def _enhance_job_data_from_page(
        self, job_data, soup, final_url, platform, page_source=""
    ):
        page_location = LocationExtractor.extract_all_methods(
            final_url, soup, job_data.get("title", ""), platform, page_source
        )
        page_location_formatted = LocationProcessor.format_location_clean(page_location)
        from extractors import DescriptionExtractor

        page_description = DescriptionExtractor.extract(soup, platform)
        page_remote = LocationProcessor.extract_remote_status_enhanced(
            soup, page_location_formatted, final_url, page_description
        )
        page_job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
        page_company = CompanyExtractor.extract_all_methods(final_url, soup)
        if page_location_formatted != "Unknown":
            job_data["location"] = page_location_formatted
        if page_remote != "Unknown":
            job_data["remote"] = page_remote
        if page_job_id != "N/A":
            job_data["job_id"] = page_job_id
        if (
            page_company
            and page_company != "Unknown"
            and not self._looks_like_title(page_company)
        ):
            job_data["company"] = page_company

    def _check_page_restrictions_no_sponsorship(self, soup):
        page_text = soup.get_text().lower()[:5000]
        clearance_patterns = [
            r"(?:security\s+)?clearance.*required",
            r"must\s+(?:be\s+able\s+to\s+)?obtain.*clearance",
            r"dod\s+(?:secret|top\s+secret)",
            r"ts/sci",
        ]
        for pattern in clearance_patterns:
            if re.search(pattern, page_text, re.I):
                return "REJECT", "Security clearance required", []
        review_flags = []
        if any(
            kw in page_text
            for kw in ["review required", "screening", "background check"]
        ):
            review_flags.append("⚠️ Review process")
        return "ACCEPT", None, review_flags

    def _check_sponsorship_tracking_only(self, soup):
        page_text = soup.get_text().lower()[:5000]
        sponsorship_patterns = [
            r"(?:no|not|without).{0,100}(?:current|future).{0,50}sponsor(?:ship)?",
            r"(?:no|not).{0,50}sponsor(?:ship)?\s+(?:available|offered|provided)",
            r"sponsor(?:ship)?\s+(?:not available|unavailable|not offered)",
            r"must (?:be|have).{0,50}(?:authorized|authorization).{0,50}(?:without|no).{0,50}sponsor",
            r"we\s+(?:do\s+not|don't)\s+sponsor",
        ]
        for pattern in sponsorship_patterns:
            if re.search(pattern, page_text, re.I):
                return "No"
        if re.search(r"(?:we\s+)?sponsor(?:s)?\s+(?:h1b|visa)", page_text, re.I):
            return "Yes"
        return "Unknown"

    def _create_discard_result(self, job_data, reason, sender):
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
        if not result:
            return
        if result.get("decision") == "valid":
            role_alert = RoleCategorizer.get_terminal_alert(result["title"])
            flags = result.get("review_flags", "")
            status_msg = f"    {result['company'][:28]}: ✓"
            if flags:
                status_msg += f" [{flags}]"
            if role_alert:
                status_msg += f" {role_alert}"
            print(status_msg)
            logging.info(f"ACCEPTED | {result['company']} | {result['title']}")
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
            print(f"    {result['company'][:28]}: ✗ {result['reason'][:35]}")
            logging.info(f"REJECTED | {result['company']} | {result['reason']}")
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

    def _add_to_valid(
        self, company, title, location, remote, url, job_id, sponsorship, source
    ):
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
        key = URLCleaner.normalize_text(f"{company}_{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(URLCleaner.clean_url(url))
        if job_id != "N/A" and not job_id.startswith("HASH_"):
            self.existing_job_ids.add(job_id.lower())

    def _is_duplicate(self, company, title, url, job_id="N/A"):
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
            logging.error(f"{source_name} failed: {e}")
            return []


if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
