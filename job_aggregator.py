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
    PROCESSED_EMAILS_FILE,
    EMAIL_TRACKING_RETENTION_DAYS,
    TECHNICAL_ROLE_KEYWORDS,
    NON_TECHNICAL_PURE,
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
    JobTypeExtractor,
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


class EmailTracker:
    @staticmethod
    def load_processed_emails():
        if os.path.exists(PROCESSED_EMAILS_FILE):
            try:
                with open(PROCESSED_EMAILS_FILE, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    @staticmethod
    def save_processed_emails(processed_emails):
        try:
            with open(PROCESSED_EMAILS_FILE, "w") as f:
                json.dump(processed_emails, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save processed emails: {e}")

    @staticmethod
    def cleanup_old_entries(processed_emails):
        cutoff_date = datetime.datetime.now() - datetime.timedelta(
            days=EMAIL_TRACKING_RETENTION_DAYS
        )
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        cleaned = {
            k: v
            for k, v in processed_emails.items()
            if v.get("processed_date", "9999-99-99") > cutoff_str
        }
        if len(cleaned) < len(processed_emails):
            logging.info(
                f"Cleaned up {len(processed_emails) - len(cleaned)} old email entries"
            )
        return cleaned

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

        self.existing_urls = self.sheets.load_urls_only()
        self.existing_company_titles = self.sheets.load_company_titles_only()
        self.existing_job_ids = self.sheets.load_job_ids_only()
        self.existing_jobs = self.existing_company_titles
        self.processed_cache = {}

        self.processing_lock = set()
        self.valid_jobs = []
        self.discarded_jobs = []
        self.duplicate_jobs = []
        self.outcomes = defaultdict(int)
        self.jobs_processed_count = 0
        self.page_text_cache = {}

        logging.info(
            f"Loaded {len(self.existing_urls)} existing URLs, {len(self.existing_company_titles)} company+title pairs, {len(self.existing_job_ids)} job IDs"
        )

    def run(self):
        if not self.jobright_auth.cookies:
            self.jobright_auth.login_interactive()
        self._scrape_simplify_github()
        print("\nProcessing email jobs...")
        try:
            emails_data = self.email_extractor.fetch_job_emails()
            if emails_data:
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
        from config import SHOW_GITHUB_COUNTS

        simplify_jobs = self._safe_scrape(SIMPLIFY_URL, "SimplifyJobs")
        vanshb03_jobs = self._safe_scrape(VANSHB03_URL, "vanshb03")

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

        simplify_valid = sum(
            1 for j in self.valid_jobs if j["source"] == "SimplifyJobs"
        )
        vanshb03_valid = sum(1 for j in self.valid_jobs if j["source"] == "vanshb03")
        github_valid = simplify_valid + vanshb03_valid

        print(
            f"GitHub: {github_valid} valid jobs ({simplify_valid} SimplifyJobs, {vanshb03_valid} vanshb03)\n"
        )

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
            logging.info(
                f"REJECTED | {job['company']} | {invalid_reason} | Title: '{title}' | URL: {job['url'][:60]}"
            )
            return
        is_intern, intern_reason = TitleProcessor.is_internship_role(title, "", "")
        if not is_intern:
            print(f"  {job['company'][:30]}: ✗ {intern_reason}")
            logging.info(
                f"REJECTED | {job['company']} | {intern_reason} | Title: '{title}' | Has intern: {'intern' in title.lower()} | Has co-op: {'co-op' in title.lower()} | Has apprentice: {'apprentice' in title.lower()}"
            )
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
            matched_kw = [kw for kw in TECHNICAL_ROLE_KEYWORDS if kw in title.lower()]
            print(f"  {job['company'][:30]}: ✗ Non-CS")
            logging.info(
                f"REJECTED | {job['company']} | Non-CS | Title: '{title}' | Matched keywords: {matched_kw} | Tech count: {len(matched_kw)}"
            )
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
            from config import COMPANY_BLACKLIST, COMPANY_BLACKLIST_REASONS

            company_upper = company.upper()
            for blacklisted in COMPANY_BLACKLIST:
                if blacklisted.upper() in company_upper:
                    reason = COMPANY_BLACKLIST_REASONS.get(
                        blacklisted, f"Blacklisted company: {blacklisted}"
                    )
                    print(f"  {company[:30]}: ✗ {reason}")
                    self._add_to_discarded(
                        company,
                        title,
                        location,
                        "Unknown",
                        url,
                        "N/A",
                        reason,
                        source,
                        "Unknown",
                    )
                    self.outcomes["skipped_blacklisted_company"] = (
                        self.outcomes.get("skipped_blacklisted_company", 0) + 1
                    )
                    return
        except (ImportError, AttributeError):
            pass

        try:
            from config import PLATFORM_BLACKLIST, PLATFORM_BLACKLIST_REASONS

            url_lower = url.lower()
            for blacklisted_pattern in PLATFORM_BLACKLIST:
                if blacklisted_pattern in url_lower:
                    reason = PLATFORM_BLACKLIST_REASONS.get(
                        blacklisted_pattern,
                        f"Blacklisted platform: {blacklisted_pattern}",
                    )
                    print(f"  {company[:30]}: ✗ {reason}")
                    self._add_to_discarded(
                        company,
                        title,
                        location,
                        "Unknown",
                        url,
                        "N/A",
                        reason,
                        source,
                        "Unknown",
                    )
                    self.outcomes["skipped_blacklisted_platform"] = (
                        self.outcomes.get("skipped_blacklisted_platform", 0) + 1
                    )
                    return
        except (ImportError, AttributeError):
            pass

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
                logging.info(
                    f"REJECTED | {company} | {restriction} | Title: '{title}' | Source: {source} | URL: {final_url[:60]}"
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
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url
            )
            sponsorship = ValidationHelper.check_sponsorship_status(soup)
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup, final_url, title
            )
            if intl_check:
                print(f"  {company[:30]}: ✗ {intl_check}")
                logging.info(
                    f"REJECTED | {company} | {intl_check} | Title: '{title}' | Location: {location_formatted} | Source: {source}"
                )
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
        try:
            from config import REPROCESS_EMAILS_DAYS, EMAIL_DATE_FILTER_ENABLED
        except (ImportError, AttributeError):
            REPROCESS_EMAILS_DAYS = 4
            EMAIL_DATE_FILTER_ENABLED = True

        processed_emails = EmailTracker.load_processed_emails()
        processed_emails = EmailTracker.cleanup_old_entries(processed_emails)

        from datetime import datetime, timedelta

        cutoff_date = (datetime.now() - timedelta(days=REPROCESS_EMAILS_DAYS)).strftime(
            "%Y-%m-%d"
        )

        total_resolved = 0
        total_failed = 0
        emails_processed = 0
        emails_skipped = 0

        for email_idx, email in enumerate(emails_data, 1):
            email_id = email["email_id"]
            subject = email["subject"][:70]
            sender = email["sender"]
            url_count = len(email["urls"])

            if email_id in processed_emails:
                email_date = processed_emails[email_id].get("processed_date", "")

                should_reprocess = False
                if EMAIL_DATE_FILTER_ENABLED and email_date:
                    if email_date >= cutoff_date:
                        should_reprocess = True
                        logging.debug(f"Reprocessing recent email from {email_date}")

                if not should_reprocess:
                    print(
                        f'\nEmail {email_idx}/{len(emails_data)}: "{subject}" ({sender})'
                    )
                    print(
                        f"  ⊘ Already processed on {processed_emails[email_id]['processed_date']}"
                    )
                    emails_skipped += 1
                    continue

            emails_processed += 1
            print(f'\nEmail {email_idx}/{len(emails_data)}: "{subject}" ({sender})')
            print(f"  {url_count} URLs from this email...")

            for url in email["urls"]:
                try:
                    if "simplify.jobs/p/" in url.lower():
                        actual_url, resolved = SimplifyRedirectResolver.resolve(url)
                        if resolved:
                            total_resolved += 1
                            self._process_single_url_with_extraction(
                                actual_url, email["html"], sender
                            )
                        else:
                            total_failed += 1
                    else:
                        self._process_single_url_with_extraction(
                            url, email["html"], sender
                        )
                except Exception as e:
                    self.outcomes["crashes"] = self.outcomes.get("crashes", 0) + 1
                    print(f"    ERROR: {str(e)[:50]}")
                    logging.error(f"Exception: {e}")
                    continue

            EmailTracker.mark_email_processed(
                processed_emails, email_id, subject, url_count
            )

        EmailTracker.save_processed_emails(processed_emails)

        if emails_skipped > 0:
            print(f"\n  ⊘ Skipped {emails_skipped} already-processed emails")

        if total_resolved > 0 or total_failed > 0:
            print(
                f"  Simplify totals: {total_resolved} resolved, {total_failed} failed\n"
            )

    def _process_single_url_with_extraction(self, url, email_html, sender):
        try:
            from config import PLATFORM_BLACKLIST, PLATFORM_BLACKLIST_REASONS

            url_lower = url.lower()
            for blacklisted_pattern in PLATFORM_BLACKLIST:
                if blacklisted_pattern in url_lower:
                    reason = PLATFORM_BLACKLIST_REASONS.get(
                        blacklisted_pattern,
                        f"Blacklisted platform: {blacklisted_pattern}",
                    )
                    self.outcomes["skipped_blacklisted_platform"] = (
                        self.outcomes.get("skipped_blacklisted_platform", 0) + 1
                    )
                    return
        except (ImportError, AttributeError):
            pass

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
            from extractors import JobrightRedirectResolver

            resolved_url, success = JobrightRedirectResolver.resolve(url, email_html)
            if success and resolved_url != url:
                logging.info(f"Jobright resolved: {resolved_url[:80]}")
                url = resolved_url

            soup, _ = safe_parse_html(email_html)
            if soup:
                job_data = SourceParsers.parse_jobright_email(
                    soup, url, self.jobright_auth
                )
                if job_data:
                    email_company = job_data.get("company", "Unknown")
                    email_location = job_data.get("location", "Unknown")
                    email_remote = job_data.get("remote", "Unknown")
                    email_age = job_data.get("email_age_days")
                    if email_age is not None and email_age > MAX_JOB_AGE_DAYS:
                        self.outcomes["skipped_too_old"] += 1
                        return
                    actual_url = job_data["url"]
                    if actual_url != url and "linkedin" not in actual_url.lower():
                        response, final_url, page_source = self.page_fetcher.fetch_page(
                            actual_url
                        )
                        if response and final_url:
                            soup_page, _ = safe_parse_html(response.text)
                            if soup_page:
                                platform = PlatformDetector.detect(final_url)
                                self._enhance_job_data_from_page(
                                    job_data,
                                    soup_page,
                                    final_url,
                                    platform,
                                    page_source,
                                )
                                if (
                                    job_data.get("company") == "Unknown"
                                    and email_company != "Unknown"
                                ):
                                    job_data["company"] = email_company
                                if (
                                    job_data.get("location") == "Unknown"
                                    and email_location != "Unknown"
                                ):
                                    job_data["location"] = email_location
                                if (
                                    job_data.get("remote") == "Unknown"
                                    and email_remote != "Unknown"
                                ):
                                    job_data["remote"] = email_remote
                                job_data["url"] = final_url
                                page_age = PageParser.extract_job_age_days(soup_page)
                                if page_age is not None and page_age > MAX_JOB_AGE_DAYS:
                                    self.outcomes["skipped_too_old"] += 1
                                    return
                                is_valid_season, season_reason = (
                                    TitleProcessor.check_season_requirement(
                                        job_data["title"], soup_page.get_text()[:2000]
                                    )
                                )
                                if not is_valid_season:
                                    self.outcomes["skipped_wrong_season"] += 1
                                    return self._create_discard_result(
                                        job_data, season_reason, sender
                                    )
                                decision, restriction, review_flags = (
                                    ValidationHelper.check_page_restrictions(soup_page)
                                )
                                if decision == "REJECT" and restriction:
                                    return self._create_discard_result(
                                        job_data, restriction, sender
                                    )
                                if review_flags:
                                    job_data["review_flags"] = review_flags
                                if page_age is None:
                                    if "review_flags" not in job_data:
                                        job_data["review_flags"] = []
                                    job_data["review_flags"].append("⚠️ Age unknown")
                                intl_check = LocationProcessor.check_if_international(
                                    job_data["location"],
                                    soup_page,
                                    final_url,
                                    job_data.get("title", ""),
                                )
                                if intl_check:
                                    return self._create_discard_result(
                                        job_data, intl_check, sender
                                    )
                        else:
                            if email_company != "Unknown":
                                job_data["company"] = email_company
                            if email_location != "Unknown":
                                job_data["location"] = email_location
                    result = self._validate_parsed_job(job_data, sender)
                    if result:
                        self._handle_validation_result(result, sender)
                    else:
                        print(
                            f"    {job_data.get('company', 'Unknown')[:28]}: ⊘ Duplicate"
                        )
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

            try:
                from config import COMPANY_BLACKLIST, COMPANY_BLACKLIST_REASONS

                company_upper = company.upper()
                for blacklisted in COMPANY_BLACKLIST:
                    if blacklisted.upper() in company_upper:
                        reason = COMPANY_BLACKLIST_REASONS.get(
                            blacklisted, f"Blacklisted company: {blacklisted}"
                        )
                        print(f"    {company[:28]}: ✗ {reason}")
                        self.outcomes["skipped_blacklisted_company"] = (
                            self.outcomes.get("skipped_blacklisted_company", 0) + 1
                        )
                        return self._create_discard_result(
                            {"company": company, "title": "Unknown", "url": final_url},
                            reason,
                            sender,
                        )
            except (ImportError, AttributeError):
                pass

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
            normalized_key = URLCleaner.normalize_text(f"{company}|{title}")
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

            extracted_job_type = JobTypeExtractor.extract_all_methods(
                soup, final_url, title
            )
            page_text = soup.get_text()[:5000]
            is_intern, intern_reason = TitleProcessor.is_internship_role(
                title, extracted_job_type, page_text
            )

            if (
                isinstance(intern_reason, str)
                and "CONFLICTING SIGNALS" in intern_reason
            ):
                print(f"    {company[:28]}: ✓ {intern_reason}")

            if not is_intern:
                print(f"    {company[:28]}: ✗ {intern_reason}")
                logging.info(
                    f"REJECTED | {company} | {intern_reason} | Title: '{title}' | Source: {sender} | URL: {final_url[:60]}"
                )
                self.outcomes["skipped_senior_role"] += 1
                return self._create_discard_result(
                    {"company": company, "title": title, "url": final_url},
                    intern_reason,
                    sender,
                )
            if not TitleProcessor.is_cs_engineering_role(title):
                matched_kw = [
                    kw for kw in TECHNICAL_ROLE_KEYWORDS if kw in title.lower()
                ]
                print(f"    {company[:28]}: ✗ Non-CS")
                logging.info(
                    f"REJECTED | {company} | Non-CS | Title: '{title}' | Matched keywords: {matched_kw} | Source: {sender}"
                )
                return self._create_discard_result(
                    {"company": company, "title": title, "url": final_url},
                    "Non-CS role",
                    sender,
                )
            decision, restriction, page_flags = (
                ValidationHelper.check_page_restrictions(soup)
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
            remote = LocationProcessor.extract_remote_status_enhanced(
                soup, location_formatted, final_url
            )
            sponsorship = ValidationHelper.check_sponsorship_status(soup)
            review_flags = []
            intl_check = LocationProcessor.check_if_international(
                location_formatted, soup, final_url, title
            )
            if intl_check:
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
        extracted_job_type = job_data.get("job_type", "")
        is_intern, intern_reason = TitleProcessor.is_internship_role(
            title, extracted_job_type, ""
        )

        if isinstance(intern_reason, str) and "CONFLICTING SIGNALS" in intern_reason:
            if "review_flags" not in job_data:
                job_data["review_flags"] = []
            if isinstance(job_data["review_flags"], list):
                job_data["review_flags"].append(intern_reason)

        if not is_intern:
            logging.info(
                f"REJECTED | {job_data['company']} | {intern_reason} | Title: '{title}' | Source: {sender} | URL: {job_data.get('url', '')[:60]}"
            )
            self.outcomes["skipped_senior_role"] += 1
            return self._create_discard_result(job_data, intern_reason, sender)
        if not TitleProcessor.is_cs_engineering_role(title):
            matched_kw = [kw for kw in TECHNICAL_ROLE_KEYWORDS if kw in title.lower()]
            logging.info(
                f"REJECTED | {job_data['company']} | Non-CS | Title: '{title}' | Matched keywords: {matched_kw} | Source: {sender}"
            )
            return self._create_discard_result(job_data, "Non-CS role", sender)
        is_valid_co, fixed_co, co_reason = ValidationHelper.validate_company_field(
            job_data["company"], title, job_data["url"]
        )
        if not is_valid_co:
            return self._create_discard_result(job_data, co_reason, sender)
        job_data["company"] = fixed_co
        normalized_key = URLCleaner.normalize_text(f"{fixed_co}|{title}")
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
        page_remote = LocationProcessor.extract_remote_status_enhanced(
            soup, page_location_formatted, final_url
        )
        page_job_id = JobIDExtractor.extract_all_methods(final_url, soup, platform)
        page_company = CompanyExtractor.extract_all_methods(final_url, soup)
        page_job_type = JobTypeExtractor.extract_all_methods(
            soup, final_url, job_data.get("title", "")
        )

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
        if page_job_type != "Unknown":
            job_data["job_type"] = page_job_type

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
        key = URLCleaner.normalize_text(f"{company}|{title}")
        self.existing_jobs.add(key)
        self.existing_urls.add(URLCleaner.clean_url(url))
        if job_id != "N/A" and not job_id.startswith("HASH_"):
            self.existing_job_ids.add(job_id.lower())

    def _is_duplicate(self, company, title, url, job_id="N/A"):
        self.jobs_processed_count += 1

        if self.jobs_processed_count % 100 == 0:
            self._reload_existing_data()

        cleaned_url = URLCleaner.clean_url(url)
        company_title_key = URLCleaner.normalize_text(f"{company}|{title}")

        if cleaned_url in self.existing_urls:
            return True

        if company_title_key in self.existing_company_titles:
            return True

        if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
            if job_id.lower() in self.existing_job_ids:
                return True

        if cleaned_url in self.processing_lock:
            return True

        return False

    def _reload_existing_data(self):
        try:
            new_urls = self.sheets.load_urls_only()
            new_company_titles = self.sheets.load_company_titles_only()
            new_job_ids = self.sheets.load_job_ids_only()

            self.existing_urls.update(new_urls)
            self.existing_company_titles.update(new_company_titles)
            self.existing_job_ids.update(new_job_ids)
            self.existing_jobs = self.existing_company_titles

            logging.debug(f"Reloaded existing data at job #{self.jobs_processed_count}")
        except Exception as e:
            logging.debug(f"Mid-run reload failed: {e}")

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
        simplify_valid = sum(
            1 for j in self.valid_jobs if j.get("source") == "SimplifyJobs"
        )
        vanshb03_valid = sum(
            1 for j in self.valid_jobs if j.get("source") == "vanshb03"
        )
        email_valid = sum(
            1
            for j in self.valid_jobs
            if j.get("source") not in ["SimplifyJobs", "vanshb03"]
        )

        simplify_discarded = sum(
            1 for j in self.discarded_jobs if j.get("source") == "SimplifyJobs"
        )
        vanshb03_discarded = sum(
            1 for j in self.discarded_jobs if j.get("source") == "vanshb03"
        )
        email_discarded = sum(
            1
            for j in self.discarded_jobs
            if j.get("source") not in ["SimplifyJobs", "vanshb03"]
        )

        simplify_dups = self.outcomes.get("simplify_duplicates", 0)
        vanshb03_dups = self.outcomes.get("vanshb03_duplicates", 0)
        email_dups = self.outcomes.get("email_duplicates", 0)

        total_github_valid = simplify_valid + vanshb03_valid
        total_github_discarded = simplify_discarded + vanshb03_discarded
        total_github_dups = simplify_dups + vanshb03_dups

        crashes = self.outcomes.get("crashes", 0)

        print("\n" + "=" * 80)
        print("GITHUB BREAKDOWN:")
        print("=" * 80)
        if simplify_valid + simplify_discarded + simplify_dups > 0:
            print(f"  SimplifyJobs:")
            print(f"    ✓ Valid: {simplify_valid}")
            print(f"    ✗ Discarded: {simplify_discarded}")
            print(f"    ⊘ Duplicates: {simplify_dups}")

        if vanshb03_valid + vanshb03_discarded + vanshb03_dups > 0:
            print(f"  vanshb03:")
            print(f"    ✓ Valid: {vanshb03_valid}")
            print(f"    ✗ Discarded: {vanshb03_discarded}")
            print(f"    ⊘ Duplicates: {vanshb03_dups}")

        print(
            f"  Total GitHub: {total_github_valid} valid, {total_github_discarded} discarded, {total_github_dups} duplicates"
        )

        print("\n" + "=" * 80)
        print("EMAIL BREAKDOWN:")
        print("=" * 80)
        print(f"  ✓ Valid: {email_valid}")
        print(f"  ✗ Discarded: {email_discarded}")
        print(f"  ⊘ Duplicates: {email_dups}")
        if crashes > 0:
            print(f"  ✗ Crashed: {crashes} jobs (see log for details)")

        print("\n" + "=" * 80)
        print("OVERALL SUMMARY:")
        print("=" * 80)
        print(f"  ✓ Valid: {self.outcomes['valid']}")
        print(f"  ✗ Discarded: {self.outcomes['discarded']}")
        print(f"  ⊘ Duplicate URL: {self.outcomes['skipped_duplicate_url']}")
        print(f"  ⊘ Duplicate job: {self.outcomes['skipped_duplicate_company_title']}")
        if crashes > 0:
            print(f"  ✗ Crashed: {crashes}")
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
