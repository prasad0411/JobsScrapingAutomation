#!/usr/bin/env python3
"""
Discarded Sheet Auditor — runs every 3 days.
Scans discarded entries for false positives and fixes them:
1. Trusted companies wrongly rejected for clearance
2. Valid CS jobs wrongly marked as non-tech
3. US locations wrongly flagged as international
4. Jobs from whitelisted companies wrongly rejected
5. Duplicate discarded entries cleanup
"""

import gspread
import logging
import re
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

CREDS_FILE = ".local/credentials.json"
SHEET_NAME = "H1B visa"

# Companies that should NEVER be rejected for clearance
NO_CLEARANCE_COMPANIES = {
    "apple", "google", "meta", "amazon", "microsoft", "netflix",
    "uber", "lyft", "stripe", "airbnb", "spotify", "pinterest",
    "tesla", "nvidia", "tiktok", "bytedance", "salesforce", "slack",
    "snap", "reddit", "dropbox", "coinbase", "robinhood", "doordash",
    "instacart", "databricks", "snowflake", "palantir", "figma",
    "rivian", "lucid", "waymo", "cruise", "nuro", "zoox", "aurora",
    "openai", "anthropic", "cerebras", "groq", "ramp", "brex",
    "notion", "airtable", "asana", "canva", "miro", "vercel",
    "mongodb", "elastic", "confluent", "datadog", "cloudflare",
    "hubspot", "twilio", "okta", "crowdstrike", "sentinelone",
    "discord", "toast", "squarespace", "plaid", "affirm", "chime",
}

# US state abbreviations — for catching wrongly flagged Indiana/Milwaukee
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

# CS job title keywords
CS_TITLE_KEYWORDS = {
    "software", "data", "machine learning", "ml", "ai", "backend",
    "frontend", "full stack", "devops", "cloud", "sre", "platform",
    "infrastructure", "security", "cyber", "research", "algorithm",
}


class DiscardedAuditor:
    def __init__(self):
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        self.ss = gc.open(SHEET_NAME)
        self.discarded = self.ss.worksheet("Discarded Entries")
        self.valid = self.ss.worksheet("Valid Entries")

    def audit(self):
        log.info("Starting discarded sheet audit")
        data = self.discarded.get_all_values()
        log.info(f"Discarded entries: {len(data)-1}")

        rescued = []
        duplicates = set()
        seen_keys = set()

        for i, row in enumerate(data[1:], start=2):
            if len(row) < 4:
                continue
            reason = row[1].strip() if len(row) > 1 else ""
            company = row[2].strip() if len(row) > 2 else ""
            title = row[3].strip() if len(row) > 3 else ""
            location = row[8].strip() if len(row) > 8 else ""
            co_lower = company.lower().strip()

            # Dedup check within discarded
            key = re.sub(r"[^a-z0-9]", "", f"{company}_{title}".lower())
            if key in seen_keys:
                duplicates.add(i)
            else:
                seen_keys.add(key)

            # CHECK 1: Clearance false positive for trusted companies
            if "clearance" in reason.lower() or "security" in reason.lower():
                if any(tc in co_lower or co_lower in tc for tc in NO_CLEARANCE_COMPANIES):
                    rescued.append((i, row, f"False clearance: {company} is trusted"))
                    continue

            # CHECK 2: International false positive for US locations
            if "international" in reason.lower() or "canada" in reason.lower():
                if location:
                    # Check if location ends with US state code
                    loc_parts = location.split(",")
                    if len(loc_parts) >= 2:
                        state = loc_parts[-1].strip().upper()[:2]
                        if state in US_STATES:
                            rescued.append((i, row, f"False international: {location} is US"))
                            continue
                    # Check Indiana/Milwaukee type false positives
                    us_cities = ["indianapolis", "milwaukee", "fort wayne", "noblesville",
                                 "waukesha", "crane", "bloomington", "columbus"]
                    if any(city in location.lower() for city in us_cities):
                        rescued.append((i, row, f"False international: {location} is US city"))
                        continue

            # CHECK 3: Valid CS title wrongly rejected as non-tech
            if "not a cs" in reason.lower() or "non-tech" in reason.lower():
                if any(kw in title.lower() for kw in CS_TITLE_KEYWORDS):
                    rescued.append((i, row, f"False non-tech: '{title}' contains CS keywords"))
                    continue

            # CHECK 4: Undergrad-only false positive for MS-eligible roles
            if "undergraduate" in reason.lower() and "ms" in reason.lower():
                # Check if JD actually says "bachelor's or master's"
                # Can't re-fetch JD here, but flag if title contains "new grad"
                if "new grad" in title.lower() or "entry level" in title.lower():
                    rescued.append((i, row, f"Possible false undergrad: {title} may accept MS"))
                    continue

        log.info(f"Rescued: {len(rescued)} false positives")
        log.info(f"Duplicates in discarded: {len(duplicates)}")

        # Delete duplicates from bottom up
        if duplicates:
            for row_num in sorted(duplicates, reverse=True):
                try:
                    self.discarded.delete_rows(row_num)
                    time.sleep(0.4)
                except Exception:
                    pass
            log.info(f"Deleted {len(duplicates)} duplicate discarded entries")

        # Move rescued jobs to Valid Entries
        if rescued:
            valid_data = self.valid.get_all_values()
            next_sr = len(valid_data)

            for row_num, row_data, rescue_reason in rescued:
                log.info(f"  RESCUED: {row_data[2]:25s} | {row_data[3][:35]:35s} | {rescue_reason}")

            # Add to valid sheet
            rows_to_add = []
            for _, row_data, _ in rescued:
                next_sr += 1
                company = row_data[2] if len(row_data) > 2 else ""
                title = row_data[3] if len(row_data) > 3 else ""
                url = row_data[5] if len(row_data) > 5 else ""
                job_id = row_data[6] if len(row_data) > 6 else "N/A"
                job_type = row_data[7] if len(row_data) > 7 else "Internship"
                location = row_data[8] if len(row_data) > 8 else "Unknown"
                resume = row_data[9] if len(row_data) > 9 else "SDE"
                remote = row_data[10] if len(row_data) > 10 else "Unknown"
                source = row_data[12] if len(row_data) > 12 else "Rescued"

                new_row = [str(next_sr), "Not Applied", company, title, "N/A",
                          url, job_id, job_type, location, resume, remote,
                          self._format_date(), source, "Unknown"]
                rows_to_add.append(new_row)

            if rows_to_add:
                self.valid.append_rows(rows_to_add, value_input_option="USER_ENTERED")
                log.info(f"Added {len(rows_to_add)} rescued jobs to Valid Entries")

            # Delete rescued rows from discarded (bottom up)
            rescued_rows = sorted([r[0] for r in rescued], reverse=True)
            for row_num in rescued_rows:
                try:
                    self.discarded.delete_rows(row_num)
                    time.sleep(0.4)
                except Exception:
                    pass

        log.info("Audit complete")

    def _format_date(self):
        return datetime.now().strftime("%d %b, %I:%M %p")


def main():
    auditor = DiscardedAuditor()
    auditor.audit()


if __name__ == "__main__":
    main()
