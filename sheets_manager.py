#!/usr/bin/env python3
# cSpell:disable
"""
Google Sheets management module - ENHANCED VERSION
Dynamic column widths for all columns with optimized constraints.
"""

import gspread
import time
import re
from oauth2client.service_account import ServiceAccountCredentials
from config import (
    SHEET_NAME,
    WORKSHEET_NAME,
    DISCARDED_WORKSHEET,
    REVIEWED_WORKSHEET,
    SHEETS_CREDS_FILE,
    STATUS_COLORS,
)


class SheetsManager:
    """Manages all Google Sheets operations."""

    def __init__(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SHEETS_CREDS_FILE, scope
        )
        client = gspread.authorize(creds)

        self.spreadsheet = client.open(SHEET_NAME)
        self.valid_sheet = self.spreadsheet.worksheet(WORKSHEET_NAME)
        self._initialize_sheets()

    def _initialize_sheets(self):
        """Initialize all worksheets with proper headers."""
        # Discarded sheet
        try:
            self.discarded_sheet = self.spreadsheet.worksheet(DISCARDED_WORKSHEET)
        except:
            self.discarded_sheet = self.spreadsheet.add_worksheet(
                title=DISCARDED_WORKSHEET, rows=1000, cols=13
            )
            headers = [
                "Sr. No.",
                "Discard Reason",
                "Company",
                "Title",
                "Date Applied",
                "Job URL",
                "Job ID",
                "Job Type",
                "Location",
                "Remote?",
                "Entry Date",
                "Source",
                "Sponsorship",
            ]
            self.discarded_sheet.append_row(headers)
            self._format_headers(self.discarded_sheet, 13)

        # Reviewed sheet
        try:
            self.reviewed_sheet = self.spreadsheet.worksheet(REVIEWED_WORKSHEET)
        except:
            self.reviewed_sheet = self.spreadsheet.add_worksheet(
                title=REVIEWED_WORKSHEET, rows=1000, cols=12
            )
            headers = [
                "Sr. No.",
                "Reason",
                "Company",
                "Title",
                "Job URL",
                "Job ID",
                "Job Type",
                "Location",
                "Remote?",
                "Moved Date",
                "Source",
                "Sponsorship",
            ]
            self.reviewed_sheet.append_row(headers)
            self._format_headers(self.reviewed_sheet, 12)

    def load_existing_jobs(self):
        """Load existing jobs from all sheets for deduplication."""
        existing = {"jobs": set(), "urls": set(), "job_ids": set(), "cache": {}}

        # Load from valid sheet
        for row in self.valid_sheet.get_all_values()[1:]:
            if len(row) > 5:
                company, title = row[2].strip(), row[3].strip()
                url = row[5].strip() if len(row) > 5 else ""
                job_id = row[6].strip() if len(row) > 6 else ""

                if company and title:
                    key = self._normalize(f"{company}_{title}")
                    existing["jobs"].add(key)
                    existing["cache"][key] = {
                        "company": company,
                        "title": title,
                        "job_id": job_id,
                        "url": url,
                    }

                if url and "http" in url:
                    existing["urls"].add(self._clean_url(url))
                if job_id and job_id != "N/A":
                    existing["job_ids"].add(job_id.lower())

        # Load from discarded sheet
        for row in self.discarded_sheet.get_all_values()[1:]:
            if len(row) > 5:
                company, title = row[2].strip(), row[3].strip()
                url = row[5].strip() if len(row) > 5 else ""
                job_id = row[6].strip() if len(row) > 6 else ""

                if company and title:
                    key = self._normalize(f"{company}_{title}")
                    existing["jobs"].add(key)
                    existing["cache"][key] = {
                        "company": company,
                        "title": title,
                        "job_id": job_id,
                        "url": url,
                    }

                if url and "http" in url:
                    existing["urls"].add(self._clean_url(url))
                if job_id and job_id != "N/A":
                    existing["job_ids"].add(job_id.lower())

        # Load from reviewed sheet
        for row in self.reviewed_sheet.get_all_values()[1:]:
            if len(row) > 4:
                company, title = row[2].strip(), row[3].strip()
                url = row[4].strip() if len(row) > 4 else ""
                job_id = row[5].strip() if len(row) > 5 else ""

                if company and title:
                    key = self._normalize(f"{company}_{title}")
                    existing["jobs"].add(key)
                    existing["cache"][key] = {
                        "company": company,
                        "title": title,
                        "job_id": job_id,
                        "url": url,
                    }

                if url and "http" in url:
                    existing["urls"].add(self._clean_url(url))
                if job_id and job_id != "N/A":
                    existing["job_ids"].add(job_id.lower())

        print(
            f"Loaded: {len(existing['jobs'])} jobs, {len(existing['urls'])} URLs, {len(existing['job_ids'])} IDs"
        )
        return existing

    def get_next_row_numbers(self):
        """Get next available row numbers for sheets."""
        valid_data = self.valid_sheet.get_all_values()
        discarded_data = self.discarded_sheet.get_all_values()

        # Find actual last row with data
        next_valid_row = 2
        next_valid_sr = 1
        for idx, row in enumerate(valid_data[1:], start=2):
            if len(row) > 2 and (row[2].strip() or row[3].strip()):
                next_valid_row = idx + 1
                next_valid_sr = idx

        next_discarded_row = 2
        next_discarded_sr = 1
        for idx, row in enumerate(discarded_data[1:], start=2):
            if len(row) > 2 and (row[2].strip() or row[3].strip()):
                next_discarded_row = idx + 1
                next_discarded_sr = idx

        return {
            "valid": next_valid_row,
            "valid_sr_no": next_valid_sr,
            "discarded": next_discarded_row,
            "discarded_sr_no": next_discarded_sr,
        }

    def add_valid_jobs(self, jobs, start_row, start_sr_no):
        """Add valid jobs to sheet with formatting."""
        if not jobs:
            return 0

        for i in range(0, len(jobs), 10):
            batch = jobs[i : i + 10]
            rows = []

            for idx, job in enumerate(batch):
                sr_no = start_sr_no + i + idx
                rows.append(
                    [
                        sr_no,
                        "Not Applied",
                        job["company"],
                        job["title"],
                        "N/A",
                        job["url"],
                        job["job_id"],
                        job["job_type"],
                        job["location"],
                        job["remote"],
                        job["entry_date"],
                        job["source"],
                        job.get("sponsorship", "Unknown"),
                    ]
                )

            self._batch_update_with_formatting(
                self.valid_sheet, start_row + i, rows, is_valid_sheet=True
            )
            time.sleep(3)

        self._auto_resize_all_columns_dynamic(self.valid_sheet, 13)
        return len(jobs)

    def add_discarded_jobs(self, jobs, start_row, start_sr_no):
        """Add discarded jobs to sheet."""
        if not jobs:
            return 0

        for i in range(0, len(jobs), 10):
            batch = jobs[i : i + 10]
            rows = []

            for idx, job in enumerate(batch):
                sr_no = start_sr_no + i + idx
                rows.append(
                    [
                        sr_no,
                        job.get("reason", "Filtered"),
                        job["company"],
                        job["title"],
                        "N/A",
                        job["url"],
                        job.get("job_id", "N/A"),
                        job["job_type"],
                        job["location"],
                        job["remote"],
                        job["entry_date"],
                        job["source"],
                        job.get("sponsorship", "Unknown"),
                    ]
                )

            self._batch_update_with_formatting(
                self.discarded_sheet, start_row + i, rows, is_valid_sheet=False
            )
            time.sleep(3)

        self._auto_resize_all_columns_dynamic(self.discarded_sheet, 13)
        return len(jobs)

    def _batch_update_with_formatting(
        self, sheet, start_row, rows_data, is_valid_sheet
    ):
        """Batch update with links, dropdowns, and colors."""
        if not rows_data:
            return

        # Write data
        range_name = f"A{start_row}:M{start_row + len(rows_data) - 1}"
        sheet.update(values=rows_data, range_name=range_name, value_input_option="RAW")
        time.sleep(2)

        # Format cells
        sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(2)

        # Add URL hyperlinks
        url_requests = []
        for idx, row_data in enumerate(rows_data):
            url = row_data[5]
            if url and url.startswith("http"):
                url_requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": sheet.id,
                                "startRowIndex": start_row + idx - 1,
                                "endRowIndex": start_row + idx,
                                "startColumnIndex": 5,
                                "endColumnIndex": 6,
                            },
                            "rows": [
                                {
                                    "values": [
                                        {
                                            "userEnteredValue": {"stringValue": url},
                                            "textFormatRuns": [
                                                {"format": {"link": {"uri": url}}}
                                            ],
                                        }
                                    ]
                                }
                            ],
                            "fields": "userEnteredValue,textFormatRuns",
                        }
                    }
                )

        if url_requests:
            self.spreadsheet.batch_update({"requests": url_requests})
            time.sleep(2)

        # Add dropdown and colors for valid sheet
        if is_valid_sheet:
            self._add_status_dropdowns(sheet, start_row, len(rows_data))
            self._apply_status_colors(sheet, start_row, start_row + len(rows_data))

    def _add_status_dropdowns(self, sheet, start_row, num_rows):
        """Add status dropdowns to column B."""
        dropdown_requests = []
        for idx in range(num_rows):
            dropdown_requests.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet.id,
                            "startRowIndex": start_row + idx - 1,
                            "endRowIndex": start_row + idx,
                            "startColumnIndex": 1,
                            "endColumnIndex": 2,
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [
                                    {"userEnteredValue": status}
                                    for status in STATUS_COLORS.keys()
                                ],
                            },
                            "showCustomUi": True,
                            "strict": False,
                        },
                    }
                }
            )

        if dropdown_requests:
            self.spreadsheet.batch_update({"requests": dropdown_requests})
            time.sleep(2)

    def _apply_status_colors(self, sheet, start_row, end_row):
        """Apply color coding to status column."""
        try:
            all_data = sheet.get_all_values()
            color_requests = []

            for row_idx in range(start_row - 1, min(end_row, len(all_data))):
                if row_idx < 1 or len(all_data[row_idx]) < 2:
                    continue

                status = all_data[row_idx][1].strip()
                color = STATUS_COLORS.get(status)

                if color:
                    text_color = (
                        {"red": 1.0, "green": 1.0, "blue": 1.0}
                        if status == "Offer accepted"
                        else {"red": 0.0, "green": 0.0, "blue": 0.0}
                    )

                    color_requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet.id,
                                    "startRowIndex": row_idx,
                                    "endRowIndex": row_idx + 1,
                                    "startColumnIndex": 1,
                                    "endColumnIndex": 2,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": color,
                                        "textFormat": {
                                            "foregroundColor": text_color,
                                            "fontFamily": "Times New Roman",
                                            "fontSize": 13,
                                        },
                                        "horizontalAlignment": "CENTER",
                                    }
                                },
                                "fields": "userEnteredFormat",
                            }
                        }
                    )

            if color_requests:
                for i in range(0, len(color_requests), 20):
                    batch = color_requests[i : i + 20]
                    self.spreadsheet.batch_update({"requests": batch})
                    time.sleep(1)
        except Exception as e:
            print(f"Color application error: {e}")

    def _auto_resize_all_columns_dynamic(self, sheet, total_columns):
        """
        Auto-resize ALL columns dynamically based on content.
        Company, Title, and Discard Reason columns fully dynamic.
        URL column reduced by 5 pixels.
        """
        try:
            # Get all data from sheet
            all_data = sheet.get_all_values()

            if len(all_data) < 2:  # Only headers or empty
                return

            # Calculate optimal width for each column
            column_widths = []

            for col_idx in range(total_columns):
                max_width = 50  # Minimum width

                # Check header (headers get 10px per character + extra padding)
                if len(all_data[0]) > col_idx:
                    header_text = str(all_data[0][col_idx])
                    header_width = (
                        len(header_text) * 10 + 40
                    )  # Extra padding for headers
                    max_width = max(max_width, header_width)

                # Check all data rows
                for row in all_data[1:]:
                    if len(row) > col_idx:
                        cell_text = str(row[col_idx]).strip()
                        if cell_text:
                            # Calculate pixel width (8 pixels per char + 20px padding)
                            text_width = len(cell_text) * 8 + 25
                            max_width = max(max_width, text_width)

                # Apply column-specific constraints
                if col_idx == 0:  # Sr. No.
                    max_width = min(max_width, 80)

                elif col_idx == 1:  # Status / Discard Reason / Reason - FULLY DYNAMIC
                    # Allow to expand but with reasonable max
                    max_width = min(max_width, 400)  # Increased from 300

                elif col_idx == 2:  # Company - FULLY DYNAMIC
                    # Allow to expand to fit company names
                    max_width = min(max_width, 350)  # Increased from 250

                elif col_idx == 3:  # Title - FULLY DYNAMIC
                    # Allow to expand to fit full titles
                    max_width = min(max_width, 500)  # Increased from 400

                elif col_idx == 4:  # Date Applied (or Job URL in Reviewed sheet)
                    max_width = min(max_width, 150)

                elif col_idx == 5:  # Job URL - REDUCED BY 5 PIXELS
                    max_width = max(95, min(max_width, 115))  # Was 100-120, now 95-115

                elif col_idx == 6:  # Job ID
                    max_width = min(max_width, 120)

                elif col_idx == 7:  # Job Type
                    max_width = min(max_width, 120)

                elif col_idx == 8:  # Location
                    max_width = min(
                        max_width, 220
                    )  # Slightly increased for full location names

                elif col_idx == 9:  # Remote?
                    max_width = min(max_width, 100)

                elif col_idx == 10:  # Entry Date / Moved Date
                    max_width = min(max_width, 190)  # Increased for full date format

                elif col_idx == 11:  # Source
                    max_width = min(max_width, 130)

                elif col_idx == 12:  # Sponsorship
                    max_width = min(max_width, 150)

                column_widths.append(max_width)

            # Build batch update request for all columns
            resize_requests = []

            for col_idx, width in enumerate(column_widths):
                resize_requests.append(
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheet.id,
                                "dimension": "COLUMNS",
                                "startIndex": col_idx,
                                "endIndex": col_idx + 1,
                            },
                            "properties": {"pixelSize": int(width)},
                            "fields": "pixelSize",
                        }
                    }
                )

            # Execute resize in batches (max 100 requests per batch)
            for i in range(0, len(resize_requests), 100):
                batch = resize_requests[i : i + 100]
                self.spreadsheet.batch_update({"requests": batch})
                time.sleep(1)

        except Exception as e:
            print(f"Column resize error: {e}")

    def _format_headers(self, sheet, num_cols):
        """Format header row."""
        try:
            col_letter = chr(ord("A") + num_cols - 1)
            sheet.format(
                f"A1:{col_letter}1",
                {
                    "horizontalAlignment": "CENTER",
                    "textFormat": {
                        "fontFamily": "Times New Roman",
                        "fontSize": 14,
                        "bold": True,
                    },
                    "backgroundColor": {"red": 0.7, "green": 0.9, "blue": 0.7},
                },
            )
        except:
            pass

    @staticmethod
    def _normalize(text):
        """Normalize text for comparison."""
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
