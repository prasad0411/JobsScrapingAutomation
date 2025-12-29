#!/usr/bin/env python3
# cSpell:disable
"""
Google Sheets management module.
Handles all sheet operations: reading, writing, formatting, coloring.
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
            print("No new valid jobs to add")
            return 0

        print(f"Adding {len(jobs)} valid jobs")

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

        self._auto_resize_columns(self.valid_sheet, 5, 13)
        print(f"Added {len(jobs)} valid jobs")
        return len(jobs)

    def add_discarded_jobs(self, jobs, start_row, start_sr_no):
        """Add discarded jobs to sheet."""
        if not jobs:
            print("No new discarded jobs to add")
            return 0

        print(f"Adding {len(jobs)} discarded jobs")

        for i in range(0, len(jobs), 10):
            batch = jobs[i : i + 10]
            rows = []

            for idx, job in enumerate(batch):
                sr_no = start_discarded_sr = start_sr_no + i + idx
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

        self._auto_resize_columns(self.discarded_sheet, 5, 13)
        print(f"Added {len(jobs)} discarded jobs")
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

    def _auto_resize_columns(self, sheet, url_column_index, total_columns):
        """Auto-resize columns with smart width calculation."""
        try:
            # Auto-resize all
            self.spreadsheet.batch_update(
                {
                    "requests": [
                        {
                            "autoResizeDimensions": {
                                "dimensions": {
                                    "sheetId": sheet.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 0,
                                    "endIndex": total_columns,
                                }
                            }
                        }
                    ]
                }
            )
            time.sleep(1)

            # Calculate dynamic status column width
            all_data = sheet.get_all_values()
            max_status_width = 100
            for row in all_data[1:]:
                if len(row) > 1:
                    text_width = len(row[1].strip()) * 8 + 20
                    max_status_width = max(max_status_width, text_width)
            max_status_width = min(max_status_width, 250)

            # Fixed widths
            fixed_widths = [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 1,
                            "endIndex": 2,
                        },
                        "properties": {"pixelSize": max_status_width},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 4,
                            "endIndex": 5,
                        },
                        "properties": {"pixelSize": 150},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": url_column_index,
                            "endIndex": url_column_index + 1,
                        },
                        "properties": {"pixelSize": 100},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": 12,
                            "endIndex": 13,
                        },
                        "properties": {"pixelSize": 110},
                        "fields": "pixelSize",
                    }
                },
            ]

            self.spreadsheet.batch_update({"requests": fixed_widths})
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
