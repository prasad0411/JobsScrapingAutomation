#!/usr/bin/env python3
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
    """Manage Google Sheets operations"""

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
        """Initialize discarded and reviewed sheets if they don't exist"""
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
        """
        Load existing jobs from all sheets
        Returns: dict with jobs, urls, job_ids, and cache
        """
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

                if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
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

                if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
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

                if job_id and job_id != "N/A" and not job_id.startswith("HASH_"):
                    existing["job_ids"].add(job_id.lower())

        print(
            f"Loaded: {len(existing['jobs'])} jobs, "
            f"{len(existing['urls'])} URLs, "
            f"{len(existing['job_ids'])} IDs"
        )

        return existing

    def get_next_row_numbers(self):
        """
        Get next row numbers for inserting data
        Returns: dict with valid and discarded row numbers
        """
        valid_data = self.valid_sheet.get_all_values()
        discarded_data = self.discarded_sheet.get_all_values()

        # Find next valid row
        next_valid_row = 2
        next_valid_sr = 1
        for idx, row in enumerate(valid_data[1:], start=2):
            if len(row) > 2 and (row[2].strip() or row[3].strip()):
                next_valid_row = idx + 1
                next_valid_sr = idx

        # Find next discarded row
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
        """Add valid jobs to sheet"""
        if not jobs:
            return 0

        # Process in batches of 10
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

        # Auto-resize columns
        self._auto_resize_all_columns_dynamic(self.valid_sheet, 13)

        return len(jobs)

    def add_discarded_jobs(self, jobs, start_row, start_sr_no):
        """Add discarded jobs to sheet"""
        if not jobs:
            return 0

        # Process in batches of 10
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

        # Auto-resize columns
        self._auto_resize_all_columns_dynamic(self.discarded_sheet, 13)

        return len(jobs)

    def _batch_update_with_formatting(
        self, sheet, start_row, rows_data, is_valid_sheet
    ):
        """Update sheet with formatting"""
        if not rows_data:
            return

        end_col = "M"
        range_name = f"A{start_row}:{end_col}{start_row + len(rows_data) - 1}"

        # Update values
        sheet.update(values=rows_data, range_name=range_name, value_input_option="RAW")
        time.sleep(2)

        # Apply formatting
        sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(2)

        # Make URLs clickable
        url_requests = []
        for idx, row_data in enumerate(rows_data):
            url = row_data[5]  # URL is in column F (index 5)
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

        # Add status dropdowns and colors for valid sheet
        if is_valid_sheet:
            self._add_status_dropdowns(sheet, start_row, len(rows_data))
            self._apply_status_colors(sheet, start_row, start_row + len(rows_data))

    def _add_status_dropdowns(self, sheet, start_row, num_rows):
        """Add status dropdown validation"""
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
        """Apply status-based colors to cells"""
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
                                        "verticalAlignment": "MIDDLE",
                                    }
                                },
                                "fields": "userEnteredFormat",
                            }
                        }
                    )

            # Apply in batches
            if color_requests:
                for i in range(0, len(color_requests), 20):
                    batch = color_requests[i : i + 20]
                    self.spreadsheet.batch_update({"requests": batch})
                    time.sleep(1)

        except Exception as e:
            print(f"Color application error: {e}")

    def _auto_resize_all_columns_dynamic(self, sheet, total_columns):
        """Auto-resize columns based on content"""
        try:
            all_data = sheet.get_all_values()
            if len(all_data) < 2:
                return

            column_widths = []

            for col_idx in range(total_columns):
                max_width = 50

                # Check header width
                if len(all_data[0]) > col_idx:
                    header_text = str(all_data[0][col_idx])
                    header_width = len(header_text) * 10 + 40
                    max_width = max(max_width, header_width)

                # Check cell widths
                for row in all_data[1:]:
                    if len(row) > col_idx:
                        cell_text = str(row[col_idx]).strip()
                        if cell_text:
                            text_width = len(cell_text) * 8 + 25
                            max_width = max(max_width, text_width)

                # Apply column-specific limits
                column_limits = {
                    0: 80,  # Sr. No.
                    1: 400,  # Status/Reason
                    2: 350,  # Company
                    3: 500,  # Title
                    4: 150,  # Date Applied
                    5: 115,  # URL
                    6: 120,  # Job ID
                    7: 120,  # Job Type
                    8: 220,  # Location
                    9: 100,  # Remote
                    10: 190,  # Entry Date
                    11: 130,  # Source
                    12: 150,  # Sponsorship
                }

                if col_idx in column_limits:
                    if col_idx == 5:  # URL column
                        max_width = max(95, min(max_width, column_limits[col_idx]))
                    else:
                        max_width = min(max_width, column_limits[col_idx])

                column_widths.append(max_width)

            # Create resize requests
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

            # Apply in batches
            for i in range(0, len(resize_requests), 100):
                batch = resize_requests[i : i + 100]
                self.spreadsheet.batch_update({"requests": batch})
                time.sleep(1)

        except Exception as e:
            print(f"Column resize error: {e}")

    def _format_headers(self, sheet, num_cols):
        """Format header row"""
        try:
            col_letter = chr(ord("A") + num_cols - 1)
            sheet.format(
                f"A1:{col_letter}1",
                {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
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
        """Normalize text for comparison"""
        if not text:
            return ""
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @staticmethod
    def _clean_url(url):
        """Clean URL for comparison"""
        if not url:
            return ""

        # Special handling for Jobright
        if "jobright.ai/jobs/info/" in url.lower():
            match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
            if match:
                return match.group(1).lower()

        # Remove query params and fragments
        url = re.sub(r"\?.*$", "", url)
        url = re.sub(r"#.*$", "", url)
        return url.lower().rstrip("/")
