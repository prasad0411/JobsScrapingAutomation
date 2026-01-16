#!/usr/bin/env python3

import gspread
import time
import re
import logging
from functools import lru_cache
from oauth2client.service_account import ServiceAccountCredentials

from config import (
    SHEET_NAME,
    WORKSHEET_NAME,
    DISCARDED_WORKSHEET,
    REVIEWED_WORKSHEET,
    SHEETS_CREDS_FILE,
    STATUS_COLORS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
)

# ============================================================================
# Sheets Manager with Robust Error Handling
# ============================================================================


class SheetsManager:
    def __init__(self):
        try:
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
            logging.info("Successfully initialized Google Sheets connection")
        except Exception as e:
            logging.error(f"Failed to initialize Sheets: {e}", exc_info=True)
            raise

    def _initialize_sheets(self):
        sheet_configs = {
            DISCARDED_WORKSHEET: [
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
            ],
            REVIEWED_WORKSHEET: [
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
            ],
        }

        for sheet_name, headers in sheet_configs.items():
            try:
                sheet = self.spreadsheet.worksheet(sheet_name)
                logging.info(f"Found existing sheet: {sheet_name}")
            except:
                logging.info(f"Creating new sheet: {sheet_name}")
                sheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, rows=1000, cols=len(headers)
                )
                self._retry_operation(
                    lambda: sheet.append_row(headers), f"append headers to {sheet_name}"
                )
                self._format_headers(sheet, len(headers))

            setattr(self, sheet_name.lower().replace(" ", "_").replace("-", "_"), sheet)

    def load_existing_jobs(self):
        existing = {"jobs": set(), "urls": set(), "job_ids": set(), "cache": {}}

        for sheet_name, sheet_attr in [
            ("Valid Entries", "valid_sheet"),
            ("Discarded Entries", "discarded_entries"),
            ("Reviewed - Not Applied", "reviewed___not_applied"),
        ]:
            try:
                sheet = getattr(self, sheet_attr)
                all_values = self._retry_operation(
                    lambda: sheet.get_all_values(), f"load data from {sheet_name}"
                )

                for row in all_values[1:]:  # Skip header
                    if len(row) <= 5:
                        continue

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

                    if (
                        job_id
                        and job_id not in ["N/A", ""]
                        and not job_id.startswith("HASH_")
                    ):
                        existing["job_ids"].add(job_id.lower())

                logging.info(f"Loaded {len(all_values)-1} rows from {sheet_name}")

            except Exception as e:
                logging.error(f"Failed to load {sheet_name}: {e}", exc_info=True)
                continue

        print(
            f"Loaded: {len(existing['jobs'])} jobs, {len(existing['urls'])} URLs, {len(existing['job_ids'])} IDs"
        )
        return existing

    def get_next_row_numbers(self):
        return {
            "valid": self._find_next_row(self.valid_sheet)["row"],
            "valid_sr_no": self._find_next_row(self.valid_sheet)["sr_no"],
            "discarded": self._find_next_row(self.discarded_entries)["row"],
            "discarded_sr_no": self._find_next_row(self.discarded_entries)["sr_no"],
        }

    def _find_next_row(self, sheet):
        try:
            data = self._retry_operation(
                lambda: sheet.get_all_values(), "find next row"
            )

            for idx, row in enumerate(data[1:], start=2):
                if len(row) <= 2 or not (row[2].strip() or row[3].strip()):
                    return {"row": idx, "sr_no": idx - 1}
            return {"row": len(data) + 1, "sr_no": len(data)}

        except Exception as e:
            logging.error(f"Failed to find next row: {e}")
            return {"row": 2, "sr_no": 1}  # Safe default

    def add_valid_jobs(self, jobs, start_row, start_sr_no):
        if not jobs:
            return 0

        try:
            rows = [
                [
                    start_sr_no + idx,
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
                for idx, job in enumerate(jobs)
            ]

            self._batch_write(self.valid_sheet, start_row, rows, is_valid_sheet=True)
            self._auto_resize_columns(self.valid_sheet, 13)
            return len(jobs)

        except Exception as e:
            logging.error(f"Failed to add valid jobs: {e}", exc_info=True)
            return 0

    def add_discarded_jobs(self, jobs, start_row, start_sr_no):
        if not jobs:
            return 0

        try:
            rows = [
                [
                    start_sr_no + idx,
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
                for idx, job in enumerate(jobs)
            ]

            self._batch_write(
                self.discarded_entries, start_row, rows, is_valid_sheet=False
            )
            self._auto_resize_columns(self.discarded_entries, 13)
            return len(jobs)

        except Exception as e:
            logging.error(f"Failed to add discarded jobs: {e}", exc_info=True)
            return 0

    def _batch_write(self, sheet, start_row, rows_data, is_valid_sheet):
        if not rows_data:
            return

        try:
            end_row = start_row + len(rows_data) - 1

            # Write data with retry
            self._retry_operation(
                lambda: sheet.update(
                    values=rows_data,
                    range_name=f"A{start_row}:M{end_row}",
                    value_input_option="RAW",
                ),
                f"write {len(rows_data)} rows to sheet",
            )
            time.sleep(1)

            # Apply formatting with retry
            self._retry_operation(
                lambda: sheet.format(
                    f"A{start_row}:M{end_row}",
                    {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                    },
                ),
                "apply cell formatting",
            )
            time.sleep(1)

            # Add hyperlinks
            self._add_url_hyperlinks(sheet, start_row, rows_data)

            # Add dropdowns and colors for valid sheet
            if is_valid_sheet:
                self._add_status_dropdowns(sheet, start_row, len(rows_data))
                self._apply_status_colors(sheet, start_row, end_row)

        except Exception as e:
            logging.error(f"Batch write failed: {e}", exc_info=True)

    def _add_url_hyperlinks(self, sheet, start_row, rows_data):
        """Add clickable hyperlinks to URL column"""
        try:
            url_requests = [
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
                                        "userEnteredValue": {"stringValue": row[5]},
                                        "textFormatRuns": [
                                            {"format": {"link": {"uri": row[5]}}}
                                        ],
                                    }
                                ]
                            }
                        ],
                        "fields": "userEnteredValue,textFormatRuns",
                    }
                }
                for idx, row in enumerate(rows_data)
                if row[5] and row[5].startswith("http")
            ]

            if url_requests:
                # Batch in groups of 100 to avoid API limits
                for i in range(0, len(url_requests), 100):
                    self._retry_operation(
                        lambda batch=url_requests[
                            i : i + 100
                        ]: self.spreadsheet.batch_update({"requests": batch}),
                        f"add hyperlinks (batch {i//100 + 1})",
                    )
                    time.sleep(1)

        except Exception as e:
            logging.error(f"Failed to add hyperlinks: {e}")

    def _add_status_dropdowns(self, sheet, start_row, num_rows):
        """Add status dropdown validation to Valid Entries sheet"""
        try:
            requests = [
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
                for idx in range(num_rows)
            ]

            if requests:
                for i in range(0, len(requests), 100):
                    self._retry_operation(
                        lambda batch=requests[
                            i : i + 100
                        ]: self.spreadsheet.batch_update({"requests": batch}),
                        f"add dropdowns (batch {i//100 + 1})",
                    )
                    time.sleep(1)

        except Exception as e:
            logging.error(f"Failed to add status dropdowns: {e}")

    def _apply_status_colors(self, sheet, start_row, end_row):
        """Apply color coding based on application status"""
        try:
            all_data = self._retry_operation(
                lambda: sheet.get_all_values(), "get all values for coloring"
            )

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

            if color_requests:
                for i in range(0, len(color_requests), 100):
                    self._retry_operation(
                        lambda batch=color_requests[
                            i : i + 100
                        ]: self.spreadsheet.batch_update({"requests": batch}),
                        f"apply colors (batch {i//100 + 1})",
                    )
                    time.sleep(1)

        except Exception as e:
            logging.error(f"Color application error: {e}")

    def _auto_resize_columns(self, sheet, total_columns):
        """Auto-resize columns based on content with max width limits"""
        try:
            all_data = self._retry_operation(
                lambda: sheet.get_all_values(), "get data for column resize"
            )

            if len(all_data) < 2:
                return

            column_limits = {
                0: 80,  # Sr. No.
                1: 400,  # Status/Reason
                2: 350,  # Company
                3: 500,  # Title
                4: 150,  # Date Applied
                5: 115,  # Job URL
                6: 120,  # Job ID
                7: 120,  # Job Type
                8: 220,  # Location
                9: 100,  # Remote
                10: 190,  # Entry Date
                11: 130,  # Source
                12: 150,  # Sponsorship
            }

            widths = []
            for col_idx in range(total_columns):
                max_width = 50

                # Header width
                if len(all_data[0]) > col_idx:
                    max_width = max(max_width, len(str(all_data[0][col_idx])) * 10 + 40)

                # Data width
                for row in all_data[1:]:
                    if len(row) > col_idx and row[col_idx]:
                        max_width = max(
                            max_width, len(str(row[col_idx]).strip()) * 8 + 25
                        )

                # Apply limits
                if col_idx in column_limits:
                    max_width = (
                        max(95, min(max_width, column_limits[col_idx]))
                        if col_idx == 5
                        else min(max_width, column_limits[col_idx])
                    )

                widths.append(int(max_width))

            # Batch resize requests
            requests = [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        },
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
                for col_idx, width in enumerate(widths)
            ]

            for i in range(0, len(requests), 100):
                self._retry_operation(
                    lambda batch=requests[i : i + 100]: self.spreadsheet.batch_update(
                        {"requests": batch}
                    ),
                    f"resize columns (batch {i//100 + 1})",
                )
                time.sleep(1)

        except Exception as e:
            logging.error(f"Column resize error: {e}")

    def _format_headers(self, sheet, num_cols):
        """Format header row with background color and bold text"""
        try:
            col_letter = chr(ord("A") + num_cols - 1)
            self._retry_operation(
                lambda: sheet.format(
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
                ),
                "format headers",
            )
        except Exception as e:
            logging.error(f"Header formatting failed: {e}")

    def _retry_operation(self, operation, description, max_retries=MAX_RETRIES):
        """
        Retry Google Sheets API operations with exponential backoff.
        Handles rate limits and transient failures.
        """
        for attempt in range(max_retries):
            try:
                result = operation()
                return result

            except gspread.exceptions.APIError as e:
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY_SECONDS * (2**attempt)
                    logging.warning(
                        f"Sheets API error during '{description}' (attempt {attempt+1}/{max_retries}): {e}"
                    )
                    logging.warning(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Sheets API failed after {max_retries} retries: {e}")
                    raise

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY_SECONDS * (2**attempt)
                    logging.warning(
                        f"Error during '{description}' (attempt {attempt+1}/{max_retries}): {e}"
                    )
                    logging.warning(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(
                        f"Operation '{description}' failed after {max_retries} retries: {e}"
                    )
                    raise

    @staticmethod
    @lru_cache(maxsize=2048)
    def _normalize(text):
        try:
            return re.sub(r"[^a-z0-9]", "", text.lower()) if text else ""
        except Exception as e:
            logging.debug(f"Text normalization failed: {e}")
            return text.lower() if text else ""

    @staticmethod
    @lru_cache(maxsize=2048)
    def _clean_url(url):
        if not url:
            return ""

        try:
            if "jobright.ai/jobs/info/" in url.lower():
                match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
                if match:
                    return match.group(1).lower()
            return re.sub(r"[?#].*$", "", url).lower().rstrip("/")
        except Exception as e:
            logging.debug(f"URL cleaning failed: {e}")
            return url.lower()
