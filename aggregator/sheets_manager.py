#!/usr/bin/env python3

import gspread
import time
import re
from functools import lru_cache
from oauth2client.service_account import ServiceAccountCredentials

from aggregator.config import (
    SHEET_NAME,
    WORKSHEET_NAME,
    DISCARDED_WORKSHEET,
    REVIEWED_WORKSHEET,
    SHEETS_CREDS_FILE,
    STATUS_COLORS,
)


class SheetsManager:
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
            except:
                sheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, rows=1000, cols=len(headers)
                )
                sheet.append_row(headers)
                self._format_headers(sheet, len(headers))

            setattr(self, sheet_name.lower().replace(" ", "_").replace("-", "_"), sheet)

    def load_existing_jobs(self):
        existing = {"jobs": set(), "urls": set(), "job_ids": set(), "cache": {}}

        for sheet in [
            self.valid_sheet,
            self.discarded_entries,
            self.reviewed___not_applied,
        ]:
            for row in sheet.get_all_values()[1:]:
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

        from aggregator.config import SHOW_LOADING_STATS

        if SHOW_LOADING_STATS:
            print(
                f"Loaded: {len(existing['jobs'])} jobs, {len(existing['urls'])} URLs, {len(existing['job_ids'])} IDs"
            )
        return existing

    def load_urls_only(self):
        urls = set()
        for sheet in [
            self.valid_sheet,
            self.discarded_entries,
            self.reviewed___not_applied,
        ]:
            for row in sheet.get_all_values()[1:]:
                if len(row) > 5:
                    url = row[5].strip()
                    if url and "http" in url:
                        urls.add(self._clean_url(url))
        return urls

    def load_company_titles_only(self):
        company_titles = set()
        for sheet in [
            self.valid_sheet,
            self.discarded_entries,
            self.reviewed___not_applied,
        ]:
            for row in sheet.get_all_values()[1:]:
                if len(row) > 3:
                    company = row[2].strip()
                    title = row[3].strip()
                    if company and title:
                        key = self._normalize(f"{company}|{title}")
                        company_titles.add(key)
        return company_titles

    def load_job_ids_only(self):
        job_ids = set()
        for sheet in [
            self.valid_sheet,
            self.discarded_entries,
            self.reviewed___not_applied,
        ]:
            for row in sheet.get_all_values()[1:]:
                if len(row) > 6:
                    job_id = row[6].strip()
                    if (
                        job_id
                        and job_id not in ["N/A", ""]
                        and not job_id.startswith("HASH_")
                    ):
                        job_ids.add(job_id.lower())
        return job_ids

    def get_next_row_numbers(self):
        return {
            "valid": self._find_next_row(self.valid_sheet)["row"],
            "valid_sr_no": self._find_next_row(self.valid_sheet)["sr_no"],
            "discarded": self._find_next_row(self.discarded_entries)["row"],
            "discarded_sr_no": self._find_next_row(self.discarded_entries)["sr_no"],
        }

    def _find_next_row(self, sheet):
        data = sheet.get_all_values()
        for idx, row in enumerate(data[1:], start=2):
            if len(row) <= 2 or not (row[2].strip() or row[3].strip()):
                return {"row": idx, "sr_no": idx - 1}
        return {"row": len(data) + 1, "sr_no": len(data)}

    def ensure_sufficient_rows(self, sheet, min_available=250, add_count=1000):
        """
        NEW: Ensure sheet has sufficient empty rows before batch operations
        If available rows < min_available, expand sheet by add_count rows
        """
        try:
            import time

            current_total_rows = sheet.row_count

            all_data = sheet.get_all_values()
            used_rows = len(all_data)

            available_rows = current_total_rows - used_rows

            if available_rows < min_available:
                new_total = current_total_rows + add_count

                print(
                    f"  Expanding {sheet.title}: {current_total_rows} → {new_total} rows ({available_rows} available < {min_available} threshold)"
                )

                sheet.resize(rows=new_total)
                time.sleep(2)

                print(
                    f"  ✓ {sheet.title} now has {add_count + available_rows} available rows"
                )
            else:
                pass

        except Exception as e:
            print(f"  Warning: Could not check/expand {sheet.title}: {e}")

    def add_valid_jobs(self, jobs, start_row, start_sr_no):
        if not jobs:
            return 0

        self.ensure_sufficient_rows(self.valid_sheet)

        from aggregator.utils import DataSanitizer

        sanitized_jobs = [DataSanitizer.sanitize_all_fields(job) for job in jobs]

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
                self._classify_resume(job["title"]),
                job["remote"],
                job["entry_date"],
                job["source"],
                job.get("sponsorship", "Unknown"),
            ]
            for idx, job in enumerate(sanitized_jobs)
        ]

        self._batch_write(self.valid_sheet, start_row, rows, is_valid_sheet=True)
        self._auto_resize_columns(self.valid_sheet, 14)
        return len(jobs)

    def add_discarded_jobs(self, jobs, start_row, start_sr_no):
        if not jobs:
            return 0

        self.ensure_sufficient_rows(self.discarded_entries)

        from aggregator.utils import DataSanitizer

        sanitized_jobs = [DataSanitizer.sanitize_all_fields(job) for job in jobs]

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
            for idx, job in enumerate(sanitized_jobs)
        ]

        end_row = start_row + len(rows) - 1
        self.discarded_entries.update(
            values=rows,
            range_name=f"A{start_row}:M{end_row}",
            value_input_option="RAW",
        )
        import time; time.sleep(1)
        self.discarded_entries.format(
            f"A{start_row}:M{end_row}",
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        import time; time.sleep(1)
        self._auto_resize_columns(self.discarded_entries, 13)
        return len(jobs)

    def _batch_write(self, sheet, start_row, rows_data, is_valid_sheet):
        if not rows_data:
            return

        end_row = start_row + len(rows_data) - 1
        sheet.update(
            values=rows_data,
            range_name=f"A{start_row}:N{end_row}",
            value_input_option="RAW",
        )
        time.sleep(1)

        sheet.format(
            f"A{start_row}:N{end_row}",
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(1)

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
            for i in range(0, len(url_requests), 100):
                self.spreadsheet.batch_update({"requests": url_requests[i : i + 100]})
                time.sleep(1)

        if is_valid_sheet:
            self._add_status_dropdowns(sheet, start_row, len(rows_data))
            self._add_resume_dropdowns(sheet, start_row, len(rows_data))
            self._apply_status_colors(sheet, start_row, end_row)

    def _add_resume_dropdowns(self, sheet, start_row, num_rows):
        reqs = [{
            "setDataValidation": {
                "range": {"sheetId": sheet.id, "startRowIndex": start_row+i-1, "endRowIndex": start_row+i,
                          "startColumnIndex": 9, "endColumnIndex": 10},
                "rule": {"condition": {"type": "ONE_OF_LIST",
                         "values": [{"userEnteredValue": "SDE"}, {"userEnteredValue": "ML"}]},
                         "showCustomUi": True, "strict": False},
            }
        } for i in range(num_rows)]
        if reqs:
            for i in range(0, len(reqs), 100):
                self.spreadsheet.batch_update({"requests": reqs[i:i+100]})
                import time; time.sleep(1)

    def _add_status_dropdowns(self, sheet, start_row, num_rows):
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
                self.spreadsheet.batch_update({"requests": requests[i : i + 100]})
                time.sleep(1)

    def _apply_status_colors(self, sheet, start_row, end_row):
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

            if color_requests:
                for i in range(0, len(color_requests), 100):
                    self.spreadsheet.batch_update(
                        {"requests": color_requests[i : i + 100]}
                    )
                    time.sleep(1)

        except Exception as e:
            print(f"Color application error: {e}")

    def _auto_resize_columns(self, sheet, total_columns):
        try:
            all_data = sheet.get_all_values()
            if len(all_data) < 2:
                return

            column_limits = {
                0: 80,
                1: 400,
                2: 350,
                3: 500,
                4: 150,
                5: 115,
                6: 120,
                7: 120,
                8: 220,
                9: 80,
                10: 100,
                11: 190,
                12: 130,
                13: 150,
            }

            widths = []
            for col_idx in range(total_columns):
                max_width = 50

                if len(all_data[0]) > col_idx:
                    max_width = max(max_width, len(str(all_data[0][col_idx])) * 10 + 40)

                for row in all_data[1:]:
                    if len(row) > col_idx and row[col_idx]:
                        max_width = max(
                            max_width, len(str(row[col_idx]).strip()) * 8 + 25
                        )

                if col_idx in column_limits:
                    max_width = (
                        max(95, min(max_width, column_limits[col_idx]))
                        if col_idx == 5
                        else min(max_width, column_limits[col_idx])
                    )

                widths.append(int(max_width))

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
                self.spreadsheet.batch_update({"requests": requests[i : i + 100]})
                time.sleep(1)

        except Exception as e:
            print(f"Column resize error: {e}")

    def _format_headers(self, sheet, num_cols):
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
    @lru_cache(maxsize=2048)
    def _normalize(text):
        return re.sub(r"[^a-z0-9]", "", text.lower()) if text else ""

    @staticmethod
    @lru_cache(maxsize=2048)
    def _clean_url(url):
        if not url:
            return ""
        if "jobright.ai/jobs/info/" in url.lower():
            match = re.search(r"(jobright\.ai/jobs/info/[a-f0-9]+)", url, re.I)
            if match:
                return match.group(1).lower()
        return re.sub(r"[?#].*$", "", url).lower().rstrip("/")
