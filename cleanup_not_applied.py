#!/usr/bin/env python3
# cSpell:disable
"""
Automatic cleanup script - moves 'Not Applied' jobs to Reviewed sheet.
Production-optimized with dynamic column sizing and auto-run.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
CREDS_FILE = "credentials.json"


class ManualCleanup:
    """✅ OPTIMIZED: Auto-run cleanup with dynamic column sizing."""

    # Status color mapping
    STATUS_COLORS = {
        "Not Applied": {"red": 0.6, "green": 0.76, "blue": 1.0},
        "Applied": {"red": 0.58, "green": 0.93, "blue": 0.31},
        "Rejected": {"red": 0.97, "green": 0.42, "blue": 0.42},
        "OA Round 1": {"red": 1.0, "green": 0.95, "blue": 0.4},
        "OA Round 2": {"red": 1.0, "green": 0.95, "blue": 0.4},
        "Interview 1": {"red": 0.82, "green": 0.93, "blue": 0.94},
        "Offer accepted": {"red": 0.16, "green": 0.65, "blue": 0.27},
        "Assessment": {"red": 0.89, "green": 0.89, "blue": 0.89},
    }

    # Status dropdown values
    STATUS_VALUES = list(STATUS_COLORS.keys())

    def __init__(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)

        self.spreadsheet = client.open(SHEET_NAME)
        self.sheet = self.spreadsheet.worksheet(WORKSHEET_NAME)
        self._init_reviewed_sheet()

    def _init_reviewed_sheet(self):
        """Initialize Reviewed sheet with proper headers."""
        try:
            self.reviewed_sheet = self.spreadsheet.worksheet(REVIEWED_WORKSHEET)

            # Ensure 12 columns
            current_cols = len(self.reviewed_sheet.row_values(1))
            if current_cols < 12:
                self.reviewed_sheet.resize(rows=1000, cols=12)
                time.sleep(2)

            # Ensure Sponsorship header exists
            headers = self.reviewed_sheet.row_values(1)
            if "Sponsorship" not in headers:
                self.reviewed_sheet.update_cell(1, 12, "Sponsorship")
                time.sleep(1)
                self._format_headers(self.reviewed_sheet)

        except gspread.exceptions.WorksheetNotFound:
            # Create new sheet
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
            self._format_headers(self.reviewed_sheet)

    def _format_headers(self, sheet):
        """Format header row."""
        try:
            sheet.format(
                "A1:L1",
                {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {
                        "fontFamily": "Times New Roman",
                        "fontSize": 14,
                        "bold": True,
                        "foregroundColor": {"red": 0, "green": 0, "blue": 0},
                    },
                    "backgroundColor": {"red": 0.7, "green": 0.9, "blue": 0.7},
                },
            )
        except:
            pass

    def cleanup(self):
        """✅ OPTIMIZED: Auto-run cleanup with minimal logging."""
        print("=" * 80)
        print("CLEANUP: Moving 'Not Applied' jobs")
        print("=" * 80)

        try:
            all_data = self.sheet.get_all_values()

            if len(all_data) <= 1:
                print("No jobs to clean")
                return

            # Separate rows by status
            not_applied_rows = [
                row for row in all_data[1:] if self._get_cell(row, 1) == "Not Applied"
            ]
            remaining_rows = [
                row
                for row in all_data[1:]
                if self._get_cell(row, 1) and self._get_cell(row, 1) != "Not Applied"
            ]

            if not not_applied_rows:
                print("No 'Not Applied' jobs found")
                return

            print(f"Moving {len(not_applied_rows)} jobs, keeping {len(remaining_rows)}")

            # Move to Reviewed sheet
            if not_applied_rows:
                self._move_to_reviewed(not_applied_rows)

            # Clear and repopulate main sheet
            self._repopulate_main_sheet(all_data, remaining_rows)

            print(
                f"✓ Moved {len(not_applied_rows)} jobs, {len(remaining_rows)} remaining"
            )
            print("=" * 80 + "\n")

        except Exception as e:
            print(f"✗ Cleanup error: {e}")

    def _move_to_reviewed(self, not_applied_rows):
        """Move jobs to Reviewed sheet with dynamic column sizing."""
        reviewed_data = self.reviewed_sheet.get_all_values()
        next_row = len(reviewed_data) + 1

        # Prepare rows
        reviewed_rows = [
            [
                next_row - 1 + idx,
                "Does not match profile",
                self._get_cell(row, 2),
                self._get_cell(row, 3),
                self._get_cell(row, 5),
                self._get_cell(row, 6),
                self._get_cell(row, 7),
                self._get_cell(row, 8),
                self._get_cell(row, 9),
                self._format_date(),
                self._get_cell(row, 11, "GitHub"),
                self._get_cell(row, 12, "Unknown"),
            ]
            for idx, row in enumerate(not_applied_rows)
        ]

        # Write data
        range_name = f"A{next_row}:L{next_row + len(reviewed_rows) - 1}"
        self.reviewed_sheet.update(
            values=reviewed_rows, range_name=range_name, value_input_option="RAW"
        )
        time.sleep(2)

        # Format cells
        self.reviewed_sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(2)

        # Add URL hyperlinks
        self._add_hyperlinks(
            self.reviewed_sheet, reviewed_rows, next_row, url_col_idx=4
        )

        # ✅ CRITICAL: Dynamic column sizing with FULLY DYNAMIC Reason column
        self._auto_resize_dynamic(self.reviewed_sheet, 12)

    def _repopulate_main_sheet(self, all_data, remaining_rows):
        """Clear and repopulate main sheet with renumbering."""
        # Delete all data rows
        if len(all_data) > 1:
            self.sheet.delete_rows(2, len(all_data))
            time.sleep(2)

        if not remaining_rows:
            return

        # Renumber rows
        renumbered_rows = [
            [idx] + row[1:] for idx, row in enumerate(remaining_rows, start=1)
        ]

        # Write data
        range_name = f"A2:M{1 + len(renumbered_rows)}"
        self.sheet.update(
            values=renumbered_rows, range_name=range_name, value_input_option="RAW"
        )
        time.sleep(2)

        # Format cells
        self.sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(2)

        # Add URL hyperlinks
        self._add_hyperlinks(self.sheet, renumbered_rows, 2, url_col_idx=5)

        # Add status dropdowns
        self._add_status_dropdowns(self.sheet, 2, len(renumbered_rows))

        # Apply status colors
        self._apply_status_colors(self.sheet, 2, 2 + len(renumbered_rows))

        # ✅ Dynamic column sizing
        self._auto_resize_dynamic(self.sheet, 13)

    def _add_hyperlinks(self, sheet, rows_data, start_row, url_col_idx):
        """Add URL hyperlinks in batch."""
        url_requests = []

        for idx, row_data in enumerate(rows_data):
            url = self._get_cell(row_data, url_col_idx)

            if url and url.startswith("http"):
                url_requests.append(
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": sheet.id,
                                "startRowIndex": start_row + idx - 1,
                                "endRowIndex": start_row + idx,
                                "startColumnIndex": url_col_idx,
                                "endColumnIndex": url_col_idx + 1,
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

    def _add_status_dropdowns(self, sheet, start_row, num_rows):
        """Add status dropdowns to column B."""
        dropdown_requests = [
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
                                for status in self.STATUS_VALUES
                            ],
                        },
                        "showCustomUi": True,
                        "strict": False,
                    },
                }
            }
            for idx in range(num_rows)
        ]

        if dropdown_requests:
            self.spreadsheet.batch_update({"requests": dropdown_requests})
            time.sleep(2)

    def _apply_status_colors(self, sheet, start_row, end_row):
        """Apply color coding to status column."""
        try:
            all_data = sheet.get_all_values()
            color_requests = []

            for row_idx in range(start_row - 1, min(end_row, len(all_data))):
                if row_idx < 1 or row_idx >= len(all_data):
                    continue

                status = self._get_cell(all_data[row_idx], 1)
                color = self.STATUS_COLORS.get(status)

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

            # Batch update in chunks
            for i in range(0, len(color_requests), 20):
                batch = color_requests[i : i + 20]
                self.spreadsheet.batch_update({"requests": batch})
                time.sleep(1)

        except:
            pass

    def _auto_resize_dynamic(self, sheet, total_columns):
        """✅ CRITICAL FIX: Fully dynamic column sizing with special Reason column handling."""
        try:
            all_data = sheet.get_all_values()

            if len(all_data) < 2:
                return

            # Calculate optimal width for each column
            column_widths = []

            for col_idx in range(total_columns):
                max_width = 50  # Minimum width

                # Check header (10px per char + padding)
                if len(all_data[0]) > col_idx:
                    header_text = str(all_data[0][col_idx])
                    header_width = len(header_text) * 10 + 40
                    max_width = max(max_width, header_width)

                # Check all data rows (8px per char + padding)
                for row in all_data[1:]:
                    if len(row) > col_idx:
                        cell_text = str(row[col_idx]).strip()
                        if cell_text:
                            text_width = len(cell_text) * 8 + 25
                            max_width = max(max_width, text_width)

                # ✅ Apply column-specific constraints
                if col_idx == 0:  # Sr. No.
                    max_width = min(max_width, 80)

                elif col_idx == 1:  # Status / Reason - FULLY DYNAMIC
                    # ✅ CRITICAL FIX: Allow Reason column to expand fully
                    max_width = min(max_width, 500)  # Increased from 250 to 500

                elif col_idx == 2:  # Company
                    max_width = min(max_width, 350)

                elif col_idx == 3:  # Title
                    max_width = min(max_width, 500)

                elif col_idx == 4:  # Date Applied / Job URL (Reviewed)
                    max_width = min(max_width, 150)

                elif col_idx == 5:  # Job URL (Valid) / Job ID (Reviewed)
                    max_width = min(max_width, 120)

                elif col_idx == 6:  # Job ID (Valid) / Job Type (Reviewed)
                    max_width = min(max_width, 120)

                elif col_idx == 7:  # Job Type (Valid) / Location (Reviewed)
                    max_width = min(max_width, 220)

                elif col_idx == 8:  # Location (Valid) / Remote (Reviewed)
                    max_width = min(max_width, 220)

                elif col_idx == 9:  # Remote (Valid) / Moved Date (Reviewed)
                    max_width = min(max_width, 190)

                elif col_idx == 10:  # Entry Date (Valid) / Source (Reviewed)
                    max_width = min(max_width, 190)

                elif col_idx == 11:  # Source (Valid) / Sponsorship (Reviewed)
                    max_width = min(max_width, 150)

                elif col_idx == 12:  # Sponsorship (Valid only)
                    max_width = min(max_width, 150)

                column_widths.append(max_width)

            # Build batch resize request
            resize_requests = [
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
                for col_idx, width in enumerate(column_widths)
            ]

            # Execute in batches
            for i in range(0, len(resize_requests), 100):
                batch = resize_requests[i : i + 100]
                self.spreadsheet.batch_update({"requests": batch})
                time.sleep(1)

        except:
            pass

    def _get_cell(self, row, index, default=""):
        """✅ OPTIMIZED: Safe cell access."""
        try:
            return row[index].strip() if len(row) > index and row[index] else default
        except:
            return default

    def _format_date(self):
        """Format current date/time."""
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")


if __name__ == "__main__":
    cleaner = ManualCleanup()
    cleaner.cleanup()
