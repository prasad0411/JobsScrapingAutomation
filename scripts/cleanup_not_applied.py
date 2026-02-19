#!/usr/bin/env python3
# cSpell:disable
"""
Automatic cleanup script - moves 'Not Applied' jobs to Reviewed sheet.
ENHANCED: Includes automatic 7-day backup to private GitHub repo.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time
import os
import shutil
import subprocess
from pathlib import Path

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
CREDS_FILE = os.path.join(".local", "credentials.json")

BACKUP_FOLDER = "../job-tracker-secrets"
BACKUP_TRACKING_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local", "last_backup_date.txt")
BACKUP_INTERVAL_DAYS = 7

FILES_TO_BACKUP = [
    "credentials.json",
    "gmail_credentials.json",
    "gmail_token.pickle",
    "jobright_cookies.json",
    "nu_cookies.json",
    "processed_emails.json",
    "workday_mapping.json",
    ".env",
]


class ManualCleanup:
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
        try:
            self.reviewed_sheet = self.spreadsheet.worksheet(REVIEWED_WORKSHEET)

            current_cols = len(self.reviewed_sheet.row_values(1))
            if current_cols < 12:
                self.reviewed_sheet.resize(rows=1000, cols=12)
                time.sleep(2)

            headers = self.reviewed_sheet.row_values(1)
            if "Sponsorship" not in headers:
                self.reviewed_sheet.update_cell(1, 12, "Sponsorship")
                time.sleep(1)
                self._format_headers(self.reviewed_sheet)

        except gspread.exceptions.WorksheetNotFound:
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
        print("=" * 80)
        print("CLEANUP: Moving 'Not Applied' jobs")
        print("=" * 80)

        try:
            all_data = self.sheet.get_all_values()

            if len(all_data) <= 1:
                print("No jobs to clean")
                return

            not_applied_rows = [
                row for row in all_data[1:] if self._get_cell(row, 1) == "Not Applied"
            ]
            remaining_rows = [
                row
                for row in all_data[1:]
                if self._get_cell(row, 1) and self._get_cell(row, 1) != "Not Applied"
            ]

            if not_applied_rows:
                print(
                    f"Moving {len(not_applied_rows)} jobs, keeping {len(remaining_rows)}"
                )
                self._move_to_reviewed(not_applied_rows)
                self._repopulate_main_sheet(all_data, remaining_rows)
                # Ensure buffer rows after cleanup
                current = self.sheet.row_count
                if current - len(remaining_rows) - 1 < 200:
                    self.sheet.resize(rows=current + 1000)
                    print(f"  ✓ Added 1000 buffer rows (now {current + 1000} total)")
                print(
                    f"✓ Moved {len(not_applied_rows)} jobs, {len(remaining_rows)} remaining"
                )
            else:
                print("No 'Not Applied' jobs found")

            print("=" * 80 + "\n")

        except Exception as e:
            print(f"✗ Cleanup error: {e}")

    def _move_to_reviewed(self, not_applied_rows):
        reviewed_data = self.reviewed_sheet.get_all_values()
        used_rows = len(reviewed_data)
        total_rows = self.reviewed_sheet.row_count
        available_rows = total_rows - used_rows

        if available_rows < 250:
            new_total = total_rows + 1000
            print(
                f"  Expanding Reviewed sheet: {total_rows} → {new_total} rows ({available_rows} available < 250 threshold)"
            )
            self.reviewed_sheet.resize(rows=new_total)
            time.sleep(2)
            print(f"  ✓ Reviewed sheet now has {1000 + available_rows} available rows")

        next_row = len(reviewed_data) + 1

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

        range_name = f"A{next_row}:L{next_row + len(reviewed_rows) - 1}"
        self.reviewed_sheet.update(
            values=reviewed_rows, range_name=range_name, value_input_option="RAW"
        )
        time.sleep(2)

        self.reviewed_sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )

        # Add clickable hyperlinks for Job URL column (E, index 4)
        self._add_hyperlinks(self.reviewed_sheet, reviewed_rows, next_row, url_col_idx=4)

    def _repopulate_main_sheet(self, all_data, remaining_rows):
        if len(all_data) > 1:
            self.sheet.delete_rows(2, len(all_data))
            time.sleep(2)

        if not remaining_rows:
            return

        renumbered_rows = []
        for idx, row in enumerate(remaining_rows, start=1):
            padded_row = (row + [""] * 15)[:15]
            new_row = [idx] + padded_row[1:15]
            renumbered_rows.append(new_row)

        range_name = f"A2:O{1 + len(renumbered_rows)}"
        self.sheet.update(
            values=renumbered_rows, range_name=range_name, value_input_option="RAW"
        )
        time.sleep(2)

        self.sheet.format(
            range_name,
            {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
            },
        )
        time.sleep(2)

        self._add_hyperlinks(self.sheet, renumbered_rows, 2, url_col_idx=5)
        self._add_status_dropdowns(self.sheet, 2, len(renumbered_rows))
        self._apply_status_colors(self.sheet, 2, 2 + len(renumbered_rows))

    def _add_hyperlinks(self, sheet, rows_data, start_row, url_col_idx):
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

            for i in range(0, len(color_requests), 20):
                batch = color_requests[i : i + 20]
                self.spreadsheet.batch_update({"requests": batch})
                time.sleep(1)

        except:
            pass

    def _get_cell(self, row, index, default=""):
        try:
            return row[index].strip() if len(row) > index and row[index] else default
        except:
            return default

    def _format_date(self):
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")


def check_if_backup_needed():
    """Check if 7 days have passed since last backup."""
    if not os.path.exists(BACKUP_TRACKING_FILE):
        return True

    try:
        with open(BACKUP_TRACKING_FILE, "r") as f:
            last_backup_str = f.read().strip()

        last_backup = datetime.datetime.strptime(last_backup_str, "%Y-%m-%d")
        days_since = (datetime.datetime.now() - last_backup).days

        if days_since >= BACKUP_INTERVAL_DAYS:
            return True
        else:
            print(f"\nℹ️  Backup not needed (last backup {days_since} days ago)")
            return False
    except:
        return True


def backup_to_private_repo():
    """Automated backup of secret files to private GitHub repo."""
    print("\n" + "=" * 80)
    print("AUTOMATED BACKUP TO PRIVATE REPO")
    print("=" * 80)

    project_dir = Path(__file__).parent.parent  # scripts/ → project root
    backup_dir = project_dir.parent / "job-tracker-secrets"

    if not backup_dir.exists():
        print(f"⚠️  Backup directory not found: {backup_dir}")
        print(f"   Run setup: See BACKUP_AUTOMATION_COMPLETE_GUIDE.md")
        return False

    backed_up = []
    missing = []

    for filename in FILES_TO_BACKUP:
        source = project_dir / ".local" / filename
        if not source.exists():
            source = project_dir / filename
        destination = backup_dir / filename

        if source.exists():
            try:
                shutil.copy2(source, destination)
                backed_up.append(filename)
                print(f"  ✓ Backed up: {filename}")
            except Exception as e:
                print(f"  ✗ Failed: {filename} - {e}")
        else:
            missing.append(filename)

    timestamp_file = backup_dir / "last_backup.txt"
    with open(timestamp_file, "w") as f:
        f.write(
            f"Last backup: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        f.write(f"Files backed up: {len(backed_up)}\n")
        f.write(f"Files: {', '.join(backed_up)}\n")

    print(f"\n  Backed up {len(backed_up)} files")
    if missing:
        print(f"  Skipped {len(missing)} missing: {', '.join(missing)}")

    try:
        original_dir = os.getcwd()
        os.chdir(backup_dir)

        subprocess.run(["git", "add", "."], check=True, capture_output=True)

        commit_msg = f"Auto-backup {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg], capture_output=True, text=True
        )

        if "nothing to commit" in result.stdout:
            print(f"\n  ℹ️  No changes since last backup")
            os.chdir(original_dir)

            with open(BACKUP_TRACKING_FILE, "w") as f:
                f.write(datetime.datetime.now().strftime("%Y-%m-%d"))

            print("=" * 80)
            return True

        push_result = subprocess.run(
            ["git", "push", "origin", "main"],
            check=True,
            capture_output=True,
            text=True,
        )

        os.chdir(original_dir)

        with open(BACKUP_TRACKING_FILE, "w") as f:
            f.write(datetime.datetime.now().strftime("%Y-%m-%d"))

        print(f"\n  ✅ Pushed to private GitHub repo")
        print(
            f"  Next backup: {(datetime.datetime.now() + datetime.timedelta(days=BACKUP_INTERVAL_DAYS)).strftime('%Y-%m-%d')}"
        )
        print("=" * 80)
        return True

    except subprocess.CalledProcessError as e:
        os.chdir(original_dir)
        print(f"\n  ✗ Git error: {e}")
        print(f"  Check GitHub authentication")
        print("=" * 80)
        return False
    except Exception as e:
        os.chdir(original_dir)
        print(f"\n  ✗ Backup failed: {e}")
        print("=" * 80)
        return False


if __name__ == "__main__":
    cleaner = ManualCleanup()
    cleaner.cleanup()

    if check_if_backup_needed():
        backup_to_private_repo()

    print()
