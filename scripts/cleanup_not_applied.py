#!/usr/bin/env python3
# cSpell:disable
"""
Automatic cleanup script - moves 'Not Applied' jobs to Reviewed sheet.
ENHANCED: Includes automatic 7-day backup to private GitHub repo.
ENHANCED: Moves expired jobs (blank status, 3+ days old) to Reviewed sheet.
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
BACKUP_TRACKING_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".local",
    "last_backup_date.txt",
)
BACKUP_INTERVAL_DAYS = 7

EXPIRY_DAYS = 2  # Jobs older than this with no status get moved

FILES_TO_BACKUP = [
    "credentials.json",
    "gmail_credentials.json",
    "gmail_token.pickle",
    "jobright_cookies.json",
    "nu_cookies.json",
    "processed_emails.json",
    "workday_mapping.json",
    ".env",
    "Prasad Kanade SWE Resume.pdf",
    "Prasad Kanade ML Resume.pdf",
    "brain.json",
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

    # Statuses that are PROTECTED - never moved regardless of age
    PROTECTED_STATUSES = {
        "Applied",
        "Rejected",
        "OA Round 1",
        "OA Round 2",
        "Interview 1",
        "Offer accepted",
        "Assessment",
    }

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
        # Load Outreach Tracker to check Extract/email status before expiring
        self._outreach_map = {}
        try:
            from outreach.outreach_config import C, OUTREACH_TAB
            from outreach.outreach_data import _pad
            ows = self.spreadsheet.worksheet(OUTREACH_TAB)
            time.sleep(1)
            odata = ows.get_all_values()
            for row in odata[1:]:
                row = _pad(row)
                co = row[C["company"]].strip().lower()
                ti = row[C["title"]].strip().lower()
                extract = row[C["extract"]].strip().lower() if len(row) > C["extract"] else ""
                hm_email = row[C["hm_email"]].strip() if len(row) > C["hm_email"] else ""
                rec_email = row[C["rec_email"]].strip() if len(row) > C["rec_email"] else ""
                if co:
                    self._outreach_map[(co, ti)] = {
                        "extract": extract,
                        "hm_email": hm_email,
                        "rec_email": rec_email,
                    }
        except Exception as _oe:
            pass  # Non-fatal — cleanup proceeds without protection check

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

    def _parse_entry_date(self, date_str):
        """Parse Entry Date format: '17 March, 11:10 AM' → datetime object."""
        if not date_str:
            return None
        try:
            yr = datetime.datetime.now().year
            return datetime.datetime.strptime(f"{date_str.strip()} {yr}", "%d %B, %I:%M %p %Y")
        except ValueError:
            try:
                dt = datetime.datetime.strptime(date_str.strip(), "%d %B, %I:%M %p")
                return dt.replace(year=datetime.datetime.now().year)
            except ValueError:
                return None

    def _is_expired(self, row):
        """Returns True if row has no protected status AND entry date is 2+ days old."""
        status = self._get_cell(row, 1)

        # Protected statuses — never move
        if status in self.PROTECTED_STATUSES:
            return False

        # Only move blank or "Not Applied" status rows
        if status not in ("", "Not Applied"):
            return False

        # PROTECTION: if Extract=yes AND both emails empty → outreach pipeline
        # is still working on finding emails — do NOT expire
        co = self._get_cell(row, 2).strip().lower()
        ti = self._get_cell(row, 3).strip().lower()
        outreach = self._outreach_map.get((co, ti), {})
        if outreach.get("extract", "").lower() == "yes":
            hm = outreach.get("hm_email", "").strip()
            rec = outreach.get("rec_email", "").strip()
            if not hm and not rec:
                return False  # Still being worked on

        # Check entry date (column index 11 = L = Entry Date)
        entry_date_str = self._get_cell(row, 11)
        if not entry_date_str:
            return False

        entry_date = self._parse_entry_date(entry_date_str)
        if not entry_date:
            return False

        # Set current year since our date format has no year
        now = datetime.datetime.now()
        entry_date = entry_date.replace(year=now.year)

        age_days = (now - entry_date).days
        return age_days >= EXPIRY_DAYS

    def cleanup_expired(self):
        """Move blank-status jobs older than EXPIRY_DAYS days to Reviewed sheet."""
        print("=" * 80)
        print(
            f"EXPIRY CLEANUP: Moving jobs with no status older than {EXPIRY_DAYS} days"
        )
        print("=" * 80)

        try:
            all_data = self.sheet.get_all_values()

            if len(all_data) <= 1:
                print("No jobs to check")
                return

            expired_rows = [row for row in all_data[1:] if self._is_expired(row)]
            remaining_rows = [
                row
                for row in all_data[1:]
                if not self._is_expired(row) and self._get_cell(row, 1)
            ]  # keep rows with any status

            # Also keep blank-status rows that are NOT expired yet
            not_expired_blank = [
                row
                for row in all_data[1:]
                if not self._is_expired(row)
                and self._get_cell(row, 1) not in self.PROTECTED_STATUSES
                and self._get_cell(row, 1) == ""
            ]

            remaining_rows = [
                row
                for row in all_data[1:]
                if not self._is_expired(row) and self._get_cell(row, 0)
            ]

            if expired_rows:
                print(f"Found {len(expired_rows)} expired jobs to move")
                for row in expired_rows:
                    company = self._get_cell(row, 2)
                    entry_date = self._get_cell(row, 11)
                    print(f"  → {company} (added {entry_date})")

                self._move_to_reviewed(expired_rows, reason="Expired: 2+ days")
                self._repopulate_main_sheet(all_data, remaining_rows)

                current = self.sheet.row_count
                used = len(remaining_rows) + 1
                empty_rows = current - used
                if empty_rows < 200:
                    self.sheet.resize(rows=current + 1000)

                print(
                    f"✓ Moved {len(expired_rows)} expired jobs, {len(remaining_rows)} remaining"
                )
            else:
                print(
                    f"No expired jobs found (all blank-status jobs are under {EXPIRY_DAYS} days old)"
                )

            print("=" * 80 + "\n")

        except Exception as e:
            print(f"✗ Expiry cleanup error: {e}")
            import traceback

            traceback.print_exc()

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
                self._move_to_reviewed(
                    not_applied_rows, reason="Does not match profile"
                )
                self._repopulate_main_sheet(all_data, remaining_rows)
                current = self.sheet.row_count
                used = len(remaining_rows) + 1
                empty_rows = current - used
                if empty_rows < 200:
                    self.sheet.resize(rows=current + 1000)
                    print(f"  ✓ Added 1000 buffer rows (now {current + 1000} total)")
                else:
                    print(f"  ℹ️  Buffer ok ({empty_rows} empty rows available)")
                print(
                    f"✓ Moved {len(not_applied_rows)} jobs, {len(remaining_rows)} remaining"
                )
            else:
                print("No 'Not Applied' jobs found")

            print("=" * 80 + "\n")

        except Exception as e:
            print(f"✗ Cleanup error: {e}")

    def _move_to_reviewed(self, rows_to_move, reason="Does not match profile"):
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
                reason,
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
            for idx, row in enumerate(rows_to_move)
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

        self._add_hyperlinks(
            self.reviewed_sheet, reviewed_rows, next_row, url_col_idx=4
        )

    def _repopulate_main_sheet(self, all_data, remaining_rows):
        if len(all_data) > 1:
            self.sheet.delete_rows(2, len(all_data))
            time.sleep(2)

        if not remaining_rows:
            return

        renumbered_rows = []
        for idx, row in enumerate(remaining_rows, start=1):
            padded_row = (row + [""] * 16)[:16]
            new_row = [idx] + padded_row[1:16]
            renumbered_rows.append(new_row)

        range_name = f"A2:P{1 + len(renumbered_rows)}"
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

    project_dir = Path(__file__).parent.parent
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
        print("=" * 80)
        return False
    except Exception as e:
        os.chdir(original_dir)
        print(f"\n  ✗ Backup failed: {e}")
        print("=" * 80)
        return False


if __name__ == "__main__":
    cleaner = ManualCleanup()

    # Run expiry cleanup FIRST (blank status, 3+ days old)
    cleaner.cleanup_expired()

    # Then run standard Not Applied cleanup
    cleaner.cleanup()

    if check_if_backup_needed():
        backup_to_private_repo()

    print()
