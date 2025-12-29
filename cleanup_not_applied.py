#!/usr/bin/env python3

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time
import logging

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
CREDS_FILE = 'credentials.json'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('cleanup.log', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)


class ManualCleanup:
    def __init__(self):
        logger.info("Initializing Manual Cleanup")
        
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open(SHEET_NAME)
        self.sheet = spreadsheet.worksheet(WORKSHEET_NAME)
        self.spreadsheet = spreadsheet
        
        try:
            self.reviewed_sheet = spreadsheet.worksheet(REVIEWED_WORKSHEET)
            
            current_cols = len(self.reviewed_sheet.row_values(1))
            if current_cols < 12:
                logger.info(f"Expanding Reviewed sheet from {current_cols} to 12 columns")
                self.reviewed_sheet.resize(rows=1000, cols=12)
                time.sleep(2)
            
            headers = self.reviewed_sheet.row_values(1)
            if 'Sponsorship' not in headers:
                logger.info("Adding Sponsorship header to column 12")
                self.reviewed_sheet.update_cell(1, 12, 'Sponsorship')
                time.sleep(1)
                self.format_sheet_headers(self.reviewed_sheet)
        except gspread.exceptions.WorksheetNotFound:
            self.reviewed_sheet = spreadsheet.add_worksheet(title=REVIEWED_WORKSHEET, rows=1000, cols=12)
            headers = ['Sr. No.', 'Reason', 'Company', 'Title', 'Job URL', 'Job ID', 
                      'Job Type', 'Location', 'Remote?', 'Moved Date', 'Source', 'Sponsorship']
            self.reviewed_sheet.append_row(headers)
            self.format_sheet_headers(self.reviewed_sheet)
    
    def safe_get_cell(self, row, index, default=''):
        try:
            if len(row) > index:
                return row[index].strip() if row[index] else default
            return default
        except:
            return default
    
    def format_sheet_headers(self, sheet):
        try:
            sheet.format('A1:L1', {
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE',
                'textFormat': {
                    'fontFamily': 'Times New Roman',
                    'fontSize': 14,
                    'bold': True,
                    'foregroundColor': {'red': 0, 'green': 0, 'blue': 0}
                },
                'backgroundColor': {'red': 0.7, 'green': 0.9, 'blue': 0.7}
            })
        except Exception as e:
            logger.warning(f"Header format: {e}")
    
    def format_date(self):
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")
    
    def auto_resize_all_columns_except_url(self, sheet, url_column_index=4, total_columns=12):
        try:
            requests = [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": total_columns
                    }
                }
            }]
            
            self.spreadsheet.batch_update({"requests": requests})
            time.sleep(1)
            
            fixed_width_requests = []
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 2
                    },
                    "properties": {"pixelSize": 250},
                    "fields": "pixelSize"
                }
            })
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": url_column_index,
                        "endIndex": url_column_index + 1
                    },
                    "properties": {"pixelSize": 100},
                    "fields": "pixelSize"
                }
            })
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 9,
                        "endIndex": 10
                    },
                    "properties": {"pixelSize": 150},
                    "fields": "pixelSize"
                }
            })
            
            self.spreadsheet.batch_update({"requests": fixed_width_requests})
            
        except Exception as e:
            logger.warning(f"Resize: {e}")
    
    def get_status_color(self, status):
        colors = {
            'Not Applied': {'red': 0.6, 'green': 0.76, 'blue': 1.0},
            'Applied': {'red': 0.58, 'green': 0.93, 'blue': 0.31},
            'Rejected': {'red': 0.97, 'green': 0.42, 'blue': 0.42},
            'OA Round 1': {'red': 1.0, 'green': 0.95, 'blue': 0.4},
            'OA Round 2': {'red': 1.0, 'green': 0.95, 'blue': 0.4},
            'Interview 1': {'red': 0.82, 'green': 0.93, 'blue': 0.94},
            'Offer accepted': {'red': 0.16, 'green': 0.65, 'blue': 0.27},
            'Assessment': {'red': 0.89, 'green': 0.89, 'blue': 0.89}
        }
        return colors.get(status, None)
    
    def apply_status_colors_to_range(self, start_row, end_row):
        try:
            all_data = self.sheet.get_all_values()
            
            color_requests = []
            for row_idx in range(start_row - 1, min(end_row, len(all_data))):
                if row_idx < 1 or row_idx >= len(all_data):
                    continue
                
                row = all_data[row_idx]
                status = self.safe_get_cell(row, 1, '')
                color = self.get_status_color(status)
                
                if color:
                    text_color = {'red': 1.0, 'green': 1.0, 'blue': 1.0} if status == 'Offer accepted' else {'red': 0.0, 'green': 0.0, 'blue': 0.0}
                    
                    color_requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": self.sheet.id,
                                "startRowIndex": row_idx,
                                "endRowIndex": row_idx + 1,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": text_color,
                                        "fontFamily": "Times New Roman",
                                        "fontSize": 13
                                    },
                                    "horizontalAlignment": "CENTER",
                                    "verticalAlignment": "MIDDLE"
                                }
                            },
                            "fields": "userEnteredFormat"
                        }
                    })
            
            if color_requests:
                for i in range(0, len(color_requests), 20):
                    batch = color_requests[i:i+20]
                    self.spreadsheet.batch_update({"requests": batch})
                    time.sleep(1)
                
        except Exception as e:
            logger.warning(f"Colors: {e}")
    
    def cleanup(self):
        logger.info("=" * 70)
        logger.info("MANUAL CLEANUP: Moving 'Not Applied' jobs")
        logger.info("=" * 70)
        
        try:
            all_data = self.sheet.get_all_values()
            
            if len(all_data) <= 1:
                logger.info("No jobs in main sheet")
                return
            
            not_applied_rows = []
            remaining_rows = []
            
            for idx, row in enumerate(all_data[1:], start=2):
                status = self.safe_get_cell(row, 1, '')
                
                if status == "Not Applied":
                    not_applied_rows.append(row)
                elif status:
                    remaining_rows.append(row)
            
            if not not_applied_rows:
                logger.info("No 'Not Applied' jobs found")
                return
            
            logger.info(f"Found {len(not_applied_rows)} 'Not Applied' jobs")
            logger.info(f"Keeping {len(remaining_rows)} jobs with other statuses")
            
            print(f"\nThis will move {len(not_applied_rows)} 'Not Applied' jobs to Reviewed sheet.")
            print(f"Main sheet will have {len(remaining_rows)} jobs remaining.")
            
            confirm = input("\nProceed? (yes/no): ").strip().lower()
            
            if confirm not in ['yes', 'y']:
                logger.info("Cleanup cancelled")
                return
            
            logger.info("Starting cleanup...")
            
            reviewed_data = self.reviewed_sheet.get_all_values()
            next_reviewed_row = len(reviewed_data) + 1
            
            reviewed_rows = []
            for row in not_applied_rows:
                reviewed_rows.append([
                    next_reviewed_row - 1 + len(reviewed_rows),
                    'Does not match profile',
                    self.safe_get_cell(row, 2, ''),
                    self.safe_get_cell(row, 3, ''),
                    self.safe_get_cell(row, 5, ''),
                    self.safe_get_cell(row, 6, ''),
                    self.safe_get_cell(row, 7, ''),
                    self.safe_get_cell(row, 8, ''),
                    self.safe_get_cell(row, 9, ''),
                    self.format_date(),
                    self.safe_get_cell(row, 11, 'GitHub'),
                    self.safe_get_cell(row, 12, 'Unknown')
                ])
            
            if reviewed_rows:
                range_name = f'A{next_reviewed_row}:L{next_reviewed_row + len(reviewed_rows) - 1}'
                self.reviewed_sheet.update(values=reviewed_rows, range_name=range_name, value_input_option='RAW')
                time.sleep(2)
                
                self.reviewed_sheet.format(range_name, {
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'textFormat': {'fontFamily': 'Times New Roman', 'fontSize': 13}
                })
                time.sleep(2)
                
                url_requests = []
                for idx in range(len(reviewed_rows)):
                    row_num = next_reviewed_row + idx
                    url = reviewed_rows[idx][4]
                    
                    if url and url.startswith('http'):
                        url_requests.append({
                            "updateCells": {
                                "range": {
                                    "sheetId": self.reviewed_sheet.id,
                                    "startRowIndex": row_num - 1,
                                    "endRowIndex": row_num,
                                    "startColumnIndex": 4,
                                    "endColumnIndex": 5
                                },
                                "rows": [{
                                    "values": [{
                                        "userEnteredValue": {"stringValue": url},
                                        "textFormatRuns": [{"format": {"link": {"uri": url}}}]
                                    }]
                                }],
                                "fields": "userEnteredValue,textFormatRuns"
                            }
                        })
                
                if url_requests:
                    self.spreadsheet.batch_update({"requests": url_requests})
                    time.sleep(2)
                
                self.auto_resize_all_columns_except_url(self.reviewed_sheet, url_column_index=4, total_columns=12)
                
                logger.info(f"Moved {len(reviewed_rows)} jobs to Reviewed sheet")
            
            if len(all_data) > 1:
                self.sheet.delete_rows(2, len(all_data) - 1)
                time.sleep(2)
            
            if remaining_rows:
                renumbered_rows = []
                for idx, row in enumerate(remaining_rows, start=1):
                    new_row = [idx] + row[1:]
                    renumbered_rows.append(new_row)
                
                range_name = f'A2:M{1 + len(renumbered_rows)}'
                self.sheet.update(values=renumbered_rows, range_name=range_name, value_input_option='RAW')
                time.sleep(2)
                
                self.sheet.format(range_name, {
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'textFormat': {'fontFamily': 'Times New Roman', 'fontSize': 13}
                })
                time.sleep(2)
                
                url_requests = []
                for idx, row in enumerate(renumbered_rows):
                    row_num = 2 + idx
                    url = self.safe_get_cell(row, 5, '')
                    
                    if url and url.startswith('http'):
                        url_requests.append({
                            "updateCells": {
                                "range": {
                                    "sheetId": self.sheet.id,
                                    "startRowIndex": row_num - 1,
                                    "endRowIndex": row_num,
                                    "startColumnIndex": 5,
                                    "endColumnIndex": 6
                                },
                                "rows": [{
                                    "values": [{
                                        "userEnteredValue": {"stringValue": url},
                                        "textFormatRuns": [{"format": {"link": {"uri": url}}}]
                                    }]
                                }],
                                "fields": "userEnteredValue,textFormatRuns"
                            }
                        })
                
                if url_requests:
                    self.spreadsheet.batch_update({"requests": url_requests})
                    time.sleep(2)
                
                dropdown_requests = []
                for idx in range(len(renumbered_rows)):
                    row_num = 2 + idx
                    
                    dropdown_requests.append({
                        "setDataValidation": {
                            "range": {
                                "sheetId": self.sheet.id,
                                "startRowIndex": row_num - 1,
                                "endRowIndex": row_num,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2
                            },
                            "rule": {
                                "condition": {
                                    "type": "ONE_OF_LIST",
                                    "values": [
                                        {"userEnteredValue": "Not Applied"},
                                        {"userEnteredValue": "Applied"},
                                        {"userEnteredValue": "Rejected"},
                                        {"userEnteredValue": "OA Round 1"},
                                        {"userEnteredValue": "OA Round 2"},
                                        {"userEnteredValue": "Interview 1"},
                                        {"userEnteredValue": "Offer accepted"},
                                        {"userEnteredValue": "Assessment"}
                                    ]
                                },
                                "showCustomUi": True,
                                "strict": False
                            }
                        }
                    })
                
                if dropdown_requests:
                    self.spreadsheet.batch_update({"requests": dropdown_requests})
                    time.sleep(2)
                
                self.apply_status_colors_to_range(2, 2 + len(renumbered_rows))
                
                logger.info(f"Renumbered {len(renumbered_rows)} remaining jobs (1-{len(renumbered_rows)})")
            
            self.auto_resize_all_columns_except_url(self.sheet, url_column_index=5, total_columns=13)
            
            logger.info("=" * 70)
            logger.info(f"CLEANUP COMPLETE!")
            logger.info(f"   Main sheet: {len(remaining_rows)} jobs")
            logger.info(f"   Reviewed sheet: +{len(not_applied_rows)} jobs")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    def auto_resize_all_columns_except_url(self, sheet, url_column_index=5, total_columns=13):
        try:
            requests = [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": total_columns
                    }
                }
            }]
            
            self.spreadsheet.batch_update({"requests": requests})
            time.sleep(1)
            
            fixed_width_requests = []
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 2
                    },
                    "properties": {"pixelSize": 90},
                    "fields": "pixelSize"
                }
            })
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 4,
                        "endIndex": 5
                    },
                    "properties": {"pixelSize": 150},
                    "fields": "pixelSize"
                }
            })
            
            fixed_width_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": url_column_index,
                        "endIndex": url_column_index + 1
                    },
                    "properties": {"pixelSize": 100},
                    "fields": "pixelSize"
                }
            })
            
            self.spreadsheet.batch_update({"requests": fixed_width_requests})
            
        except Exception as e:
            logger.warning(f"Resize: {e}")
    
    def apply_status_colors_to_range(self, start_row, end_row):
        try:
            all_data = self.sheet.get_all_values()
            
            color_requests = []
            for row_idx in range(start_row - 1, min(end_row, len(all_data))):
                if row_idx < 1 or row_idx >= len(all_data):
                    continue
                
                row = all_data[row_idx]
                status = self.safe_get_cell(row, 1, '')
                color = self.get_status_color(status)
                
                if color:
                    text_color = {'red': 1.0, 'green': 1.0, 'blue': 1.0} if status == 'Offer accepted' else {'red': 0.0, 'green': 0.0, 'blue': 0.0}
                    
                    color_requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": self.sheet.id,
                                "startRowIndex": row_idx,
                                "endRowIndex": row_idx + 1,
                                "startColumnIndex": 1,
                                "endColumnIndex": 2
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": color,
                                    "textFormat": {
                                        "foregroundColor": text_color,
                                        "fontFamily": "Times New Roman",
                                        "fontSize": 13
                                    },
                                    "horizontalAlignment": "CENTER",
                                    "verticalAlignment": "MIDDLE"
                                }
                            },
                            "fields": "userEnteredFormat"
                        }
                    })
            
            if color_requests:
                for i in range(0, len(color_requests), 20):
                    batch = color_requests[i:i+20]
                    self.spreadsheet.batch_update({"requests": batch})
                    time.sleep(1)
                
        except Exception as e:
            logger.warning(f"Colors: {e}")
    
    def trim_log_to_last_3_runs(self):
        try:
            log_file = 'cleanup.log'
            
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            complete_indices = []
            for idx, line in enumerate(lines):
                if 'CLEANUP COMPLETE!' in line:
                    complete_indices.append(idx)
            
            if len(complete_indices) > 3:
                cutoff_index = complete_indices[-3]
                
                start_index = cutoff_index
                for i in range(cutoff_index - 1, -1, -1):
                    if 'CLEANUP:' in lines[i]:
                        start_index = i
                        break
                
                trimmed_lines = lines[start_index:]
                
                with open(log_file, 'w') as f:
                    f.writelines(trimmed_lines)
        except:
            pass

if __name__ == "__main__":
    cleaner = ManualCleanup()
    cleaner.cleanup()
    cleaner.trim_log_to_last_3_runs()