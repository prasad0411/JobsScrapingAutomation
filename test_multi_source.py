#!/usr/bin/env python3
"""
UNIFIED JOB SCRAPER
- Scrapes SimplifyJobs GitHub (last 24h)
- Fetches SWE List emails from Gmail (last 24h)
- Auto-removes duplicates from all sheets
- Deduplicates across BOTH sources
- One log file, last 3 runs only
"""

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import datetime
import time
import re
import logging
import random
import base64
import pickle
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Job Tracking Automation"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
SHEETS_CREDS_FILE = 'credentials.json'
GMAIL_CREDS_FILE = 'gmail_credentials.json'
GMAIL_TOKEN_FILE = 'gmail_token.pickle'

SIMPLIFY_URL = 'https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md'
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# SIMPLE LOGGING - One file, last 3 runs only
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('job_scraper.log', mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)


class UnifiedJobAggregator:
    def __init__(self):
        logger.info("=" * 50)
        logger.info("🚀 Unified Job Tracker - Starting")
        logger.info("=" * 50)
        
        # Google Sheets setup
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS_FILE, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open(SHEET_NAME)
        self.sheet = spreadsheet.worksheet(WORKSHEET_NAME)
        self.spreadsheet = spreadsheet
        
        try:
            self.discarded_sheet = spreadsheet.worksheet(DISCARDED_WORKSHEET)
        except:
            self.discarded_sheet = spreadsheet.add_worksheet(title=DISCARDED_WORKSHEET, rows=1000, cols=12)
            headers = ['Sr. No.', 'Discard Reason', 'Company', 'Title', 'Date Applied', 
                      'Job URL', 'Job ID', 'Job Type', 'Location', 'Remote?', 'Entry Date', 'Source']
            self.discarded_sheet.append_row(headers)
            self.format_sheet_headers(self.discarded_sheet, num_cols=12)
        
        try:
            self.reviewed_sheet = spreadsheet.worksheet(REVIEWED_WORKSHEET)
        except:
            self.reviewed_sheet = spreadsheet.add_worksheet(title=REVIEWED_WORKSHEET, rows=1000, cols=11)
            headers = ['Sr. No.', 'Reason', 'Company', 'Title', 'Job URL', 'Job ID', 
                      'Job Type', 'Location', 'Remote?', 'Moved Date', 'Source']
            self.reviewed_sheet.append_row(headers)
            self.format_sheet_headers(self.reviewed_sheet, num_cols=11)
        
        # STEP 1: Auto-remove duplicates
        self.remove_duplicates_from_all_sheets()
        
        # STEP 2: Load existing jobs
        logger.info("🔍 Loading jobs from all sheets...")
        
        self.existing_jobs = set()
        self.existing_urls = set()
        self.next_row = 2
        self.next_sr_no = 1
        self.next_discarded_row = 2
        self.next_discarded_sr_no = 1
        
        # Load from main sheet
        main_data = self.sheet.get_all_values()
        for idx, row in enumerate(main_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ''
                
                if company or title:
                    self.next_row = idx + 1
                    self.next_sr_no = idx
                    if company and title:
                        self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
        
        # Load from discarded sheet
        discarded_data = self.discarded_sheet.get_all_values()
        for idx, row in enumerate(discarded_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ''
                
                if company or title:
                    self.next_discarded_row = idx + 1
                    self.next_discarded_sr_no = idx
                    if company and title:
                        self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
        
        # Load from reviewed sheet
        reviewed_data = self.reviewed_sheet.get_all_values()
        for row in reviewed_data[1:]:
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[4].strip() if len(row) > 4 else ''
                
                if company and title:
                    self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
        
        logger.info(f"  ✅ Tracking {len(self.existing_jobs)} jobs")
        logger.info(f"  ✅ Tracking {len(self.existing_urls)} URLs")
        
        self.added = 0
        self.discarded = 0
        self.valid_jobs = []
        self.discarded_jobs = []
        self.gmail_service = None
    
    def remove_duplicates_from_all_sheets(self):
        """Auto-remove duplicates at startup"""
        logger.info("🧹 Checking for duplicates...")
        
        total_removed = 0
        total_removed += self.remove_duplicates_from_sheet(self.sheet, "Main", 2, 3, 5)
        total_removed += self.remove_duplicates_from_sheet(self.discarded_sheet, "Discarded", 2, 3, 5)
        total_removed += self.remove_duplicates_from_sheet(self.reviewed_sheet, "Reviewed", 2, 3, 4)
        
        if total_removed > 0:
            logger.info(f"  ✅ Removed {total_removed} duplicates")
        else:
            logger.info(f"  ✅ No duplicates")
    
    def remove_duplicates_from_sheet(self, sheet, name, c_idx, t_idx, u_idx):
        """Remove duplicates from single sheet"""
        try:
            all_data = sheet.get_all_values()
            if len(all_data) <= 1:
                return 0
            
            seen_jobs = set()
            seen_urls = set()
            rows_to_delete = []
            
            for idx, row in enumerate(all_data[1:], start=2):
                if len(row) <= max(c_idx, t_idx, u_idx):
                    continue
                
                company = row[c_idx].strip()
                title = row[t_idx].strip()
                url = row[u_idx].strip() if len(row) > u_idx else ''
                
                if not company and not title:
                    continue
                
                job_key = f"{company.lower()}_{title.lower()}"
                url_key = self.clean_url(url) if url and 'http' in url else None
                
                is_dup = job_key in seen_jobs or (url_key and url_key in seen_urls)
                
                if is_dup:
                    rows_to_delete.append(idx)
                else:
                    seen_jobs.add(job_key)
                    if url_key:
                        seen_urls.add(url_key)
            
            if not rows_to_delete:
                return 0
            
            logger.info(f"  🗑️  {name}: {len(rows_to_delete)} duplicates")
            
            for row_num in reversed(rows_to_delete):
                sheet.delete_rows(row_num)
                time.sleep(0.3)
            
            remaining = sheet.get_all_values()
            for idx in range(1, len(remaining)):
                sheet.update_cell(idx + 1, 1, idx)
                time.sleep(0.3)
            
            return len(rows_to_delete)
        except:
            return 0
    
    def authenticate_gmail(self):
        """Authenticate with Gmail API"""
        creds = None
        
        if os.path.exists(GMAIL_TOKEN_FILE):
            with open(GMAIL_TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CREDS_FILE, GMAIL_SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(GMAIL_TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
        
        self.gmail_service = build('gmail', 'v1', credentials=creds)
        logger.info("  ✅ Gmail authenticated")
    
    def fetch_swelist_emails(self):
        """Fetch SWE List emails from last 24h"""
        try:
            if not self.gmail_service:
                self.authenticate_gmail()
            
            # Search for SWE List emails from last 24 hours
            query = 'from:noreply@swelist.com newer_than:1d'
            
            results = self.gmail_service.users().messages().list(
                userId='me',
                q=query,
                maxResults=10
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.info("  No new SWE List emails")
                return []
            
            logger.info(f"  Found {len(messages)} SWE List emails")
            
            all_email_jobs = []
            
            for message in messages:
                msg = self.gmail_service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()
                
                # Get HTML body
                html_content = None
                
                if 'parts' in msg['payload']:
                    for part in msg['payload']['parts']:
                        if part['mimeType'] == 'text/html':
                            html_data = part['body'].get('data', '')
                            html_content = base64.urlsafe_b64decode(html_data).decode('utf-8')
                            break
                elif 'body' in msg['payload']:
                    html_data = msg['payload']['body'].get('data', '')
                    if html_data:
                        html_content = base64.urlsafe_b64decode(html_data).decode('utf-8')
                
                if html_content:
                    jobs = self.parse_swelist_email(html_content)
                    all_email_jobs.extend(jobs)
            
            logger.info(f"  ✅ Extracted {len(all_email_jobs)} jobs from emails")
            return all_email_jobs
            
        except FileNotFoundError:
            logger.warning("  ⚠️  Gmail credentials not found")
            logger.warning("  ⚠️  Run setup first: See SETUP_INSTRUCTIONS.md")
            return []
        except Exception as e:
            logger.error(f"  ❌ Gmail error: {e}")
            return []
    
    def parse_swelist_email(self, email_html):
        """Parse SWE List email HTML"""
        soup = BeautifulSoup(email_html, 'html.parser')
        
        jobs = []
        
        # Find all <p class="internship"> tags
        internship_paragraphs = soup.find_all('p', class_='internship')
        
        for p in internship_paragraphs:
            try:
                strong_tag = p.find('strong')
                link_tag = p.find('a')
                
                if not strong_tag or not link_tag:
                    continue
                
                company = strong_tag.text.strip().rstrip(':')
                title = link_tag.text.strip()
                url = link_tag.get('href', '')
                
                # Clean URL (remove tracking)
                if '?utm_source' in url:
                    url = url.split('?utm_source')[0]
                
                if not company or not title or not url:
                    continue
                
                jobs.append({
                    'company': self.remove_emojis(company),
                    'title': self.remove_emojis(title),
                    'url': url,
                    'source': 'SWE List Email'
                })
            except:
                continue
        
        return jobs
    
    def process_email_jobs(self, email_jobs):
        """Process jobs from emails (apply same filters as GitHub)"""
        if not email_jobs:
            return
        
        logger.info(f"📧 Processing {len(email_jobs)} email jobs...")
        
        for job in email_jobs:
            company = job['company']
            title = job['title']
            url = job['url']
            
            # CHECK DUPLICATE (already loaded from all 3 sheets)
            if self.is_duplicate(company, title, url):
                continue
            
            # Apply filters
            discard_reason = None
            if not self.is_cs_engineering_role(title):
                discard_reason = "Non-tech"
            elif '🔒' in title or 'Closed' in title:
                discard_reason = "Closed"
            
            if discard_reason:
                self.discarded_jobs.append({
                    'company': company, 'title': title, 'location': 'Unknown',
                    'job_type': self.determine_job_type(title),
                    'remote': 'Unknown',
                    'url': url, 'reason': discard_reason, 'source': 'SWE List Email'
                })
                # Track to prevent re-adding
                self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                self.existing_urls.add(self.clean_url(url))
                continue
            
            logger.info(f"  ✅ EMAIL: {company}")
            
            # Visit job page
            result = self.process_job_url(url, company, title)
            
            if result['status'] == 'rejected':
                self.discarded_jobs.append({
                    'company': company, 'title': title, 'location': 'Unknown',
                    'job_type': self.determine_job_type(title),
                    'remote': 'Unknown',
                    'url': result.get('url', url), 'reason': result['reason'], 'source': 'SWE List Email'
                })
                # Track
                self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                self.existing_urls.add(self.clean_url(result.get('url', url)))
            else:
                # Try to extract location
                location = result.get('location', 'Unknown')
                
                # Check if USA
                if location != 'Unknown' and not self.is_usa_location(location):
                    self.discarded_jobs.append({
                        'company': company, 'title': title, 'location': self.extract_city_only(location),
                        'job_type': self.determine_job_type(title),
                        'remote': "Remote" if "remote" in location.lower() else "On Site",
                        'url': result['final_url'], 'reason': 'Non-USA', 'source': 'SWE List Email'
                    })
                    # Track
                    self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                    self.existing_urls.add(self.clean_url(result['final_url']))
                    continue
                
                self.valid_jobs.append({
                    'company': company, 'job_id': result['job_id'], 'title': title,
                    'job_type': self.determine_job_type(title), 
                    'location': self.extract_city_only(location) if location != 'Unknown' else 'Unknown',
                    'remote': "Remote" if location != 'Unknown' and "remote" in location.lower() else "Unknown",
                    'entry_date': self.format_date(), 'url': result['final_url'], 'source': 'SWE List Email'
                })
                # Track
                self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                self.existing_urls.add(self.clean_url(result['final_url']))
    
    def clean_url(self, url):
        """Remove tracking params"""
        if not url:
            return ''
        return url.split('?')[0].lower().rstrip('/')
    
    def remove_emojis(self, text):
        """Remove emojis"""
        if not text:
            return text
        
        emoji_pattern = re.compile(
            "["
            u"\U0001F600-\U0001F64F"
            u"\U0001F300-\U0001F5FF"
            u"\U0001F680-\U0001F6FF"
            u"\U0001F1E0-\U0001F1FF"
            u"\U00002500-\U00002BEF"
            u"\U00002702-\U000027B0"
            u"\U000024C2-\U0001F251"
            u"\U0001f926-\U0001f937"
            u"\U00010000-\U0010ffff"
            u"\u2640-\u2642"
            u"\u2600-\u2B55"
            u"\u200d"
            u"\u23cf"
            u"\u23e9"
            u"\u231a"
            u"\ufe0f"
            u"\u3030"
            u"\u2018-\u201F"
            "]+",
            flags=re.UNICODE
        )
        
        text = emoji_pattern.sub(r'', text)
        text = re.sub(r'[↳🇺🇸🛂\*🔒❌✅📦🎓🚀💼]+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def safe_get_cell(self, row, index, default=''):
        try:
            if len(row) > index:
                return row[index].strip() if row[index] else default
            return default
        except:
            return default
    
    def format_sheet_headers(self, sheet, num_cols=12):
        try:
            col_letter = chr(ord('A') + num_cols - 1)
            col_range = f'A1:{col_letter}1'
            
            sheet.format(col_range, {
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
        except:
            pass
    
    def auto_resize_all_columns_except_url(self, sheet, url_column_index, total_columns):
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
            
            requests = [{
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
            }]
            
            self.spreadsheet.batch_update({"requests": requests})
        except:
            pass
    
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
        except:
            pass
    
    def is_cs_engineering_role(self, title):
        title_lower = title.lower()
        excluded = ['product management', 'marketing', 'sales', 'hr', 'finance']
        for keyword in excluded:
            if keyword in title_lower: return False
        required = ['software', 'swe', 'sde', 'engineer', 'developer', 'data', 'tech', 'automation', 'rpa']
        return any(keyword in title_lower for keyword in required)
    
    def check_page_for_restrictions(self, soup):
        try:
            page_text = soup.get_text().lower()
            restrictions = ['mba required', 'security clearance', 'us citizen only', 'no visa sponsorship', 'does not sponsor']
            for r in restrictions:
                if r in page_text: return r
            return None
        except:
            return None
    
    def extract_job_id_from_page(self, soup, url):
        try:
            page_text = soup.get_text()
            
            patterns = [
                r'\bJR\d{8}\b', r'\bJR\d{7}\b', r'\bJ-\d{8}\b', 
                r'\bREQ\d{6,}\b', r'\bR-\d{8}\b'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text)
                if match: return match.group(0)
            
            if 'workday' in url.lower():
                match = re.search(r'_([A-Z]+\d+)(?:\?|$)', url)
                if match: return match.group(1)
                match = re.search(r'_(\d{4,})(?:\?|$)', url)
                if match: return match.group(1)
            
            if 'greenhouse' in url: 
                match = re.search(r'/jobs/(\d{7,})', url)
                if match: return match.group(1)
            
            if any(x in url for x in ['ashbyhq', 'lever', 'smartrecruiters']):
                match = re.search(r'/([a-f0-9\-]{36})', url)
                if match: return match.group(1)[:13]
            
            return 'N/A'
        except:
            return 'N/A'
    
    def extract_location_from_page(self, soup):
        """Try to extract location from job page"""
        try:
            page_text = soup.get_text()
            
            location_patterns = [
                r'Location:\s*([^|\n]+)',
                r'Office Location:\s*([^|\n]+)',
                r'Work Location:\s*([^|\n]+)'
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, page_text)
                if match:
                    return match.group(1).strip()
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def is_usa_location(self, location):
        if not location: return True
        non_usa = ['canada', 'uk', 'toronto', 'london', 'montreal', 'vancouver']
        return not any(kw in location.lower() for kw in non_usa)
    
    def extract_city_only(self, location):
        if not location or 'remote' in location.lower(): return "Remote"
        return location.split(',')[0].strip() or "Remote"
    
    def determine_job_type(self, title):
        if 'full time' in title.lower(): return "Full Time"
        if 'co-op' in title.lower(): return "Co-op"
        return "Internship"
    
    def format_date(self):
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")
    
    def parse_age(self, age_str):
        match = re.search(r'(\d+)d', age_str.lower()) if age_str else None
        return int(match.group(1)) if match else 999
    
    def is_duplicate(self, company, title, url):
        """2-LAYER DUPLICATE CHECK"""
        job_key = f"{company.lower()}_{title.lower()}"
        if job_key in self.existing_jobs:
            return True
        
        clean_url = self.clean_url(url)
        if clean_url and clean_url in self.existing_urls:
            return True
        
        return False
    
    def process_job_url(self, url, company, title):
        try:
            time.sleep(random.uniform(1.5, 3.0))
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            response = requests.get(url, headers=headers, allow_redirects=True, timeout=15)
            
            if response.status_code != 200:
                return {'status': 'rejected', 'reason': f'HTTP {response.status_code}', 'url': url}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            restriction = self.check_page_for_restrictions(soup)
            
            if restriction:
                return {'status': 'rejected', 'reason': restriction, 'url': response.url}
            
            job_id = self.extract_job_id_from_page(soup, response.url)
            location = self.extract_location_from_page(soup)
            
            return {'status': 'accepted', 'final_url': response.url, 'job_id': job_id, 'location': location}
            
        except:
            return {'status': 'rejected', 'reason': 'Error', 'url': url}
    
    def scrape_simplify_github(self):
        logger.info("📦 Scraping SimplifyJobs GitHub...")
        
        try:
            response = requests.get(SIMPLIFY_URL, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) < 5: continue
                    
                    company_link = cells[0].find('a')
                    if not company_link: continue
                    
                    company = self.remove_emojis(company_link.get_text(strip=True))
                    title = self.remove_emojis(cells[1].get_text(strip=True))
                    location = self.remove_emojis(cells[2].get_text(strip=True).replace('</br>', ' '))
                    age = cells[4].get_text(strip=True)
                    
                    apply_link = cells[3].find('a', href=True)
                    if not apply_link: continue
                    apply_url = apply_link.get('href', '')
                    
                    if not company or not title or not apply_url: continue
                    
                    age_days = self.parse_age(age)
                    if age_days > 1:
                        continue
                    
                    # CHECK DUPLICATE
                    if self.is_duplicate(company, title, apply_url):
                        continue
                    
                    discard_reason = None
                    if not self.is_cs_engineering_role(title):
                        discard_reason = "Non-tech"
                    elif not self.is_usa_location(location):
                        discard_reason = "Non-USA"
                    elif '🔒' in str(cells[3]):
                        discard_reason = "Closed"
                    
                    if discard_reason:
                        self.discarded_jobs.append({
                            'company': company, 'title': title, 'location': self.extract_city_only(location),
                            'job_type': self.determine_job_type(title), 
                            'remote': "Remote" if "remote" in location.lower() else "On Site",
                            'url': apply_url, 'reason': discard_reason, 'source': 'SimplifyJobs'
                        })
                        # Track
                        self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                        self.existing_urls.add(self.clean_url(apply_url))
                        continue
                    
                    logger.info(f"  ✅ GITHUB: {company}")
                    result = self.process_job_url(apply_url, company, title)
                    
                    if result['status'] == 'rejected':
                        self.discarded_jobs.append({
                            'company': company, 'title': title, 'location': self.extract_city_only(location),
                            'job_type': self.determine_job_type(title),
                            'remote': "Remote" if "remote" in location.lower() else "On Site",
                            'url': result.get('url', apply_url), 'reason': result['reason'], 'source': 'SimplifyJobs'
                        })
                        # Track
                        self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                        self.existing_urls.add(self.clean_url(result.get('url', apply_url)))
                    else:
                        self.valid_jobs.append({
                            'company': company, 'job_id': result['job_id'], 'title': title,
                            'job_type': self.determine_job_type(title), 'location': self.extract_city_only(location),
                            'remote': "Remote" if "remote" in location.lower() else "On Site",
                            'entry_date': self.format_date(), 'url': result['final_url'], 'source': 'SimplifyJobs'
                        })
                        # Track
                        self.existing_jobs.add(f"{company.lower()}_{title.lower()}")
                        self.existing_urls.add(self.clean_url(result['final_url']))
            
            logger.info(f"  ✅ GitHub: {len([j for j in self.valid_jobs if j['source'] == 'SimplifyJobs'])} valid")
                
        except Exception as e:
            logger.error(f"  ❌ GitHub error: {e}")
    
    def batch_update_with_links_and_dropdowns(self, sheet, start_row, rows_data, is_valid_sheet=True):
        try:
            if not rows_data:
                return
            
            range_name = f'A{start_row}:L{start_row + len(rows_data) - 1}'
            sheet.update(values=rows_data, range_name=range_name, value_input_option='RAW')
            time.sleep(2)
            
            sheet.format(range_name, {
                'horizontalAlignment': 'CENTER',
                'verticalAlignment': 'MIDDLE',
                'textFormat': {'fontFamily': 'Times New Roman', 'fontSize': 13}
            })
            time.sleep(2)
            
            url_requests = []
            for idx, row_data in enumerate(rows_data):
                row_num = start_row + idx
                url = row_data[5]
                
                if url and url.startswith('http'):
                    url_requests.append({
                        "updateCells": {
                            "range": {
                                "sheetId": sheet.id,
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
            
            if is_valid_sheet:
                dropdown_requests = []
                for idx in range(len(rows_data)):
                    row_num = start_row + idx
                    
                    dropdown_requests.append({
                        "setDataValidation": {
                            "range": {
                                "sheetId": sheet.id,
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
                
                self.apply_status_colors_to_range(start_row, start_row + len(rows_data))
            
        except Exception as e:
            if '429' in str(e):
                logger.warning("⏳ Quota limit, waiting 60s...")
                time.sleep(60)
    
    def add_to_sheet(self):
        if not self.valid_jobs:
            logger.info("No new jobs")
            return
        
        logger.info(f"📊 Adding {len(self.valid_jobs)} jobs...")
        
        batch_size = 10
        
        for batch_start in range(0, len(self.valid_jobs), batch_size):
            batch_end = min(batch_start + batch_size, len(self.valid_jobs))
            batch_jobs = self.valid_jobs[batch_start:batch_end]
            
            rows_data = []
            for idx, job in enumerate(batch_jobs):
                sr_no = self.next_sr_no + batch_start + idx
                
                rows_data.append([
                    sr_no, 'Not Applied', job['company'], job['title'], '',
                    job['url'], job['job_id'], job['job_type'],
                    job['location'], job['remote'], job['entry_date'], job['source']
                ])
            
            start_row = self.next_row + batch_start
            self.batch_update_with_links_and_dropdowns(self.sheet, start_row, rows_data, is_valid_sheet=True)
            self.added += len(rows_data)
            
            time.sleep(3)
        
        self.auto_resize_all_columns_except_url(self.sheet, url_column_index=5, total_columns=12)
        logger.info(f"✅ Added {self.added} jobs")
    
    def add_to_discarded(self):
        if not self.discarded_jobs:
            return
        
        logger.info(f"📋 Adding {len(self.discarded_jobs)} discarded...")
        
        batch_size = 10
        
        for batch_start in range(0, len(self.discarded_jobs), batch_size):
            batch_end = min(batch_start + batch_size, len(self.discarded_jobs))
            batch_jobs = self.discarded_jobs[batch_start:batch_end]
            
            rows_data = []
            for idx, job in enumerate(batch_jobs):
                sr_no = self.next_discarded_sr_no + batch_start + idx
                
                rows_data.append([
                    sr_no, job.get('reason', 'Filtered'), job['company'], job['title'], '',
                    job['url'], 'N/A', job['job_type'],
                    job['location'], job['remote'], self.format_date(), job['source']
                ])
            
            start_row = self.next_discarded_row + batch_start
            self.batch_update_with_links_and_dropdowns(self.discarded_sheet, start_row, rows_data, is_valid_sheet=False)
            self.discarded += len(rows_data)
            
            time.sleep(3)
        
        self.auto_resize_all_columns_except_url(self.discarded_sheet, url_column_index=5, total_columns=12)
        logger.info(f"✅ Discarded {self.discarded} jobs")
    
    def trim_log_to_last_3_runs(self):
        """Keep only last 3 runs in log"""
        try:
            log_file = 'job_scraper.log'
            
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            done_indices = []
            for idx, line in enumerate(lines):
                if '✅ DONE:' in line:
                    done_indices.append(idx)
            
            if len(done_indices) > 3:
                cutoff_index = done_indices[-3]
                
                start_index = cutoff_index
                for i in range(cutoff_index - 1, -1, -1):
                    if '🚀 Unified Job Tracker' in lines[i]:
                        start_index = i
                        break
                
                trimmed_lines = lines[start_index:]
                
                with open(log_file, 'w') as f:
                    f.writelines(trimmed_lines)
        except:
            pass
    
    def run(self):
        # Scrape GitHub
        self.scrape_simplify_github()
        
        # Fetch Gmail emails
        try:
            email_jobs = self.fetch_swelist_emails()
            if email_jobs:
                self.process_email_jobs(email_jobs)
        except Exception as e:
            logger.error(f"  ❌ Email processing failed: {e}")
            logger.info("  Continuing with GitHub jobs only...")
        
        # Summary
        github_count = len([j for j in self.valid_jobs if j['source'] == 'SimplifyJobs'])
        email_count = len([j for j in self.valid_jobs if j['source'] == 'SWE List Email'])
        
        logger.info(f"📊 Summary: {github_count} from GitHub, {email_count} from emails")
        
        # Add to sheets
        self.add_to_sheet()
        self.add_to_discarded()
        
        logger.info("=" * 50)
        logger.info(f"✅ DONE: {self.added} valid, {self.discarded} discarded")
        logger.info("=" * 50)
        
        # Trim log
        self.trim_log_to_last_3_runs()

if __name__ == "__main__":
    UnifiedJobAggregator().run()