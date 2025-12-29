#!/usr/bin/env python3
"""
ULTIMATE JOB SCRAPER - PRODUCTION VERSION
Handles all edge cases with multiple extraction mechanisms
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
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Selenium not installed. Install with: pip install selenium webdriver-manager")

SHEET_NAME = "H1B visa"
WORKSHEET_NAME = "Valid Entries"
DISCARDED_WORKSHEET = "Discarded Entries"
REVIEWED_WORKSHEET = "Reviewed - Not Applied"
SHEETS_CREDS_FILE = 'credentials.json'
GMAIL_CREDS_FILE = 'gmail_credentials.json'
GMAIL_TOKEN_FILE = 'gmail_token.pickle'

SIMPLIFY_URL = 'https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/master/README.md'
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

US_STATES = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
    'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
    'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
    'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS', 'missouri': 'MO',
    'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
    'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH',
    'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 'vermont': 'VT',
    'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
    'district of columbia': 'DC'
}

CANADA_PROVINCES = {'ON', 'QC', 'BC', 'AB', 'MB', 'SK', 'NS', 'NB', 'NL', 'PE', 'YT', 'NT', 'NU'}

CITY_TO_STATE = {
    'new york': 'NY', 'brooklyn': 'NY', 'manhattan': 'NY', 'queens': 'NY', 'bronx': 'NY',
    'los angeles': 'CA', 'san francisco': 'CA', 'san diego': 'CA', 'san jose': 'CA',
    'palo alto': 'CA', 'mountain view': 'CA', 'sunnyvale': 'CA', 'santa clara': 'CA',
    'cupertino': 'CA', 'menlo park': 'CA', 'redwood city': 'CA', 'irvine': 'CA',
    'santa monica': 'CA', 'pasadena': 'CA', 'berkeley': 'CA', 'oakland': 'CA',
    'sacramento': 'CA', 'fresno': 'CA', 'long beach': 'CA', 'anaheim': 'CA',
    'cerritos': 'CA', 'san mateo': 'CA', 'fremont': 'CA', 'san carlos': 'CA',
    'hanover': 'MD', 'des plaines': 'IL', 'la grange park': 'IL',
    'west valley city': 'UT', 'salt lake city': 'UT', 'provo': 'UT',
    'seattle': 'WA', 'bellevue': 'WA', 'redmond': 'WA', 'tacoma': 'WA', 'spokane': 'WA',
    'boston': 'MA', 'cambridge': 'MA', 'somerville': 'MA', 'worcester': 'MA',
    'chicago': 'IL', 'naperville': 'IL', 'aurora': 'IL', 'rockford': 'IL',
    'houston': 'TX', 'dallas': 'TX', 'austin': 'TX', 'san antonio': 'TX',
    'fort worth': 'TX', 'el paso': 'TX', 'arlington': 'TX', 'plano': 'TX',
    'phoenix': 'AZ', 'tucson': 'AZ', 'mesa': 'AZ', 'chandler': 'AZ', 'scottsdale': 'AZ',
    'philadelphia': 'PA', 'pittsburgh': 'PA', 'allentown': 'PA',
    'denver': 'CO', 'colorado springs': 'CO', 'boulder': 'CO',
    'atlanta': 'GA', 'augusta': 'GA', 'columbus': 'GA', 'savannah': 'GA',
    'miami': 'FL', 'orlando': 'FL', 'tampa': 'FL', 'jacksonville': 'FL',
    'fort lauderdale': 'FL', 'tallahassee': 'FL', 'st petersburg': 'FL',
    'detroit': 'MI', 'grand rapids': 'MI', 'warren': 'MI', 'ann arbor': 'MI',
    'minneapolis': 'MN', 'st paul': 'MN', 'rochester': 'MN', 'bloomington': 'MN',
    'shakopee': 'MN',
    'portland': 'OR', 'salem': 'OR', 'eugene': 'OR', 'hillsboro': 'OR',
    'las vegas': 'NV', 'reno': 'NV', 'henderson': 'NV',
    'baltimore': 'MD', 'frederick': 'MD', 'rockville': 'MD', 'gaithersburg': 'MD',
    'germantown': 'MD', 'annapolis': 'MD', 'silver spring': 'MD',
    'milwaukee': 'WI', 'madison': 'WI', 'green bay': 'WI',
    'nashville': 'TN', 'memphis': 'TN', 'knoxville': 'TN',
    'indianapolis': 'IN', 'fort wayne': 'IN', 'evansville': 'IN',
    'columbus': 'OH', 'cleveland': 'OH', 'cincinnati': 'OH', 'toledo': 'OH',
    'charlotte': 'NC', 'raleigh': 'NC', 'durham': 'NC', 'greensboro': 'NC',
    'chapel hill': 'NC', 'wilmington': 'NC',
    'oklahoma city': 'OK', 'tulsa': 'OK', 'norman': 'OK',
    'louisville': 'KY', 'lexington': 'KY',
    'kansas city': 'MO', 'st louis': 'MO', 'springfield': 'MO',
    'omaha': 'NE', 'lincoln': 'NE',
    'albuquerque': 'NM', 'santa fe': 'NM',
    'boise': 'ID', 'meridian': 'ID',
    'des moines': 'IA', 'cedar rapids': 'IA',
    'little rock': 'AR', 'fayetteville': 'AR',
    'providence': 'RI', 'warwick': 'RI',
    'bridgeport': 'CT', 'new haven': 'CT', 'stamford': 'CT', 'hartford': 'CT',
    'newark': 'NJ', 'jersey city': 'NJ', 'princeton': 'NJ', 'hoboken': 'NJ',
    'richmond': 'VA', 'virginia beach': 'VA', 'norfolk': 'VA', 'chesapeake': 'VA',
    'arlington': 'VA', 'alexandria': 'VA', 'mclean': 'VA', 'reston': 'VA',
    'charleston': 'SC', 'columbia': 'SC', 'greenville': 'SC',
    'birmingham': 'AL', 'montgomery': 'AL', 'huntsville': 'AL',
    'new orleans': 'LA', 'baton rouge': 'LA', 'shreveport': 'LA',
    'jackson': 'MS', 'gulfport': 'MS',
    'honolulu': 'HI', 'pearl city': 'HI',
    'anchorage': 'AK', 'fairbanks': 'AK',
    'portland': 'ME', 'lewiston': 'ME',
    'manchester': 'NH', 'nashua': 'NH', 'concord': 'NH',
    'burlington': 'VT', 'essex': 'VT',
    'sioux falls': 'SD', 'rapid city': 'SD',
    'fargo': 'ND', 'bismarck': 'ND',
    'billings': 'MT', 'missoula': 'MT', 'bozeman': 'MT', 'helena': 'MT',
    'cheyenne': 'WY', 'casper': 'WY',
    'newark': 'DE', 'wilmington': 'DE', 'dover': 'DE',
    'chaska': 'MN', 'irving': 'TX', 'sarasota': 'FL'
}

CANADA_CITIES = {
    'toronto': 'ON', 'markham': 'ON', 'ottawa': 'ON', 'mississauga': 'ON',
    'montreal': 'QC', 'quebec city': 'QC', 'quebec': 'QC',
    'vancouver': 'BC', 'victoria': 'BC', 'burnaby': 'BC',
    'calgary': 'AB', 'edmonton': 'AB',
    'winnipeg': 'MB', 'regina': 'SK', 'halifax': 'NS'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

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
        logger.info("=" * 80)
        logger.info("ULTIMATE JOB SCRAPER - PRODUCTION VERSION WITH ALL MECHANISMS")
        logger.info("=" * 80)
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS_FILE, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open(SHEET_NAME)
        self.sheet = spreadsheet.worksheet(WORKSHEET_NAME)
        self.spreadsheet = spreadsheet
        
        # Initialize sheets with proper structure
        headers = self.sheet.row_values(1)
        if len(headers) < 13:
            logger.info("Expanding Valid Entries to 13 columns")
            self.sheet.resize(rows=1000, cols=13)
            time.sleep(1)
        
        if 'Sponsorship' not in headers:
            logger.info("Adding Sponsorship column to Valid Entries")
            self.sheet.update_cell(1, 13, 'Sponsorship')
            time.sleep(1)
            self.format_sheet_headers(self.sheet, num_cols=13)
        
        try:
            self.discarded_sheet = spreadsheet.worksheet(DISCARDED_WORKSHEET)
            disc_headers = self.discarded_sheet.row_values(1)
            
            if len(disc_headers) < 13:
                logger.info("Expanding Discarded sheet to 13 columns")
                self.discarded_sheet.resize(rows=1000, cols=13)
                time.sleep(1)
            
            if 'Sponsorship' not in disc_headers:
                logger.info("Adding Sponsorship to Discarded sheet")
                self.discarded_sheet.update_cell(1, 13, 'Sponsorship')
                time.sleep(1)
                self.format_sheet_headers(self.discarded_sheet, num_cols=13)
        except:
            self.discarded_sheet = spreadsheet.add_worksheet(title=DISCARDED_WORKSHEET, rows=1000, cols=13)
            headers = ['Sr. No.', 'Discard Reason', 'Company', 'Title', 'Date Applied', 
                      'Job URL', 'Job ID', 'Job Type', 'Location', 'Remote?', 'Entry Date', 'Source', 'Sponsorship']
            self.discarded_sheet.append_row(headers)
            self.format_sheet_headers(self.discarded_sheet, num_cols=13)
        
        try:
            self.reviewed_sheet = spreadsheet.worksheet(REVIEWED_WORKSHEET)
            rev_headers = self.reviewed_sheet.row_values(1)
            
            if len(rev_headers) < 12:
                logger.info("Expanding Reviewed sheet to 12 columns")
                self.reviewed_sheet.resize(rows=1000, cols=12)
                time.sleep(1)
            
            if 'Sponsorship' not in rev_headers:
                logger.info("Adding Sponsorship to Reviewed sheet")
                self.reviewed_sheet.update_cell(1, 12, 'Sponsorship')
                time.sleep(1)
                self.format_sheet_headers(self.reviewed_sheet, num_cols=12)
        except:
            self.reviewed_sheet = spreadsheet.add_worksheet(title=REVIEWED_WORKSHEET, rows=1000, cols=12)
            headers = ['Sr. No.', 'Reason', 'Company', 'Title', 'Job URL', 'Job ID', 
                      'Job Type', 'Location', 'Remote?', 'Moved Date', 'Source', 'Sponsorship']
            self.reviewed_sheet.append_row(headers)
            self.format_sheet_headers(self.reviewed_sheet, num_cols=12)
        
        self.remove_duplicates_from_all_sheets()
        
        logger.info("Loading existing jobs from all sheets")
        
        self.existing_jobs = set()
        self.existing_urls = set()
        self.existing_job_ids = set()
        self.processing_lock = set()
        self.processed_jobs_cache = {}
        
        self.next_row = 2
        self.next_sr_no = 1
        self.next_discarded_row = 2
        self.next_discarded_sr_no = 1
        
        # Load from Valid Entries
        main_data = self.sheet.get_all_values()
        for idx, row in enumerate(main_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ''
                job_id = row[6].strip() if len(row) > 6 else ''
                
                if company or title:
                    self.next_row = idx + 1
                    self.next_sr_no = idx
                    if company and title:
                        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                        self.existing_jobs.add(normalized_key)
                        self.processed_jobs_cache[normalized_key] = {
                            'company': company, 'title': title, 'job_id': job_id, 'url': url
                        }
                
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != 'N/A':
                    self.existing_job_ids.add(job_id.lower())
        
        # Load from Discarded
        discarded_data = self.discarded_sheet.get_all_values()
        for idx, row in enumerate(discarded_data[1:], start=2):
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[5].strip() if len(row) > 5 else ''
                job_id = row[6].strip() if len(row) > 6 else ''
                
                if company or title:
                    self.next_discarded_row = idx + 1
                    self.next_discarded_sr_no = idx
                    if company and title:
                        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                        self.existing_jobs.add(normalized_key)
                        self.processed_jobs_cache[normalized_key] = {
                            'company': company, 'title': title, 'job_id': job_id, 'url': url
                        }
                
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != 'N/A':
                    self.existing_job_ids.add(job_id.lower())
        
        # Load from Reviewed
        reviewed_data = self.reviewed_sheet.get_all_values()
        for row in reviewed_data[1:]:
            if len(row) > 2:
                company = row[2].strip()
                title = row[3].strip()
                url = row[4].strip() if len(row) > 4 else ''
                job_id = row[5].strip() if len(row) > 5 else ''
                
                if company and title:
                    normalized_key = self.normalize_for_dedup(f"{company}_{title}")
                    self.existing_jobs.add(normalized_key)
                    self.processed_jobs_cache[normalized_key] = {
                        'company': company, 'title': title, 'job_id': job_id, 'url': url
                    }
                
                if url and 'http' in url:
                    self.existing_urls.add(self.clean_url(url))
                if job_id and job_id != 'N/A':
                    self.existing_job_ids.add(job_id.lower())
        
        logger.info(f"Loaded: {len(self.existing_jobs)} jobs, {len(self.existing_urls)} URLs, {len(self.existing_job_ids)} IDs")
        
        self.added = 0
        self.discarded = 0
        self.valid_jobs = []
        self.discarded_jobs = []
        self.gmail_service = None
        self.selenium_driver = None
        self.ziprecruiter_blocks = False
        
        self.outcomes = {
            'valid': 0,
            'discarded': 0,
            'skipped_duplicate_url': 0,
            'skipped_duplicate_company_title': 0,
            'skipped_non_job': 0,
            'skipped_marketing': 0,
            'failed_http': 0,
            'failed_extraction': 0,
            'low_quality': 0,
            'kept_both_variants': 0,
            'method_standard': 0,
            'method_rotating_agent': 0,
            'method_selenium': 0,
            'method_email_parsed': 0
        }
    
    def normalize_for_dedup(self, text):
        if not text:
            return ''
        text = text.lower()
        text = re.sub(r'[^a-z0-9]', '', text)
        return text
    
    def should_keep_both_jobs(self, new_job, existing_job):
        new_id = new_job.get('job_id', 'N/A')
        existing_id = existing_job.get('job_id', 'N/A')
        
        if new_id != 'N/A' and existing_id != 'N/A':
            if new_id.lower() != existing_id.lower():
                logger.info(f"    → Different job IDs ({new_id} vs {existing_id}) - KEEP BOTH")
                return True
            else:
                logger.info(f"    → Same job ID ({new_id}) - SKIP")
                return False
        
        new_company_norm = self.normalize_for_dedup(new_job.get('company', ''))
        existing_company_norm = self.normalize_for_dedup(existing_job.get('company', ''))
        
        if new_company_norm != existing_company_norm:
            logger.info(f"    → Different companies - KEEP BOTH")
            return True
        
        new_title_norm = self.normalize_for_dedup(new_job.get('title', ''))
        existing_title_norm = self.normalize_for_dedup(existing_job.get('title', ''))
        
        if new_title_norm != existing_title_norm:
            logger.info(f"    → Different titles - KEEP BOTH")
            return True
        
        logger.info(f"    → Same company+title, no distinguishing features - SKIP")
        return False
    
    def remove_duplicates_from_all_sheets(self):
        logger.info("Checking for duplicates in all sheets")
        
        total_removed = 0
        total_removed += self.remove_duplicates_from_sheet(self.sheet, "Valid", 2, 3, 5, 6)
        total_removed += self.remove_duplicates_from_sheet(self.discarded_sheet, "Discarded", 2, 3, 5, 6)
        total_removed += self.remove_duplicates_from_sheet(self.reviewed_sheet, "Reviewed", 2, 3, 4, 5)
        
        if total_removed > 0:
            logger.info(f"Removed {total_removed} total duplicates")
        else:
            logger.info("No duplicates found")
    
    def remove_duplicates_from_sheet(self, sheet, name, c_idx, t_idx, u_idx, j_idx):
        try:
            all_data = sheet.get_all_values()
            if len(all_data) <= 1:
                return 0
            
            seen_jobs = set()
            seen_urls = set()
            seen_job_ids = set()
            rows_to_delete = []
            
            for idx, row in enumerate(all_data[1:], start=2):
                if len(row) <= max(c_idx, t_idx, u_idx, j_idx):
                    continue
                
                company = row[c_idx].strip()
                title = row[t_idx].strip()
                url = row[u_idx].strip() if len(row) > u_idx else ''
                job_id = row[j_idx].strip() if len(row) > j_idx else ''
                
                if not company and not title:
                    continue
                
                job_key = self.normalize_for_dedup(f"{company}_{title}")
                url_key = self.clean_url(url) if url and 'http' in url else None
                job_id_key = job_id.lower() if job_id and job_id != 'N/A' else None
                
                is_dup = (job_key in seen_jobs or 
                         (url_key and url_key in seen_urls) or
                         (job_id_key and job_id_key in seen_job_ids))
                
                if is_dup:
                    rows_to_delete.append(idx)
                else:
                    seen_jobs.add(job_key)
                    if url_key:
                        seen_urls.add(url_key)
                    if job_id_key:
                        seen_job_ids.add(job_id_key)
            
            if not rows_to_delete:
                return 0
            
            logger.info(f"  Removing {len(rows_to_delete)} duplicates from {name} sheet")
            
            for row_num in reversed(rows_to_delete):
                sheet.delete_rows(row_num)
                time.sleep(0.3)
            
            remaining = sheet.get_all_values()
            for idx in range(1, len(remaining)):
                sheet.update_cell(idx + 1, 1, idx)
                time.sleep(0.3)
            
            return len(rows_to_delete)
        except Exception as e:
            logger.error(f"Error removing duplicates from {name}: {e}")
            return 0
    
    def authenticate_gmail(self):
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
        logger.info("Gmail authenticated successfully")
    
    def fetch_page_comprehensive(self, url, email_html=None, sender=None):
        """
        Try 4 mechanisms to get page content:
        1. Standard requests
        2. Rotating User-Agents
        3. Selenium
        4. Parse from email
        """
        
        logger.info(f"    MECHANISM 1: Standard request")
        response = self.try_standard_request(url)
        if response and response.status_code == 200:
            logger.info(f"    ✓ Success with standard request")
            self.outcomes['method_standard'] += 1
            return response, response.url, 'standard'
        elif response:
            logger.info(f"    ✗ Failed: HTTP {response.status_code}")
        
        logger.info(f"    MECHANISM 2: Rotating User-Agents")
        response = self.try_rotating_user_agents(url)
        if response and response.status_code == 200:
            logger.info(f"    ✓ Success with rotating UA")
            self.outcomes['method_rotating_agent'] += 1
            return response, response.url, 'rotating_agent'
        elif response:
            logger.info(f"    ✗ Failed: HTTP {response.status_code}")
        
        if SELENIUM_AVAILABLE and ('ziprecruiter' in url.lower() or self.ziprecruiter_blocks):
            logger.info(f"    MECHANISM 3: Selenium (headless Chrome)")
            html, final_url = self.try_selenium(url)
            if html:
                logger.info(f"    ✓ Success with Selenium")
                self.outcomes['method_selenium'] += 1
                
                soup = BeautifulSoup(html, 'html.parser')
                mock_response = type('obj', (object,), {
                    'text': html,
                    'status_code': 200,
                    'url': final_url
                })()
                return mock_response, final_url, 'selenium'
            else:
                logger.info(f"    ✗ Selenium failed")
        
        if email_html:
            logger.info(f"    MECHANISM 4: Parse from email content")
            job_data = self.extract_job_from_email_content(email_html, url, sender)
            if job_data:
                logger.info(f"    ✓ Success parsing from email")
                self.outcomes['method_email_parsed'] += 1
                return None, url, 'email_parsed', job_data
            else:
                logger.info(f"    ✗ Email parsing failed")
        
        logger.info(f"    ✗ ALL MECHANISMS FAILED")
        return None, None, 'all_failed'
    
    def try_standard_request(self, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            response = requests.get(url, headers=headers, allow_redirects=True, timeout=20)
            return response
        except Exception as e:
            logger.info(f"      Exception: {str(e)[:50]}")
            return None
    
    def try_rotating_user_agents(self, url):
        for idx, ua in enumerate(USER_AGENTS[:3], 1):
            try:
                logger.info(f"      Trying User-Agent {idx}/3")
                headers = {
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                }
                response = requests.get(url, headers=headers, allow_redirects=True, timeout=20)
                
                if response.status_code == 200:
                    return response
                
                time.sleep(1)
                
            except Exception as e:
                logger.info(f"      UA {idx} exception: {str(e)[:30]}")
                continue
        
        return None
    
    def try_selenium(self, url):
        driver = None
        try:
            if not SELENIUM_AVAILABLE:
                return None, None
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-bots')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument(f'user-agent={USER_AGENTS[0]}')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            driver.set_page_load_timeout(30)
            driver.get(url)
            
            time.sleep(3)
            
            final_url = driver.current_url
            html = driver.page_source
            
            return html, final_url
            
        except Exception as e:
            logger.info(f"      Selenium exception: {str(e)[:80]}")
            return None, None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def extract_job_from_email_content(self, email_html, url, sender):
        """
        Extract job data directly from email HTML
        Sender-specific parsers for known formats
        """
        
        try:
            soup = BeautifulSoup(email_html, 'html.parser')
            
            sender_lower = sender.lower() if sender else ''
            
            # ZipRecruiter parser
            if 'ziprecruiter' in sender_lower or 'ziprecruiter' in url.lower():
                return self.parse_ziprecruiter_email(soup, url)
            
            # Adzuna parser
            elif 'adzuna' in sender_lower or 'adzuna' in url.lower():
                return self.parse_adzuna_email(soup, url)
            
            # SWE List parser
            elif 'swelist' in sender_lower:
                return self.parse_swelist_email(soup, url)
            
            # Jobright parser
            elif 'jobright' in sender_lower or 'jobright' in url.lower():
                return self.parse_jobright_email(soup, url)
            
            # Generic parser (fallback)
            else:
                return self.parse_generic_email(soup, url)
                
        except Exception as e:
            logger.info(f"      Email parsing exception: {str(e)[:80]}")
            return None
    
    def parse_ziprecruiter_email(self, soup, url):
        """
        ZipRecruiter PRECISE - Plain text parsing
        Title
        Company • Location • Type
        $X / hr
        """
        try:
            logger.info(f"      ZipRecruiter PRECISE parser")
            
            # Plain text is more reliable
            all_text = soup.get_text()
            lines = [l.strip() for l in all_text.split('\n') if l.strip()]
            
            # DIAGNOSTIC
            logger.info(f"        Email text length: {len(all_text)} chars")
            logger.info(f"        Number of lines: {len(lines)}")
            
            # Find URL in text
            url_short = url[:60]
            logger.info(f"        Looking for: {url_short}")
            
            url_index = -1
            for i, line in enumerate(lines):
                if url_short in line or ('/km/' in url and '/km/' in line) or ('/ekm/' in url and '/ekm/' in line):
                    url_index = i
                    logger.info(f"        Found URL at line {i}")
                    break
            
            if url_index == -1:
                logger.info(f"        ✗ URL NOT FOUND in email")
                return None
            
            title = 'Unknown'
            company = 'Unknown'
            location = 'Unknown'
            
            # Title: 1-5 lines before URL, has intern/engineer
            for i in range(max(0, url_index - 5), url_index):
                line = lines[i]
                if 10 < len(line) < 150:
                    if any(kw in line.lower() for kw in ['intern', 'engineer', 'developer', 'programmer']):
                        if '•' not in line and 'View' not in line and 'Apply' not in line:
                            title = line
                            logger.info(f"        Title: {title[:80]}")
                            break
            
            # Company • Location: 1-5 lines after URL
            for i in range(url_index, min(len(lines), url_index + 5)):
                line = lines[i]
                
                if '•' in line and '$' not in line:
                    parts = [p.strip() for p in line.split('•')]
                    if len(parts) >= 2:
                        company = parts[0]
                        location_raw = parts[1]
                        
                        # Remove work type
                        location = re.sub(r'\s*(Hybrid|Remote|In-person|On-site)$', '', location_raw, flags=re.I).strip()
                        
                        logger.info(f"        Company: {company}")
                        logger.info(f"        Location: {location}")
                        break
            
            if title == 'Unknown':
                logger.info(f"        ✗ Title NOT FOUND")
                logger.info(f"        Searched lines {max(0, url_index - 5)} to {url_index}")
                return None
            
            return {
                'company': company,
                'title': title,
                'location': location,
                'url': url,
                'job_id': 'N/A',
                'remote': self.infer_remote_from_text(location),
                'sponsorship': 'Unknown (Email)',
                'source_method': 'email_parsed'
            }
        except Exception as e:
            logger.info(f"      ZipRecruiter error: {str(e)[:80]}")
            return None

    def parse_adzuna_email(self, soup, url):
        """
        Parse Adzuna email format
        """
        try:
            logger.info(f"      Using Adzuna-specific parser")
            
            link = soup.find('a', href=re.compile(r'adzuna'))
            if not link:
                return None
            
            title = link.get_text().strip()
            
            parent = link.find_parent(['div', 'td', 'table'])
            company = 'Unknown'
            location = 'Unknown'
            
            if parent:
                text = parent.get_text()
                lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 3]
                
                for line in lines:
                    if ',' in line and len(line) < 60:
                        location = line
                        break
                
                for line in lines:
                    if line != title and len(line) < 50 and not ',' in line:
                        company = line
                        break
            
            if company == 'Unknown':
                company = self.extract_company_from_domain(url)
            
            return {
                'company': company,
                'title': title,
                'location': location,
                'url': url,
                'job_id': 'N/A',
                'remote': self.infer_remote_from_text(location),
                'sponsorship': 'Unknown',
                'source_method': 'email_parsed'
            }
            
        except Exception as e:
            logger.info(f"      Adzuna parser error: {str(e)[:60]}")
            return None
    
    def parse_swelist_email(self, soup, url):
        """
        Parse SWE List email - usually has company and title in table
        """
        try:
            logger.info(f"      Using SWE List parser")
            
            link = soup.find('a', href=url)
            if not link:
                return None
            
            title = link.get_text().strip()
            
            tr = link.find_parent('tr')
            if tr:
                cells = tr.find_all('td')
                if len(cells) >= 2:
                    company = cells[0].get_text().strip()
                    if not title:
                        title = cells[1].get_text().strip()
                    
                    location = cells[2].get_text().strip() if len(cells) > 2 else 'Unknown'
                    
                    return {
                        'company': company,
                        'title': title,
                        'location': location,
                        'url': url,
                        'job_id': 'N/A',
                        'remote': self.infer_remote_from_text(location),
                        'sponsorship': 'Unknown',
                        'source_method': 'email_parsed'
                    }
            
            return None
            
        except Exception as e:
            logger.info(f"      SWE List parser error: {str(e)[:60]}")
            return None
    
    def parse_jobright_email(self, soup, url):
        """
        Jobright PRECISE parser - Based on actual HTML structure
        <p id="job-company-name">Western Union</p>
        <p id="job-title"><a href="...">Software Engineer Intern</a></p>
        <p id="job-tag">Denver, CO</p>
        <span id="job-time-posted">26 minutes ago</span>
        """
        try:
            logger.info(f"      Jobright PRECISE parser")
            
            # Find link with this URL
            url_base = url.split('?')[0]
            logger.info(f"        Looking for URL: {url_base[:80]}")
            
            link = soup.find('a', href=re.compile(re.escape(url_base)))
            
            if not link:
                logger.info(f"        ✗ Link NOT FOUND in email")
                return None
            
            logger.info(f"        ✓ Link found")
            
            # Get parent table/div
            container = link.find_parent('table')
            if not container:
                container = link.find_parent('div')
            
            if not container:
                logger.info(f"        ✗ Container NOT FOUND")
                return None
            
            logger.info(f"        ✓ Container found")
            
            # Company: <p id="job-company-name">
            company_elem = container.find('p', id='job-company-name')
            company = company_elem.get_text().strip() if company_elem else 'Unknown'
            
            logger.info(f"        Company: {company}")
            
            # Title: Link text in <p id="job-title">
            title_p = container.find('p', id='job-title')
            if title_p:
                title_link = title_p.find('a')
                title = title_link.get_text().strip() if title_link else title_p.get_text().strip()
            else:
                title = link.get_text().strip()
            
            logger.info(f"        Title: {title[:80]}")
            
            # Location: <p id="job-tag"> with City, ST
            location = 'Unknown'
            job_tags = container.find_all('p', id='job-tag')
            
            for tag in job_tags:
                text = tag.get_text().strip()
                
                # Skip salary
                if '$' in text or '/hr' in text or '/wk' in text:
                    continue
                # Skip referrals
                if 'referral' in text.lower():
                    continue
                
                # Location: has comma OR is "Remote"
                if ',' in text:
                    location = text
                    logger.info(f"        Location: {location}")
                    break
                elif text == 'Remote':
                    location = 'Remote'
                    logger.info(f"        Location: Remote")
                    break
            
            # Check time: <span id="job-time-posted">
            time_span = container.find('span', id='job-time-posted')
            if time_span:
                time_text = time_span.get_text().strip()
                if not self.is_recent_posting(time_text):
                    logger.info(f"        ✗ Too old: {time_text}")
                    return None
                logger.info(f"        Posted: {time_text}")
            
            return {
                'company': company,
                'title': title,
                'location': location,
                'url': url,
                'job_id': 'N/A',
                'remote': self.infer_remote_from_text(location),
                'sponsorship': 'Unknown (Email)',
                'source_method': 'email_parsed'
            }
        except Exception as e:
            logger.info(f"      Jobright error: {str(e)[:80]}")
            return None

    def parse_generic_email(self, soup, url):
        """
        Generic email parser - tries common patterns
        """
        try:
            logger.info(f"      Using generic parser")
            
            link = soup.find('a', href=url)
            if not link:
                return None
            
            title = link.get_text().strip()
            
            parent = link.find_parent(['div', 'td', 'tr'])
            if parent:
                text = parent.get_text()
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                
                company = 'Unknown'
                location = 'Unknown'
                
                for line in lines:
                    if ',' in line and len(line) < 80:
                        location = line
                    elif line != title and len(line) < 50:
                        company = line
                
                if company == 'Unknown':
                    company = self.extract_company_from_domain(url)
                
                return {
                    'company': company,
                    'title': title,
                    'location': location,
                    'url': url,
                    'job_id': 'N/A',
                    'remote': self.infer_remote_from_text(location + ' ' + title),
                    'sponsorship': 'Unknown',
                    'source_method': 'email_parsed'
                }
            
            return None
            
        except Exception as e:
            logger.info(f"      Generic parser error: {str(e)[:60]}")
            return None
    
    def infer_remote_from_text(self, text):
        if not text:
            return 'Unknown'
        
        text_lower = text.lower()
        
        if 'remote' in text_lower:
            return 'Remote'
        if 'hybrid' in text_lower:
            return 'Hybrid'
        if 'on-site' in text_lower or 'onsite' in text_lower:
            return 'On Site'
        
        return 'Unknown'
    

    
    def is_recent_posting(self, time_text):
        """Check if posted within 3 days"""
        if not time_text:
            return True
        
        time_lower = time_text.lower()
        
        if 'minute' in time_lower or 'hour' in time_lower:
            return True
        
        days_match = re.search(r'(\d+)\s*days?\s+ago', time_lower)
        if days_match:
            days = int(days_match.group(1))
            return days <= 3
        
        posted_match = re.search(r'posted\s+(\d+)\s*days?\s+ago', time_lower)
        if posted_match:
            days = int(posted_match.group(1))
            return days <= 3
        
        return True
    
    def is_external_job_board(self, url):
        """Check if external board vs platform page"""
        if not url:
            return False
        
        url_lower = url.lower()
        
        # NOT external
        if 'ziprecruiter.com' in url_lower or 'jobright.ai' in url_lower:
            return False
        
        # IS external
        external = [
            'greenhouse', 'lever.co', 'workday', 'paylocity', 'icims',
            'ashbyhq', 'smartrecruiters', 'bamboohr', 'buildsubmarines',
            'recruiting.', 'careers.', 'jobs.', 'apply.'
        ]
        
        return any(board in url_lower for board in external)
    def detect_sender_name(self, msg_headers):
        """
        Extract sender name from email headers
        """
        for header in msg_headers:
            if header['name'] == 'From':
                from_field = header['value']
                
                if 'ziprecruiter' in from_field.lower():
                    return 'ZipRecruiter'
                elif 'adzuna' in from_field.lower():
                    return 'Adzuna'
                elif 'swelist' in from_field.lower():
                    return 'SWE List'
                elif 'jobright' in from_field.lower():
                    return 'Jobright'
                elif 'fursah' in from_field.lower():
                    return 'Fursah'
                else:
                    match = re.search(r'(?:from|at)\s+([A-Za-z\s]+)', from_field, re.I)
                    if match:
                        return match.group(1).strip()
                    
                    return 'Email'
        
        return 'Email'
    

    
    def extract_from_jobright_page(self, soup, url):
        """
        JOBRIGHT 4-LAYER EXTRACTION
        Jobright pages have all data including sponsorship badges
        """
        
        logger.info(f"    ▶ JOBRIGHT EXTRACTION (4 layers)")
        
        company = None
        title = None
        location = None
        sponsorship = 'Unknown'
        remote = 'Unknown'
        
        page_text = soup.get_text()
        
        # === LAYER 1: Primary structure ===
        try:
            # Title: H1 (always the job title)
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text().strip()
                logger.info(f"      L1 Title: {title[:80]}")
            
            # Company: H2/H3 without job keywords
            for tag in ['h2', 'h3']:
                elem = soup.find(tag)
                if elem:
                    text = elem.get_text().strip()
                    if 5 < len(text) < 60:
                        if not any(w in text.lower() for w in ['intern', 'engineer', 'summer', '2026', 'developer']):
                            company = text
                            logger.info(f"      L1 Company: {company}")
                            break
            
            # Location: City, ST pattern
            loc_patterns = [
                r'\b(San Diego),\s*(CA)\b',
                r'\b(Austin),\s*(TX)\b',
                r'\b(Cary),\s*(NC)\b',
                r'\b(New York),\s*(United States)\b',
                r'\b(Foster City),\s*(CA)\b',
                r'\b(Hanover),\s*(Maryland)\b',
                r'\b(Buffalo),\s*(New York)\b',
                r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})\b'
            ]
            
            for pattern in loc_patterns:
                match = re.search(pattern, page_text[:2000])
                if match:
                    if len(match.groups()) == 2:
                        if 'United States' in match.group(2) or 'Maryland' in match.group(2) or 'New York' in match.group(2):
                            # Full state name
                            city = match.group(1)
                            state_full = match.group(2)
                            # Try to convert to abbreviation
                            location = city  # Will be formatted later
                        else:
                            location = f"{match.group(1)}, {match.group(2)}"
                    else:
                        location = match.group(0)
                    logger.info(f"      L1 Location: {location}")
                    break
            
            if not location and 'Remote' in page_text[:1500]:
                location = 'Remote'
                logger.info(f"      L1 Location: Remote")
            
            # Sponsorship badges (CRITICAL for Jobright!)
            if 'H1B Sponsor Likely' in page_text or 'H-1B Sponsor Likely' in page_text or 'H1-B Sponsor Likely' in page_text:
                sponsorship = 'Yes'
                logger.info(f"      L1 Sponsorship: Yes (badge found)")
            elif 'No H1B' in page_text or 'No H-1B' in page_text or 'No H1-B' in page_text:
                sponsorship = 'No'
                logger.info(f"      L1 Sponsorship: No (badge found)")
            
            # Work type
            if 'Onsite' in page_text[:1500] or 'On-site' in page_text[:1500]:
                remote = 'On Site'
            elif 'Hybrid' in page_text[:1500]:
                remote = 'Hybrid'
            elif 'Remote' in page_text[:1500]:
                remote = 'Remote'
            
            if company and title:
                logger.info(f"    ✓ LAYER 1 SUCCESS")
                return {
                    'company': company,
                    'title': title,
                    'location': location if location else 'Unknown',
                    'sponsorship': sponsorship,
                    'remote': remote
                }
        except Exception as e:
            logger.info(f"      L1 failed: {str(e)[:50]}")
        
        # === LAYER 2: Class-based ===
        try:
            logger.info(f"    ▶ LAYER 2: Classes")
            
            company_elem = soup.find(['div', 'span'], class_=re.compile('company|employer', re.I))
            if company_elem:
                company = company_elem.get_text().strip()
            
            if company and title:
                logger.info(f"    ✓ LAYER 2 SUCCESS")
                return {
                    'company': company,
                    'title': title,
                    'location': location if location else 'Unknown',
                    'sponsorship': sponsorship,
                    'remote': remote
                }
        except:
            pass
        
        # === LAYER 3: Line analysis ===
        try:
            logger.info(f"    ▶ LAYER 3: Lines")
            
            lines = [l.strip() for l in page_text.split('\n') if l.strip()]
            
            for line in lines[:50]:
                if re.match(r'^[A-Z][A-Za-z\s&,\.]+$', line) and len(line) < 60:
                    if not any(w in line.lower() for w in ['intern', 'engineer', 'summer']):
                        company = line
                        break
            
            if company and title:
                logger.info(f"    ✓ LAYER 3 SUCCESS")
                return {
                    'company': company,
                    'title': title,
                    'location': location if location else 'Unknown',
                    'sponsorship': sponsorship,
                    'remote': remote
                }
        except:
            pass
        
        # === LAYER 4: JSON-LD ===
        try:
            logger.info(f"    ▶ LAYER 4: JSON")
            
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    if data.get('hiringOrganization'):
                        company = data['hiringOrganization'].get('name')
                    if data.get('title'):
                        title = data.get('title')
                    
                    if company and title:
                        logger.info(f"    ✓ LAYER 4 SUCCESS")
                        return {
                            'company': company,
                            'title': title,
                            'location': location if location else 'Unknown',
                            'sponsorship': sponsorship,
                            'remote': remote
                        }
        except:
            pass
        
        logger.info(f"    ✗ All 4 layers failed")
        return None
    def fetch_swelist_emails(self):
        try:
            if not self.gmail_service:
                self.authenticate_gmail()
            
            query = 'label:"Job Hunt" newer_than:1d'
            
            logger.info("Fetching emails with 'Job Hunt' label from last 24 hours")
            
            results = self.gmail_service.users().messages().list(
                userId='me',
                q=query,
                maxResults=50
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.info("No labeled emails found")
                return []
            
            logger.info(f"Found {len(messages)} labeled emails")
            
            all_email_data = []
            
            for idx, message in enumerate(messages, 1):
                logger.info(f"\nProcessing email {idx}/{len(messages)}")
                
                msg = self.gmail_service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()
                
                headers = msg['payload'].get('headers', [])
                
                subject = ''
                for header in headers:
                    if header['name'] == 'Subject':
                        subject = header['value']
                        break
                
                sender = self.detect_sender_name(headers)
                
                logger.info(f"  From: {sender}")
                logger.info(f"  Subject: {subject[:100]}")
                
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
                    urls = self.extract_job_urls_from_email(html_content)
                    logger.info(f"  Extracted {len(urls)} job URLs")
                    
                    for url in urls:
                        all_email_data.append({
                            'url': url,
                            'email_html': html_content,
                            'sender': sender
                        })
                else:
                    logger.info(f"  No HTML content found")
            
            logger.info(f"\nTotal: {len(all_email_data)} job URLs from all emails")
            return all_email_data
            
        except FileNotFoundError:
            logger.warning("Gmail credentials file not found")
            return []
        except Exception as e:
            logger.error(f"Gmail error: {e}")
            return []
    
    def extract_job_urls_from_email(self, email_html):
        soup = BeautifulSoup(email_html, 'html.parser')
        
        job_urls = []
        all_links = soup.find_all('a', href=True)
        
        job_board_domains = [
            'greenhouse', 'lever.co', 'workday', 'ashbyhq', 'smartrecruiters',
            'icims.com', 'myworkdayjobs', 'jobs.lever.co', 'boards.greenhouse.io',
            'simplify.jobs', 'linkedin.com/jobs', 'indeed.com', 'glassdoor.com',
            'angellist.com', 'wellfound.com', 'monster.com',
            'dice.com', 'builtin.com', 'ycombinator.com/jobs', 'stackoverflow.com/jobs',
            'jobs.github.com', 'careers.', 'apply.workable.com', 'breezy.hr',
            'recruiting.', 'talentify', 'workable', 'jobvite', 'ultipro',
            'paylocity', 'paycomonline', 'bamboohr', 'fountain.com',
            # NEW DOMAINS
            'ziprecruiter', 'ziprecruiter.com',
            'adzuna', 'adzuna.com',
            'jobright', 'jobright.ai',
            'fursah', 'fursah.com'
        ]
        
        for link in all_links:
            url = link.get('href', '')
            
            if not url.startswith('http'):
                continue
            
            is_job_board = any(domain in url.lower() for domain in job_board_domains)
            
            if is_job_board:
                # Filter out non-job URLs
                if not self.is_non_job_url(url):
                    job_urls.append(url)
        
        return list(set(job_urls))
    
    def is_valid_job_title(self, title):
        if not title or title == 'Unknown':
            return False, "No title"
        
        title_lower = title.lower()
        
        marketing_phrases = [
            'meet your', 'join our team', 'learn more', 'discover how',
            'explore our', 'about our', 'contact us', 'get started',
            'find out', 'see how', 'welcome to', 'introducing'
        ]
        
        for phrase in marketing_phrases:
            if phrase in title_lower:
                return False, f"Marketing: '{phrase}'"
        
        job_role_words = [
            'intern', 'engineer', 'developer', 'analyst', 'scientist',
            'designer', 'manager', 'specialist', 'coordinator', 'associate',
            'consultant', 'architect', 'researcher', 'technician', 'administrator'
        ]
        
        has_job_word = any(word in title_lower for word in job_role_words)
        
        if not has_job_word:
            return False, "No job keywords"
        
        if len(title) < 10:
            return False, "Too short"
        
        generic_service = ['copilot', 'platform', 'service', 'tool', 'portal']
        for word in generic_service:
            if word in title_lower and 'engineer' not in title_lower:
                return False, f"Service page: '{word}'"
        
        return True, None
    

    
    def is_non_job_url(self, url):
        """Filter non-job links"""
        if not url:
            return True
        
        url_lower = url.lower()
        
        non_job = [
            '/unsubscribe', '/my-alerts', '/blog', '/prepper',
            'twitter.com', 'facebook.com', 'play.google.com',
            'chromewebstore', '/privacy', '/terms', '/opt_out',
            '?retarget='
        ]
        
        if any(p in url_lower for p in non_job):
            return True
        
        # Adzuna: Only /land/ad/ are jobs
        if 'adzuna.com' in url_lower and '/land/ad/' not in url_lower:
            return True
        
        return False
    def calculate_quality_score(self, job_data):
        score = 0
        
        if job_data.get('company') and job_data['company'] != 'Unknown':
            score += 2
        
        if job_data.get('location') and job_data['location'] != 'Unknown':
            score += 2
        
        if job_data.get('job_id') and job_data['job_id'] != 'N/A':
            score += 1
        
        if job_data.get('title') and 15 < len(job_data['title']) < 120:
            score += 1
        
        if job_data.get('sponsorship') and job_data['sponsorship'] not in ['Unknown', 'Unknown (Email)']:
            score += 1
        
        return score
    
    def process_email_jobs(self, email_data_list):
        if not email_data_list:
            return
        
        logger.info("=" * 80)
        logger.info(f"PROCESSING {len(email_data_list)} EMAIL URLS - COMPREHENSIVE MECHANISM CASCADE")
        logger.info("=" * 80)
        
        for idx, email_data in enumerate(email_data_list, 1):
            url = email_data['url']
            email_html = email_data['email_html']
            sender = email_data['sender']
            
            logger.info(f"\n[{idx}/{len(email_data_list)}] URL: {url[:120]}")
            logger.info(f"  Sender: {sender}")
            
            clean_url_original = self.clean_url(url)
            
            # === DIAGNOSTIC LOGGING ===
            logger.info(f"  ┌─ DIAGNOSTIC ─────────────────────────────────")
            logger.info(f"  │ Original cleaned: {clean_url_original[:80]}")
            logger.info(f"  │ In existing_urls? {clean_url_original in self.existing_urls}")
            logger.info(f"  │ In processing_lock? {clean_url_original in self.processing_lock}")
            logger.info(f"  │ Processing_lock size: {len(self.processing_lock)}")
            if clean_url_original in self.existing_urls:
                logger.info(f"  │ → This URL was loaded from existing sheet!")
            if clean_url_original in self.processing_lock:
                logger.info(f"  │ → This URL was processed earlier in THIS run!")
            logger.info(f"  └──────────────────────────────────────────────")
            
            if clean_url_original in self.processing_lock:
                logger.info(f"  → SKIP: Already in processing lock")
                self.outcomes['skipped_duplicate_url'] += 1
                continue
            
            if clean_url_original in self.existing_urls:
                logger.info(f"  → SKIP: Original URL already exists")
                self.outcomes['skipped_duplicate_url'] += 1
                continue
            
            self.processing_lock.add(clean_url_original)
            
            result = self.process_single_job_comprehensive(url, email_html, sender, idx, len(email_data_list))
            
            if not result:
                continue
            
            decision = result['decision']
            
            if decision == 'skip':
                reason_type = result.get('reason_type', 'non_job')
                self.outcomes[f'skipped_{reason_type}'] += 1
                logger.info(f"  → SKIP: {result.get('reason')}")
                continue
            
            elif decision == 'discard':
                self.discarded_jobs.append({
                    'company': result['company'], 'title': result['title'], 
                    'location': result['location'],
                    'job_type': self.determine_job_type(result['title']),
                    'remote': result['remote'],
                    'url': result['url'], 'job_id': result['job_id'], 
                    'reason': result['reason'], 'source': result['source'],
                    'sponsorship': result['sponsorship']
                })
                
                normalized_key = self.normalize_for_dedup(f"{result['company']}_{result['title']}")
                self.existing_jobs.add(normalized_key)
                self.existing_urls.add(self.clean_url(result['url']))
                
                if result['job_id'] != 'N/A':
                    self.existing_job_ids.add(result['job_id'].lower())
                
                self.outcomes['discarded'] += 1
                logger.info(f"  ✗ DISCARDED: {result['reason']}")
            
            elif decision == 'valid':
                self.valid_jobs.append({
                    'company': result['company'], 'job_id': result['job_id'], 'title': result['title'],
                    'job_type': self.determine_job_type(result['title']), 
                    'location': result['location'],
                    'remote': result['remote'],
                    'entry_date': self.format_date(), 
                    'url': result['url'], 
                    'source': result['source'],
                    'sponsorship': result['sponsorship']
                })
                
                normalized_key = self.normalize_for_dedup(f"{result['company']}_{result['title']}")
                self.existing_jobs.add(normalized_key)
                self.processed_jobs_cache[normalized_key] = {
                    'company': result['company'],
                    'title': result['title'],
                    'job_id': result['job_id'],
                    'url': result['url']
                }
                self.existing_urls.add(self.clean_url(result['url']))
                
                if result['job_id'] != 'N/A':
                    self.existing_job_ids.add(result['job_id'].lower())
                
                self.outcomes['valid'] += 1
                logger.info(f"  ✓ VALID: Added to valid_jobs")
        
        logger.info("\n" + "=" * 80)
        logger.info("EMAIL PROCESSING COMPLETE")
        logger.info("=" * 80)
    
    def process_single_job_comprehensive(self, url, email_html, sender, current_idx, total):
        """
        Process a single job with comprehensive mechanism cascade
        Returns: decision dict or None
        """
        try:
            time.sleep(random.uniform(1.5, 2.5))
            
            # Try comprehensive fetch (4 mechanisms)
            fetch_result = self.fetch_page_comprehensive(url, email_html, sender)
            
            if len(fetch_result) == 4:
                _, final_url, method, email_parsed_data = fetch_result
                
                if method == 'email_parsed':
                    return self.process_email_parsed_job(email_parsed_data, sender)
            
            elif len(fetch_result) == 3:
                response, final_url, method = fetch_result
                
                if not response:
                    self.outcomes['failed_http'] += 1
                    logger.info(f"  → FAIL: All mechanisms failed")
                    return None
                
                clean_final = self.clean_url(final_url)
                clean_original = self.clean_url(url)
                
                # === DIAGNOSTIC LOGGING (FINAL URL) ===
                logger.info(f"  ┌─ FINAL URL DIAGNOSTIC ───────────────────────")
                logger.info(f"  │ Final URL: {final_url[:80]}")
                logger.info(f"  │ Final cleaned: {clean_final[:80]}")
                logger.info(f"  │ In existing_urls? {clean_final in self.existing_urls}")
                logger.info(f"  │ In processing_lock? {clean_final in self.processing_lock}")
                logger.info(f"  │ Same as original? {clean_final == clean_original}")
                if clean_final in self.processing_lock:
                    logger.info(f"  │ → URL was processed earlier!")
                if clean_final in self.existing_urls:
                    logger.info(f"  │ → URL exists in sheet!")
                logger.info(f"  └──────────────────────────────────────────────")
                
                # CRITICAL FIX: Don't skip if final == original (it's the SAME URL, not a duplicate!)
                if clean_final in self.processing_lock and clean_final != clean_original:
                    self.outcomes['skipped_duplicate_url'] += 1
                    logger.info(f"  → SKIP: Final URL in processing lock")
                    return None
                
                if clean_final in self.existing_urls:
                    self.outcomes['skipped_duplicate_url'] += 1
                    logger.info(f"  → SKIP: Final URL duplicate")
                    self.existing_urls.add(self.clean_url(url))
                    return None
                
                self.processing_lock.add(clean_final)
                
                logger.info(f"  Final URL: {final_url[:100]}")
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                return self.process_scraped_job(soup, final_url, url, email_html, sender, method)
            
            else:
                self.outcomes['failed_http'] += 1
                return None
                
        except Exception as e:
            self.outcomes['failed_extraction'] += 1
            logger.info(f"  → EXCEPTION: {str(e)[:100]}")
            return None
    
    def process_email_parsed_job(self, job_data, sender):
        """
        Process job extracted from email content
        """
        
        company = job_data['company']
        title_raw = job_data['title']
        location_raw = job_data.get('location', 'Unknown')
        url = job_data['url']
        
        logger.info(f"  Processing email-parsed data")
        logger.info(f"  Company: {company}")
        logger.info(f"  Title: {title_raw[:80]}")
        logger.info(f"  Location: {location_raw}")
        
        title_no_location = self.remove_location_from_title(title_raw)
        title_final = self.clean_title(title_no_location)
        
        is_valid_title, title_reason = self.is_valid_job_title(title_final)
        if not is_valid_title:
            logger.info(f"  → SKIP: {title_reason}")
            return {
                'decision': 'skip',
                'reason': title_reason,
                'reason_type': 'marketing' if 'Marketing' in title_reason else 'non_job'
            }
        
        if not self.is_cs_engineering_role(title_final):
            logger.info(f"  → DISCARD: Non-CS role")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': 'Unknown',
                'remote': 'Unknown',
                'url': url,
                'job_id': 'N/A',
                'reason': 'Non-CS role',
                'source': sender,
                'sponsorship': 'Unknown (Email)'
            }
        
        is_valid_company, fixed_company, company_reason = self.validate_company_field(company, title_final, url)
        
        if not is_valid_company:
            logger.info(f"  → DISCARD: {company_reason}")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': 'Unknown',
                'remote': 'Unknown',
                'url': url,
                'job_id': 'N/A',
                'reason': company_reason,
                'source': sender,
                'sponsorship': 'Unknown (Email)'
            }
        
        company = fixed_company
        
        normalized_key = self.normalize_for_dedup(f"{company}_{title_final}")
        
        if normalized_key in self.existing_jobs:
            logger.info(f"  Company+Title already exists - checking if different job")
            
            existing_job = self.processed_jobs_cache.get(normalized_key)
            
            new_job_data = {
                'company': company,
                'title': title_final,
                'job_id': 'N/A',
                'url': url
            }
            
            if existing_job and self.should_keep_both_jobs(new_job_data, existing_job):
                self.outcomes['kept_both_variants'] += 1
            else:
                self.outcomes['skipped_duplicate_company_title'] += 1
                return None
        
        location_formatted = self.format_location_for_us(location_raw) if location_raw != 'Unknown' else 'Unknown'
        
        quality_score = self.calculate_quality_score({
            'company': company,
            'title': title_final,
            'location': location_formatted,
            'job_id': 'N/A',
            'sponsorship': 'Unknown (Email)'
        })
        
        logger.info(f"  Quality score: {quality_score}/7")
        
        if quality_score < 3:
            logger.info(f"  → DISCARD: Low quality")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': location_formatted,
                'remote': job_data.get('remote', 'Unknown'),
                'url': url,
                'job_id': 'N/A',
                'reason': f"Low quality: {quality_score}/7",
                'source': sender,
                'sponsorship': 'Unknown (Email)'
            }
        
        logger.info(f"  ✓ VALID (from email)")
        
        self.processed_jobs_cache[normalized_key] = {
            'company': company,
            'title': title_final,
            'job_id': 'N/A',
            'url': url
        }
        
        return {
            'decision': 'valid',
            'company': company,
            'title': title_final,
            'location': location_formatted,
            'remote': job_data.get('remote', 'Unknown'),
            'url': url,
            'job_id': 'N/A',
            'source': sender,
            'sponsorship': 'Unknown (Email)'
        }
    
    def validate_and_decide_on_job(self, company, title_raw, location, remote, sponsorship, url, job_id, sender, soup):
        """Common validation for all jobs"""
        
        # Clean title
        title_no_loc = self.remove_location_from_title(title_raw)
        title = self.clean_title(title_no_loc)
        
        # Validate title
        is_valid, reason = self.is_valid_job_title(title)
        if not is_valid:
            return {'decision': 'skip', 'reason': reason, 'reason_type': 'non_job'}
        
        # Check CS role
        if not self.is_cs_engineering_role(title):
            return {
                'decision': 'discard',
                'company': company, 'title': title, 'location': 'Unknown',
                'remote': 'Unknown', 'url': url, 'job_id': job_id,
                'reason': 'Non-CS', 'source': sender, 'sponsorship': sponsorship
            }
        
        # Validate company
        is_valid_co, fixed_co, co_reason = self.validate_company_field(company, title, url)
        if not is_valid_co:
            return {
                'decision': 'discard',
                'company': company, 'title': title, 'location': 'Unknown',
                'remote': 'Unknown', 'url': url, 'job_id': job_id,
                'reason': co_reason, 'source': sender, 'sponsorship': sponsorship
            }
        
        company = fixed_co
        
        # Check duplicates
        norm_key = self.normalize_for_dedup(f"{company}_{title}")
        if norm_key in self.existing_jobs:
            existing = self.processed_jobs_cache.get(norm_key)
            if existing and self.should_keep_both_jobs(
                {'company': company, 'title': title, 'job_id': job_id, 'url': url},
                existing
            ):
                self.outcomes['kept_both_variants'] += 1
            else:
                self.outcomes['skipped_duplicate_company_title'] += 1
                return None
        
        # Check restrictions
        if soup:
            restriction = self.check_page_for_restrictions(soup)
            if restriction:
                country = self.detect_country_simple(location)
                return {
                    'decision': 'discard',
                    'company': company, 'title': title, 'location': country,
                    'remote': remote, 'url': url, 'job_id': job_id,
                    'reason': restriction, 'source': sender, 'sponsorship': sponsorship
                }
        
        # Check international
        intl_check = self.check_if_international_location(location, soup)
        if intl_check:
            country = self.detect_country_simple(location)
            return {
                'decision': 'discard',
                'company': company, 'title': title, 'location': country,
                'remote': remote, 'url': url, 'job_id': job_id,
                'reason': intl_check, 'source': sender, 'sponsorship': sponsorship
            }
        
        # Format location
        location_fmt = self.format_location_for_us(location)
        
        # Quality check
        quality = self.calculate_quality_score({
            'company': company, 'title': title, 'location': location_fmt,
            'job_id': job_id, 'sponsorship': sponsorship
        })
        
        if quality < 3:
            return {
                'decision': 'discard',
                'company': company, 'title': title, 'location': location_fmt,
                'remote': remote, 'url': url, 'job_id': job_id,
                'reason': f"Low quality: {quality}/7", 'source': sender, 'sponsorship': sponsorship
            }
        
        # VALID!
        self.processed_jobs_cache[norm_key] = {
            'company': company, 'title': title, 'job_id': job_id, 'url': url
        }
        
        return {
            'decision': 'valid',
            'company': company, 'title': title, 'location': location_fmt,
            'remote': remote, 'url': url, 'job_id': job_id,
            'source': sender, 'sponsorship': sponsorship
        }
    
    def process_scraped_job(self, soup, final_url, original_url, email_html, sender, method):
        """
        Process job scraped from actual page
        """
        
        logger.info(f"  Extracting from page (method: {method})")
        # === PLATFORM-SPECIFIC EXTRACTION ===
        
        # JOBRIGHT: Extract from platform page (has all data!)
        if 'jobright.ai/jobs/info/' in final_url.lower():
            logger.info(f"  Platform: JOBRIGHT")
            
            jobright_data = self.extract_from_jobright_page(soup, final_url)
            
            if jobright_data:
                company = jobright_data['company']
                title_raw = jobright_data['title']
                location = jobright_data['location']
                sponsorship = jobright_data['sponsorship']
                remote = jobright_data['remote']
                
                logger.info(f"  Jobright: {company} - {title_raw[:60]}")
                logger.info(f"  Sponsorship: {sponsorship}")
                
                # Continue with validation...
                title_no_loc = self.remove_location_from_title(title_raw)
                title = self.clean_title(title_no_loc)
                
                return self.validate_and_decide_on_job(
                    company, title, location, remote, sponsorship,
                    final_url, 'N/A', sender, soup
                )
            else:
                # Fallback to email
                logger.info(f"  Jobright extraction failed - email")
                email_data = self.extract_job_from_email_content(email_html, original_url, sender)
                if email_data:
                    return self.process_email_parsed_job(email_data, sender)
                return None
        
        # ZIPRECRUITER: Check redirect destination
        elif 'ziprecruiter.com' in original_url.lower():
            logger.info(f"  Platform: ZIPRECRUITER")
            
            if self.is_external_job_board(final_url):
                logger.info(f"  ✓ External board: {final_url[:80]}")
                
                # Extract from external board
                company = self.extract_company_from_page(soup, final_url)
                title_raw = self.extract_title_from_page(soup)
                
                if company and title_raw:
                    location = self.extract_location_comprehensive(soup, final_url)
                    job_id = self.extract_job_id_from_page(soup, final_url)
                    remote_status = self.extract_remote_status(soup, location, final_url)
                    sponsorship = self.check_sponsorship_status(soup)
                    
                    title_no_loc = self.remove_location_from_title(title_raw)
                    title = self.clean_title(title_no_loc)
                    
                    return self.validate_and_decide_on_job(
                        company, title, location, remote_status, sponsorship,
                        final_url, job_id, sender, soup
                    )
                return None
            else:
                # ZipRecruiter page - use email
                logger.info(f"  ✗ ZipRecruiter page - email fallback")
                email_data = self.extract_job_from_email_content(email_html, original_url, sender)
                if email_data:
                    return self.process_email_parsed_job(email_data, sender)
                return None
        
        # === STANDARD EXTRACTION ===
        else:
            company = self.extract_company_from_page(soup, final_url)
            title_raw = self.extract_title_from_page(soup)
            
            if not company or not title_raw:
                self.outcomes['failed_extraction'] += 1
                logger.info(f"  → FAIL: No company or title")
                return None
            
            title_no_location = self.remove_location_from_title(title_raw)
            title_final = self.clean_title(title_no_location)
            
            logger.info(f"  Company: {company}")
            logger.info(f"  Title: {title_final[:80]}")
        
        is_valid_title, title_reason = self.is_valid_job_title(title_final)
        if not is_valid_title:
            logger.info(f"  → SKIP: {title_reason}")
            return {
                'decision': 'skip',
                'reason': title_reason,
                'reason_type': 'marketing' if 'Marketing' in title_reason else 'non_job'
            }
        
        if not self.is_cs_engineering_role(title_final):
            logger.info(f"  → DISCARD: Non-CS role")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': 'Unknown',
                'remote': 'Unknown',
                'url': final_url,
                'job_id': 'N/A',
                'reason': 'Non-CS role',
                'source': sender,
                'sponsorship': 'Unknown'
            }
        
        is_valid_company, fixed_company, company_reason = self.validate_company_field(company, title_final, final_url)
        
        if not is_valid_company:
            logger.info(f"  → DISCARD: {company_reason}")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': 'Unknown',
                'remote': 'Unknown',
                'url': final_url,
                'job_id': 'N/A',
                'reason': company_reason,
                'source': sender,
                'sponsorship': 'Unknown'
            }
        
        company = fixed_company
        
        normalized_key = self.normalize_for_dedup(f"{company}_{title_final}")
        
        if normalized_key in self.existing_jobs:
            logger.info(f"  Company+Title exists - comparing")
            
            existing_job = self.processed_jobs_cache.get(normalized_key)
            
            new_job_data = {
                'company': company,
                'title': title_final,
                'job_id': self.extract_job_id_from_page(soup, final_url),
                'url': final_url
            }
            
            if existing_job and self.should_keep_both_jobs(new_job_data, existing_job):
                self.outcomes['kept_both_variants'] += 1
            else:
                self.outcomes['skipped_duplicate_company_title'] += 1
                return None
        
        logger.info(f"  Checking page restrictions...")
        restriction = self.check_page_for_restrictions(soup)
        
        if restriction:
            logger.info(f"  → DISCARD: {restriction}")
            job_id = self.extract_job_id_from_page(soup, final_url)
            location = self.extract_location_comprehensive(soup, final_url)
            sponsorship = self.check_sponsorship_status(soup)
            
            country_only = self.detect_country_simple(location)
            
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': country_only,
                'remote': 'Unknown',
                'url': final_url,
                'job_id': job_id,
                'reason': restriction,
                'source': sender,
                'sponsorship': sponsorship
            }
        
        logger.info(f"  Extracting location...")
        job_id = self.extract_job_id_from_page(soup, final_url)
        location_extracted = self.extract_location_comprehensive(soup, final_url)
        remote = self.extract_remote_status(soup, location_extracted, final_url)
        sponsorship = self.check_sponsorship_status(soup)
        
        logger.info(f"  Location: {location_extracted}")
        logger.info(f"  Job ID: {job_id}")
        logger.info(f"  Sponsorship: {sponsorship}")
        
        location_intl_check = self.check_if_international_location(location_extracted, soup)
        
        if location_intl_check:
            logger.info(f"  → DISCARD: {location_intl_check}")
            
            country_only = self.detect_country_simple(location_extracted)
            
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': country_only,
                'remote': remote,
                'url': final_url,
                'job_id': job_id,
                'reason': location_intl_check,
                'source': sender,
                'sponsorship': sponsorship
            }
        
        location_formatted = self.format_location_for_us(location_extracted)
        
        logger.info(f"  Formatted location: {location_formatted}")
        
        quality_score = self.calculate_quality_score({
            'company': company,
            'title': title_final,
            'location': location_formatted,
            'job_id': job_id,
            'sponsorship': sponsorship
        })
        
        logger.info(f"  Quality score: {quality_score}/7")
        
        if quality_score < 3:
            logger.info(f"  → DISCARD: Low quality")
            return {
                'decision': 'discard',
                'company': company,
                'title': title_final,
                'location': location_formatted,
                'remote': remote,
                'url': final_url,
                'job_id': job_id,
                'reason': f"Low quality: {quality_score}/7",
                'source': sender,
                'sponsorship': sponsorship
            }
        
        logger.info(f"  ✓ VALID JOB - All checks passed")
        
        self.processed_jobs_cache[normalized_key] = {
            'company': company,
            'title': title_final,
            'job_id': job_id,
            'url': final_url
        }
        
        return {
            'decision': 'valid',
            'company': company,
            'title': title_final,
            'location': location_formatted,
            'remote': remote,
            'url': final_url,
            'job_id': job_id,
            'source': sender,
            'sponsorship': sponsorship
        }
    
    def detect_country_simple(self, location):
        """
        Extract just country name for discarded entries
        """
        if not location or location == 'Unknown':
            return 'Unknown'
        
        location_lower = location.lower()
        
        if 'canada' in location_lower or any(f', {p}' in location for p in CANADA_PROVINCES):
            return 'Canada'
        
        if 'uk' in location_lower or 'united kingdom' in location_lower:
            return 'UK'
        
        if 'india' in location_lower:
            return 'India'
        
        if 'china' in location_lower:
            return 'China'
        
        if 'australia' in location_lower:
            return 'Australia'
        
        if 'singapore' in location_lower:
            return 'Singapore'
        
        return location
    
    def check_if_international_location(self, location, soup):
        """
        Check if location is outside US
        """
        if not location or location == 'Unknown':
            if soup:
                country = self.detect_country_from_page_content(soup)
                if country:
                    return f"Location: {country}"
            return None
        
        location_lower = location.lower()
        
        canadian_cities = [
            'toronto', 'montreal', 'vancouver', 'ottawa', 'calgary',
            'edmonton', 'winnipeg', 'quebec', 'markham', 'mississauga'
        ]
        for city in canadian_cities:
            if city in location_lower:
                return "Location: Canada"
        
        if 'canada' in location_lower:
            return "Location: Canada"
        
        for prov in CANADA_PROVINCES:
            if f', {prov}' in location or f', {prov.lower()}' in location_lower:
                return "Location: Canada"
        
        uk_cities = ['london', 'manchester', 'birmingham', 'edinburgh']
        for city in uk_cities:
            if city in location_lower:
                return "Location: UK"
        
        if 'uk' in location_lower or 'united kingdom' in location_lower:
            return "Location: UK"
        
        countries = {
            'australia': 'Australia', 'india': 'India', 'singapore': 'Singapore',
            'china': 'China', 'japan': 'Japan', 'germany': 'Germany'
        }
        for country_key, country_name in countries.items():
            if country_key in location_lower:
                return f"Location: {country_name}"
        
        return None
    
    def detect_country_from_page_content(self, soup):
        try:
            page_text = soup.get_text()[:4000]
            
            canada_patterns = [
                r'\b(Markham|Toronto|Vancouver),\s*([A-Z]{2}),\s*Canada',
                r'\bCanada\b.{0,50}?\b(office|location|based)',
            ]
            
            for pattern in canada_patterns:
                if re.search(pattern, page_text, re.I):
                    return 'Canada'
            
            if re.search(r'\b(London|Manchester),\s*UK', page_text, re.I):
                return 'UK'
            
            return None
        except:
            return None
    
    def extract_location_comprehensive(self, soup, url):
        """
        Multi-tier location extraction with platform-specific handlers
        """
        
        if 'simplify.jobs' in url.lower():
            simplify_loc = self.extract_location_from_simplify(soup)
            if simplify_loc != 'Unknown':
                logger.info(f"    Location (Simplify): {simplify_loc}")
                return simplify_loc
        
        page_label = self.extract_location_from_page_labels(soup)
        if page_label != 'Unknown':
            logger.info(f"    Location (Label): {page_label}")
            return page_label
        
        json_loc = self.extract_location_from_json_ld(soup)
        if json_loc != 'Unknown':
            logger.info(f"    Location (JSON-LD): {json_loc}")
            return json_loc
        
        if 'workday' in url.lower():
            match = re.search(r'/job/([^/]+)/', url)
            if match:
                location_raw = match.group(1)
                if not location_raw.lower().startswith('remote'):
                    workday_loc = self.extract_city_from_workday_backwards(location_raw)
                    if workday_loc != 'Unknown':
                        logger.info(f"    Location (Workday): {workday_loc}")
                        return workday_loc
        
        enhanced_scan = self.scan_page_for_location_enhanced(soup)
        if enhanced_scan != 'Unknown':
            logger.info(f"    Location (Scan): {enhanced_scan}")
            return enhanced_scan
        
        country = self.detect_country_from_page_content(soup)
        if country:
            logger.info(f"    Location (Country fallback): {country}")
            return country
        
        logger.info(f"    Location: Unknown")
        return 'Unknown'
    
    def extract_location_from_simplify(self, soup):
        try:
            page_text = soup.get_text()
            
            patterns = [
                r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2}),\s*(USA|Canada|UK)',
                r'\b([A-Z][a-z]+),\s*(Canada|India|UK)',
                r'(?:Location|Office|Based):\s*([A-Za-z\s,]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text[:2500], re.I)
                if match:
                    groups = match.groups()
                    
                    if len(groups) == 3:
                        return f"{groups[0]}, {groups[1]}, {groups[2]}"
                    elif len(groups) == 2:
                        return f"{groups[0]}, {groups[1]}"
                    else:
                        extracted = groups[0].strip()
                        if 10 < len(extracted) < 100:
                            return extracted
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def scan_page_for_location_enhanced(self, soup):
        try:
            page_text = soup.get_text()[:5000]
            
            pattern1 = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2}),\s*(USA|Canada)'
            matches = re.findall(pattern1, page_text)
            if matches:
                city, state, country = matches[0]
                return f"{city}, {state}, {country}"
            
            pattern2 = r'\b([A-Z][a-z]+),\s*(Canada|UK|India)'
            matches = re.findall(pattern2, page_text)
            if matches:
                return f"{matches[0][0]}, {matches[0][1]}"
            
            pattern3 = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b'
            matches = re.findall(pattern3, page_text)
            if matches:
                for city, state in matches:
                    if state.upper() in US_STATES.values():
                        return f"{city}, {state.upper()}"
                    if state.upper() in CANADA_PROVINCES:
                        return f"{city}, {state.upper()}, Canada"
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def validate_company_field(self, company, title, url):
        if not company or company == 'Unknown':
            fixed = self.extract_company_from_domain(url)
            if fixed != 'Unknown':
                return True, fixed, None
            return True, company, None
        
        company_lower = company.lower().strip()
        title_lower = title.lower().strip()
        
        if company_lower == title_lower:
            fixed = self.extract_company_from_domain(url)
            if fixed != 'Unknown' and fixed.lower() != title_lower:
                logger.info(f"    Fixed Company=Title: '{company}' → '{fixed}'")
                return True, fixed, None
            return False, company, "Bad data: Company=Title"
        
        if re.search(r'20\d{2}', company):
            return False, company, "Bad data: Year in company"
        
        if re.search(r'\bintern(ship)?\b', company, re.I):
            return False, company, "Bad data: Intern in company"
        
        return True, company, None
    
    def remove_location_from_title(self, title):
        if not title:
            return title
        
        title = re.sub(r'\s*[-,]\s*(Canada|UK|India|Remote|Hybrid).*$', '', title, flags=re.I)
        title = re.sub(r'\s*[-,]\s*[A-Z][a-z]+,\s*[A-Z]{2}.*$', '', title)
        title = re.sub(r',?\s*Or\s+\d+\s+months.*$', '', title, flags=re.I)
        
        return title.strip()
    
    def clean_title(self, title):
        if not title or len(title) < 5:
            return title
        
        original = title
        
        title = re.sub(r'\s*[-–]\s*(Summer|Fall|Winter|Spring)\s+20\d{2}\s*', '', title, flags=re.I)
        title = re.sub(r'\s*\((Graduate|Undergraduate|PhD)\s*\)\s*', '', title, flags=re.I)
        title = re.sub(r'\s*\((Remote|Hybrid|On-?site)\s*\)\s*', '', title, flags=re.I)
        title = re.sub(r'\bInternships\b', 'Intern', title)
        title = re.sub(r'\s+', ' ', title).strip()
        
        if len(title) < 8 and len(original) > 10:
            return original
        
        return title
    
    def extract_company_from_domain(self, url):
        try:
            if 'workday' in url.lower():
                match = re.search(r'https?://([^.]+)\.(?:wd\d+\.)?myworkdayjobs', url)
                if match:
                    return self.format_company_name(match.group(1))
            
            match = re.search(r'https?://(?:www\.)?([^./]+)', url)
            if match:
                return self.format_company_name(match.group(1))
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def format_company_name(self, slug):
        slug = slug.replace('-', ' ').replace('_', ' ')
        
        special = {
            'stanfordhealthcare': 'Stanford Health Care',
            'bmo': 'BMO', 'jpmorgan': 'JPMorgan',
            'figma': 'Figma', 'ibm': 'IBM',
            'simplify': 'Simplify Jobs'
        }
        
        slug_clean = slug.lower().replace(' ', '')
        if slug_clean in special:
            return special[slug_clean]
        
        return slug.title()
    
    def extract_city_from_workday_backwards(self, location_str):
        try:
            location_str = re.sub(r'^[0-9]+[A-Z]+\s*[-–]?\s*', '', location_str)
            location_str = location_str.replace('-', ' ').replace('_', ' ')
            parts = [p.strip() for p in location_str.split() if p.strip()]
            
            if not parts:
                return 'Unknown'
            
            state = None
            city_words = []
            
            for i in range(len(parts) - 1, -1, -1):
                word_upper = parts[i].upper()
                
                if not state and word_upper in US_STATES.values():
                    state = word_upper
                    continue
                
                if state:
                    city_words.insert(0, parts[i])
                    potential_city = ' '.join(city_words).lower()
                    
                    if potential_city in CITY_TO_STATE:
                        if CITY_TO_STATE[potential_city] == state:
                            return f"{' '.join(city_words).title()} - {state}"
            
            if state and city_words:
                facility = ['hospital', 'building', 'pkwy', 'patient', 'meadows']
                cleaned = [w for w in city_words if w.lower() not in facility and not w.isdigit()]
                
                if cleaned:
                    return f"{' '.join(cleaned).title()} - {state}"
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def extract_location_from_page_labels(self, soup):
        try:
            page_text = soup.get_text()
            
            patterns = [
                r'Location:\s*([^\n|]+)',
                r'Office Location:\s*([^\n|]+)',
                r'Work Location:\s*([^\n|]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    location = match.group(1).strip()
                    location = re.sub(r'\s*\|.*$', '', location)
                    if location and len(location) < 100:
                        return location
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def extract_location_from_json_ld(self, soup):
        try:
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                data = json.loads(json_ld.string)
                if isinstance(data, dict):
                    job_loc = data.get('jobLocation', {})
                    if isinstance(job_loc, dict):
                        addr = job_loc.get('address', {})
                        if isinstance(addr, dict):
                            city = addr.get('addressLocality', '')
                            state = addr.get('addressRegion', '')
                            if city and state:
                                return f"{city}, {state}"
            return 'Unknown'
        except:
            return 'Unknown'
    
    def format_location_for_us(self, location):
        """
        Format location for US entries only
        International format checked first, then returns City - STATE
        """
        if not location or location == 'Unknown':
            return 'Unknown'
        
        location = location.strip()
        
        # Check international FIRST
        intl_pattern = r'^([A-Za-z\s]+),\s*([A-Z]{2}),\s*(Canada|USA|UK)$'
        intl_match = re.match(intl_pattern, location, re.I)
        
        if intl_match:
            city, state_prov, country = intl_match.groups()
            
            if country.lower() == 'usa':
                if state_prov in US_STATES.values():
                    return f"{city.title()} - {state_prov}"
            else:
                # International: Return City, PROV (no country)
                return f"{city.title()}, {state_prov.upper()}"
        
        # Remove USA suffix
        location = re.sub(r',\s*USA?$', '', location, flags=re.I)
        
        # Check multiple locations (after international handled)
        if '|' in location or (', ' in location and location.count(',') > 1):
            # Make sure it's not international
            if not any(prov in location for prov in CANADA_PROVINCES):
                if not re.search(r'(Canada|UK|India)', location, re.I):
                    return "Many US locations"
        
        # Standard US format
        match = re.search(r'([^,]+),\s*([A-Z]{2})\b', location)
        if match:
            city = match.group(1).strip()
            state = match.group(2).upper()
            if state in US_STATES.values():
                return f"{city} - {state}"
            elif state in CANADA_PROVINCES:
                return f"{city}, {state}"
        
        return location
    
    def clean_url(self, url):
        """Clean URL - PRESERVE Jobright job IDs"""
        if not url:
            return ''
        
        # Jobright: Keep job ID (each is unique page)
        if 'jobright.ai/jobs/info/' in url.lower():
            match = re.search(r'(jobright\.ai/jobs/info/[a-f0-9]+)', url, re.I)
            if match:
                return match.group(1).lower()
        
        # Standard cleaning
        url = re.sub(r'\?.*$', '', url)
        url = re.sub(r'#.*$', '', url)
        return url.lower().rstrip('/')
    
    def remove_emojis(self, text):
        if not text:
            return text
        
        emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"
            u"\U0001F300-\U0001F5FF"
            u"\U0001F680-\U0001F6FF"
            u"\U0001F1E0-\U0001F1FF"
            u"\U00002500-\U00002BEF"
            "]+", flags=re.UNICODE)
        
        text = emoji_pattern.sub(r'', text)
        text = re.sub(r'[↳🇺🇸🛂\*🔒❌✅]+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def safe_get_cell(self, row, index, default=''):
        try:
            if len(row) > index:
                return row[index].strip() if row[index] else default
            return default
        except:
            return default
    
    def format_sheet_headers(self, sheet, num_cols=13):
        try:
            col_letter = chr(ord('A') + num_cols - 1)
            sheet.format(f'A1:{col_letter}1', {
                'horizontalAlignment': 'CENTER',
                'textFormat': {
                    'fontFamily': 'Times New Roman',
                    'fontSize': 14,
                    'bold': True
                },
                'backgroundColor': {'red': 0.7, 'green': 0.9, 'blue': 0.7}
            })
        except:
            pass
    
    def auto_resize_all_columns_except_url(self, sheet, url_column_index, total_columns):
        try:
            self.spreadsheet.batch_update({"requests": [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": total_columns
                    }
                }
            }]})
            time.sleep(1)
            
            is_discarded = (total_columns == 13 and url_column_index == 5)
            
            fixed_widths = []
            
            # Status or Discard Reason
            fixed_widths.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 350 if is_discarded else 65},
                    "fields": "pixelSize"
                }
            })
            
            # Date Applied or Moved Date
            fixed_widths.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5},
                    "properties": {"pixelSize": 150},
                    "fields": "pixelSize"
                }
            })
            
            # URL
            fixed_widths.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": url_column_index, "endIndex": url_column_index + 1},
                    "properties": {"pixelSize": 100},
                    "fields": "pixelSize"
                }
            })
            
            # Sponsorship
            fixed_widths.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": 12, "endIndex": 13},
                    "properties": {"pixelSize": 110},
                    "fields": "pixelSize"
                }
            })
            
            self.spreadsheet.batch_update({"requests": fixed_widths})
        except Exception as e:
            logger.warning(f"Column resize error: {e}")
    
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
                if row_idx < 1:
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
                                    "horizontalAlignment": "CENTER"
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
    
    def check_sponsorship_status(self, soup):
        try:
            page_text = soup.get_text().lower()
            
            positive = ['visa sponsorship available', 'h1b sponsorship', 'will sponsor', 'opt eligible']
            for indicator in positive:
                if indicator in page_text:
                    return "Yes"
            
            negative = ['no visa sponsorship', 'does not sponsor', 'cannot sponsor']
            for indicator in negative:
                if indicator in page_text:
                    return "No"
            
            return "Unknown"
        except:
            return "Unknown"
    
    def get_detailed_discard_reason(self, title):
        title_lower = title.lower()
        
        if 'phd' in title_lower and 'or' not in title_lower:
            return "PhD required"
        
        excluded = ['product management', 'marketing', 'sales', 'hr', 'finance']
        for kw in excluded:
            if kw in title_lower:
                return f"Non-CS: {kw.title()}"
        
        if '🔒' in title:
            return "Position closed"
        
        return "Filtered"
    
    def is_cs_engineering_role(self, title):
        title_lower = title.lower()
        
        excluded = ['product management', 'marketing', 'sales', 'hr', 'finance']
        for kw in excluded:
            if kw in title_lower:
                return False
        
        required = ['software', 'swe', 'engineer', 'developer', 'data', 'tech', 'algorithm', 'ml', 'ai']
        return any(kw in title_lower for kw in required)
    
    def check_page_for_restrictions(self, soup):
        try:
            page_text = soup.get_text().lower()
            
            if 'security clearance' in page_text:
                return "Security clearance required"
            
            if 'us citizen only' in page_text or 'must be a us citizen' in page_text:
                return "US citizenship required"
            
            if 'undergraduate students only' in page_text or 'bachelor\'s degree in progress' in page_text:
                return "Bachelor's requirement only"
            
            return None
        except:
            return None
    
    def extract_job_id_from_page(self, soup, url):
        try:
            page_text = soup.get_text()
            
            patterns = [
                (r'Req ID:\s*([A-Z0-9\-]+)', 1),
                (r'Job ID:\s*([A-Z0-9\-]+)', 1),
            ]
            
            for pattern, group in patterns:
                match = re.search(pattern, page_text, re.I)
                if match:
                    return match.group(group).strip()
            
            if 'workday' in url.lower():
                match = re.search(r'_([A-Z]*\d+)(?:\?|$)', url)
                if match:
                    return match.group(1)
            
            return 'N/A'
        except:
            return 'N/A'
    
    def extract_remote_status(self, soup, location, url):
        try:
            if 'remote' in url.lower():
                return "Remote"
            
            if location and 'remote' in location.lower():
                return "Remote"
            
            page_text = soup.get_text().lower()
            if 'hybrid' in page_text:
                return "Hybrid"
            
            return "On Site" if location != 'Unknown' else "Unknown"
        except:
            return "Unknown"
    
    def determine_job_type(self, title):
        if 'co-op' in title.lower():
            return "Co-op"
        return "Internship"
    
    def format_date(self):
        return datetime.datetime.now().strftime("%d %B, %I:%M %p")
    
    def parse_age(self, age_str):
        match = re.search(r'(\d+)d', age_str.lower()) if age_str else None
        return int(match.group(1)) if match else 999
    
    def is_duplicate(self, company, title, url, job_id='N/A'):
        normalized_key = self.normalize_for_dedup(f"{company}_{title}")
        if normalized_key in self.existing_jobs:
            return True
        
        if self.clean_url(url) in self.existing_urls:
            return True
        
        if job_id != 'N/A' and job_id.lower() in self.existing_job_ids:
            return True
        
        return False
    
    def extract_company_from_page(self, soup, url):
        try:
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, dict):
                        org = data.get('hiringOrganization', {})
                        if isinstance(org, dict):
                            name = org.get('name', '')
                            if name and len(name) < 100:
                                return name
                except:
                    pass
            
            meta = soup.find('meta', {'property': 'og:site_name'})
            if meta and meta.get('content'):
                company = meta.get('content').strip()
                company = re.sub(r'\s*[-|]\s*(careers|jobs).*$', '', company, flags=re.I)
                if company and len(company) < 50:
                    return company
            
            return self.extract_company_from_domain(url)
        except:
            return 'Unknown'
    
    def extract_title_from_page(self, soup):
        try:
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                try:
                    data = json.loads(json_ld.string)
                    if isinstance(data, dict):
                        title = data.get('title', '')
                        if title and 5 < len(title) < 200:
                            return title
                except:
                    pass
            
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text().strip()
                if title and 5 < len(title) < 200:
                    return title
            
            title_tag = soup.find('title')
            if title_tag:
                full_title = title_tag.get_text().strip()
                parts = full_title.split('-')
                if parts:
                    title = parts[0].strip()
                    if title and 5 < len(title) < 200:
                        return title
            
            return 'Unknown'
        except:
            return 'Unknown'
    
    def process_job_url(self, url, company, title):
        """
        For GitHub jobs - doesn't have email HTML
        """
        try:
            fetch_result = self.fetch_page_comprehensive(url, email_html=None, sender='GitHub')
            
            if len(fetch_result) != 3:
                self.outcomes['failed_http'] += 1
                return {'status': 'rejected', 'reason': 'Failed', 'url': url}
            
            response, final_url, method = fetch_result
            
            if not response:
                return {'status': 'rejected', 'reason': 'HTTP failed', 'url': url}
            
            clean_final = self.clean_url(final_url)
            
            if clean_final in self.processing_lock or clean_final in self.existing_urls:
                self.outcomes['skipped_duplicate_url'] += 1
                self.existing_urls.add(self.clean_url(url))
                self.existing_urls.add(clean_final)
                return None
            
            self.processing_lock.add(clean_final)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            restriction = self.check_page_for_restrictions(soup)
            
            if restriction:
                job_id = self.extract_job_id_from_page(soup, final_url)
                location = self.extract_location_comprehensive(soup, final_url)
                sponsorship = self.check_sponsorship_status(soup)
                
                country_only = self.detect_country_simple(location)
                
                self.existing_urls.add(clean_final)
                
                return {
                    'status': 'rejected',
                    'reason': restriction,
                    'url': final_url,
                    'job_id': job_id,
                    'location': country_only,
                    'sponsorship': sponsorship
                }
            
            job_id = self.extract_job_id_from_page(soup, final_url)
            location = self.extract_location_comprehensive(soup, final_url)
            remote = self.extract_remote_status(soup, location, final_url)
            sponsorship = self.check_sponsorship_status(soup)
            
            self.existing_urls.add(clean_final)
            
            return {
                'status': 'accepted',
                'final_url': final_url,
                'job_id': job_id,
                'location': location,
                'remote': remote,
                'sponsorship': sponsorship
            }
            
        except Exception as e:
            self.outcomes['failed_extraction'] += 1
            return {'status': 'rejected', 'reason': 'Error', 'url': url}
    
    def scrape_simplify_github(self):
        logger.info("Scraping SimplifyJobs GitHub")
        
        try:
            response = requests.get(SIMPLIFY_URL, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) < 5:
                        continue
                    
                    company_link = cells[0].find('a')
                    if not company_link:
                        continue
                    
                    company = self.remove_emojis(company_link.get_text(strip=True))
                    title_raw = self.remove_emojis(cells[1].get_text(strip=True))
                    location = self.remove_emojis(cells[2].get_text(strip=True))
                    age = cells[4].get_text(strip=True)
                    
                    apply_link = cells[3].find('a', href=True)
                    if not apply_link:
                        continue
                    apply_url = apply_link.get('href', '')
                    
                    if not company or not title_raw or not apply_url:
                        continue
                    
                    if self.parse_age(age) > 1:
                        continue
                    
                    title_no_loc = self.remove_location_from_title(title_raw)
                    title = self.clean_title(title_no_loc)
                    
                    if self.is_duplicate(company, title, apply_url, self.extract_job_id_from_url(apply_url)):
                        self.outcomes['skipped_duplicate_url'] += 1
                        continue
                    
                    is_valid, reason = self.is_valid_job_title(title)
                    if not is_valid:
                        self.outcomes['skipped_non_job'] += 1
                        continue
                    
                    discard_reason = self.get_detailed_discard_reason(title)
                    
                    if '🔒' in str(cells[3]):
                        discard_reason = "Position closed"
                    
                    if discard_reason != "Filtered":
                        country = self.detect_country_simple(location) if discard_reason.startswith("Location") else self.format_location_for_us(location)
                        
                        self.discarded_jobs.append({
                            'company': company, 'title': title, 'location': country,
                            'job_type': self.determine_job_type(title),
                            'remote': "Remote" if "remote" in location.lower() else "On Site",
                            'url': apply_url, 'job_id': self.extract_job_id_from_url(apply_url),
                            'reason': discard_reason, 'source': 'GitHub',
                            'sponsorship': 'Unknown'
                        })
                        
                        self.existing_jobs.add(self.normalize_for_dedup(f"{company}_{title}"))
                        self.existing_urls.add(self.clean_url(apply_url))
                        self.outcomes['discarded'] += 1
                        continue
                    
                    result = self.process_job_url(apply_url, company, title)
                    
                    if not result:
                        continue
                    
                    if result['status'] == 'rejected':
                        self.discarded_jobs.append({
                            'company': company, 'title': title,
                            'location': result.get('location', location),
                            'job_type': self.determine_job_type(title),
                            'remote': result.get('remote', 'On Site'),
                            'url': result.get('url', apply_url),
                            'job_id': result.get('job_id', 'N/A'),
                            'reason': result.get('reason', 'Unknown'),
                            'source': 'GitHub',
                            'sponsorship': result.get('sponsorship', 'Unknown')
                        })
                        
                        self.existing_jobs.add(self.normalize_for_dedup(f"{company}_{title}"))
                        self.existing_urls.add(self.clean_url(result.get('url', apply_url)))
                        self.outcomes['discarded'] += 1
                    else:
                        quality = self.calculate_quality_score({
                            'company': company, 'title': title,
                            'location': result.get('location'),
                            'job_id': result.get('job_id'),
                            'sponsorship': result.get('sponsorship')
                        })
                        
                        if quality < 3:
                            self.outcomes['low_quality'] += 1
                            continue
                        
                        location_check = self.check_if_international_location(result.get('location'), None)
                        
                        if location_check:
                            country = self.detect_country_simple(result.get('location'))
                            
                            self.discarded_jobs.append({
                                'company': company, 'title': title,
                                'location': country,
                                'job_type': self.determine_job_type(title),
                                'remote': result.get('remote'),
                                'url': result['final_url'],
                                'job_id': result.get('job_id'),
                                'reason': location_check,
                                'source': 'GitHub',
                                'sponsorship': result.get('sponsorship')
                            })
                            self.outcomes['discarded'] += 1
                        else:
                            self.valid_jobs.append({
                                'company': company, 'job_id': result['job_id'], 'title': title,
                                'job_type': self.determine_job_type(title),
                                'location': self.format_location_for_us(result.get('location')),
                                'remote': result.get('remote'),
                                'entry_date': self.format_date(),
                                'url': result['final_url'],
                                'source': 'GitHub',
                                'sponsorship': result.get('sponsorship')
                            })
                            
                            norm_key = self.normalize_for_dedup(f"{company}_{title}")
                            self.existing_jobs.add(norm_key)
                            self.processed_jobs_cache[norm_key] = {
                                'company': company, 'title': title,
                                'job_id': result['job_id'], 'url': result['final_url']
                            }
                            self.existing_urls.add(self.clean_url(result['final_url']))
                            self.outcomes['valid'] += 1
            
            logger.info(f"GitHub: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} valid")
                
        except Exception as e:
            logger.error(f"GitHub error: {e}")
    
    def extract_job_id_from_url(self, url):
        try:
            if 'workday' in url.lower():
                match = re.search(r'_([A-Z]*\d+)(?:\?|$)', url)
                if match:
                    return match.group(1)
            return 'N/A'
        except:
            return 'N/A'
    
    def ensure_mutual_exclusion(self):
        if not self.valid_jobs or not self.discarded_jobs:
            logger.info("Mutual exclusion: No overlap possible")
            return
        
        valid_keys = {(self.normalize_for_dedup(j['company']), 
                      self.normalize_for_dedup(j['title']), 
                      self.clean_url(j['url'])) for j in self.valid_jobs}
        
        discarded_keys = {(self.normalize_for_dedup(j['company']), 
                          self.normalize_for_dedup(j['title']), 
                          self.clean_url(j['url'])) for j in self.discarded_jobs}
        
        overlap = valid_keys & discarded_keys
        
        if overlap:
            logger.warning(f"⚠ MUTUAL EXCLUSION: {len(overlap)} jobs in BOTH lists")
            
            overlap_simple = {(c, t) for c, t, u in overlap}
            
            removed = [j for j in self.valid_jobs 
                      if (self.normalize_for_dedup(j['company']), 
                         self.normalize_for_dedup(j['title'])) in overlap_simple]
            
            for job in removed:
                logger.warning(f"  Removed: {job['company']} - {job['title'][:60]}")
            
            self.valid_jobs = [j for j in self.valid_jobs 
                              if (self.normalize_for_dedup(j['company']), 
                                 self.normalize_for_dedup(j['title'])) not in overlap_simple]
            
            self.outcomes['valid'] = len(self.valid_jobs)
            logger.info(f"  After exclusion: {len(self.valid_jobs)} valid remain")
        else:
            logger.info("Mutual exclusion: No overlap - clean!")
    
    def batch_update_with_links_and_dropdowns(self, sheet, start_row, rows_data, is_valid_sheet=True):
        try:
            if not rows_data:
                return
            
            range_name = f'A{start_row}:M{start_row + len(rows_data) - 1}'
            sheet.update(values=rows_data, range_name=range_name, value_input_option='RAW')
            time.sleep(2)
            
            sheet.format(range_name, {
                'horizontalAlignment': 'CENTER',
                'textFormat': {'fontFamily': 'Times New Roman', 'fontSize': 13}
            })
            time.sleep(2)
            
            url_requests = []
            for idx, row_data in enumerate(rows_data):
                url = row_data[5]
                if url and url.startswith('http'):
                    url_requests.append({
                        "updateCells": {
                            "range": {
                                "sheetId": sheet.id,
                                "startRowIndex": start_row + idx - 1,
                                "endRowIndex": start_row + idx,
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
                    dropdown_requests.append({
                        "setDataValidation": {
                            "range": {
                                "sheetId": sheet.id,
                                "startRowIndex": start_row + idx - 1,
                                "endRowIndex": start_row + idx,
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
            logger.error(f"Batch update error: {e}")
    
    def add_to_sheet(self):
        if not self.valid_jobs:
            logger.info("No new valid jobs")
            return
        
        logger.info(f"Adding {len(self.valid_jobs)} valid jobs")
        
        for i in range(0, len(self.valid_jobs), 10):
            batch = self.valid_jobs[i:i+10]
            
            rows = []
            for idx, job in enumerate(batch):
                sr_no = self.next_sr_no + i + idx
                rows.append([
                    sr_no, 'Not Applied', job['company'], job['title'], 'N/A',
                    job['url'], job['job_id'], job['job_type'],
                    job['location'], job['remote'], job['entry_date'], job['source'],
                    job.get('sponsorship', 'Unknown')
                ])
            
            self.batch_update_with_links_and_dropdowns(self.sheet, self.next_row + i, rows, True)
            self.added += len(rows)
            time.sleep(3)
        
        self.auto_resize_all_columns_except_url(self.sheet, 5, 13)
        logger.info(f"Added {self.added} valid jobs")
    
    def add_to_discarded(self):
        if not self.discarded_jobs:
            logger.info("No new discarded jobs")
            return
        
        logger.info(f"Adding {len(self.discarded_jobs)} discarded")
        
        for i in range(0, len(self.discarded_jobs), 10):
            batch = self.discarded_jobs[i:i+10]
            
            rows = []
            for idx, job in enumerate(batch):
                sr_no = self.next_discarded_sr_no + i + idx
                rows.append([
                    sr_no, job.get('reason', 'Filtered'), job['company'], job['title'], 'N/A',
                    job['url'], job.get('job_id', 'N/A'), job['job_type'],
                    job['location'], job['remote'], self.format_date(), job['source'],
                    job.get('sponsorship', 'Unknown')
                ])
            
            self.batch_update_with_links_and_dropdowns(self.discarded_sheet, self.next_discarded_row + i, rows, False)
            self.discarded += len(rows)
            time.sleep(3)
        
        self.auto_resize_all_columns_except_url(self.discarded_sheet, 5, 13)
        logger.info(f"Added {self.discarded} discarded")
    
    def print_processing_summary(self):
        logger.info("")
        logger.info("=" * 80)
        logger.info("PROCESSING SUMMARY:")
        logger.info("=" * 80)
        logger.info(f"  ✓ Valid jobs: {self.outcomes['valid']}")
        logger.info(f"  ✗ Discarded: {self.outcomes['discarded']}")
        logger.info(f"  ⊘ Skipped (duplicate URL): {self.outcomes['skipped_duplicate_url']}")
        logger.info(f"  ⊘ Skipped (duplicate company+title): {self.outcomes['skipped_duplicate_company_title']}")
        logger.info(f"  ⊘ Skipped (non-job): {self.outcomes['skipped_non_job']}")
        logger.info(f"  ⚠ Failed (HTTP): {self.outcomes['failed_http']}")
        logger.info(f"  ⚠ Failed (extraction): {self.outcomes['failed_extraction']}")
        logger.info(f"  ⚠ Low quality: {self.outcomes['low_quality']}")
        logger.info(f"  ✓ Kept both variants: {self.outcomes['kept_both_variants']}")
        logger.info("")
        logger.info("EXTRACTION METHODS USED:")
        logger.info(f"  Standard requests: {self.outcomes['method_standard']}")
        logger.info(f"  Rotating UA: {self.outcomes['method_rotating_agent']}")
        logger.info(f"  Selenium: {self.outcomes['method_selenium']}")
        logger.info(f"  Email parsing: {self.outcomes['method_email_parsed']}")
        logger.info("=" * 80)
    
    def run(self):
        start_time = time.time()
        logger.info("Starting job aggregation\n")
        
        self.scrape_simplify_github()
        
        try:
            email_data = self.fetch_swelist_emails()
            if email_data:
                self.process_email_jobs(email_data)
        except Exception as e:
            logger.error(f"Email error: {e}")
        
        logger.info("\nRunning mutual exclusion check")
        self.ensure_mutual_exclusion()
        
        logger.info(f"\nFinal: {len([j for j in self.valid_jobs if j['source'] == 'GitHub'])} GitHub, "
                   f"{len([j for j in self.valid_jobs if j['source'] != 'GitHub'])} Email\n")
        
        self.add_to_sheet()
        self.add_to_discarded()
        
        self.print_processing_summary()
        
        elapsed = time.time() - start_time
        logger.info(f"Execution time: {elapsed/60:.1f} minutes\n")
        logger.info(f"DONE: {self.added} valid, {self.discarded} discarded")
        logger.info("=" * 80)

if __name__ == "__main__":
    UnifiedJobAggregator().run()