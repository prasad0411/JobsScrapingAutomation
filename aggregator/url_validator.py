"""
URL-Company Validator — Self-healing job data correction.

Extracts the real company and title from the job URL and corrects
mismatches caused by SWE List row shifts or Simplify redirect bugs.
Never rejects jobs — only corrects metadata.

Usage:
    from aggregator.url_validator import validate_job
    job = validate_job(job_dict)  # corrects company/title in-place
"""
import re
import logging
import json
import os
from urllib.parse import urlparse, unquote

log = logging.getLogger(__name__)

_BRAIN_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local", "brain.json")

# ATS platforms where company is in the subdomain
_WORKDAY_PATTERN = re.compile(r"^([a-z0-9-]+)\.wd\d+\.myworkday", re.I)
_WORKDAY_SITE_PATTERN = re.compile(r"myworkdaysite\.com/recruiting/([a-z0-9-]+)/", re.I)

# ATS platforms where company is in the URL path
_PATH_ATS = {
    "greenhouse.io": r"/(?:job-boards\.(?:eu\.)?greenhouse\.io|boards\.greenhouse\.io)/([^/]+)/",
    "lever.co": r"jobs\.lever\.co/([^/]+)/",
    "ashbyhq.com": r"jobs\.ashbyhq\.com/([^/]+)/",
    "workable.com": r"apply\.workable\.com/([^/]+)/",
    "icims.com": r"careers-([^.]+)\.icims\.com/",
    "rippling.com": r"ats\.rippling\.com/([^/]+)/",
    "smartrecruiters.com": r"jobs\.smartrecruiters\.com/([^/]+)/",
}

# Known ATS names that are NOT companies
_ATS_NAMES = {
    "greenhouse", "lever", "ashbyhq", "workable", "icims", "rippling",
    "smartrecruiters", "ultipro", "jobvite", "myworkdayjobs", "oraclecloud",
    "successfactors", "bamboohr", "applytojob", "taleo", "recruiting",
}

# Slug cleanup patterns
_SLUG_NOISE = re.compile(
    r"(utm_source|ref|gh_src|gh_jid|mobile|needsRedirect|apply|application|job|jobs|careers|external|en|sites|CX_\d+|hcmUI|CandidateExperience)$",
    re.I,
)


def _load_url_cache():
    """Load URL-company cache from brain.json."""
    try:
        if os.path.exists(_BRAIN_PATH):
            brain = json.load(open(_BRAIN_PATH))
            return brain.get("url_company_cache", {})
    except Exception:
        pass
    return {}


def _save_url_cache(cache):
    """Save URL-company cache to brain.json."""
    try:
        brain = {}
        if os.path.exists(_BRAIN_PATH):
            brain = json.load(open(_BRAIN_PATH))
        brain["url_company_cache"] = cache
        with open(_BRAIN_PATH, "w") as f:
            json.dump(brain, f, indent=2)
    except Exception:
        pass


def extract_company_from_url(url):
    """Extract company name from job URL domain/path."""
    if not url:
        return None

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        path = unquote(parsed.path)
    except Exception:
        return None

    # Check URL-company cache first
    cache = _load_url_cache()
    domain_key = domain.split(".")[0] if "myworkdayjobs" in domain else domain
    if domain_key in cache:
        return cache[domain_key]

    # Workday: company is the subdomain
    m = _WORKDAY_PATTERN.match(domain)
    if m:
        slug = m.group(1).lower()
        if slug not in _ATS_NAMES and len(slug) > 2:
            company = slug.replace("-", " ").replace("_", " ").strip().title()
            return company

    # Workday site variant
    m = _WORKDAY_SITE_PATTERN.search(url)
    if m:
        slug = m.group(1).lower()
        if slug not in _ATS_NAMES:
            return slug.replace("-", " ").title()

    # Path-based ATS
    for ats, pattern in _PATH_ATS.items():
        if ats.split(".")[0] in domain:
            m = re.search(pattern, url, re.I)
            if m:
                slug = m.group(1).lower()
                if slug not in _ATS_NAMES and len(slug) > 2:
                    return slug.replace("-", " ").replace("_", " ").strip().title()

    # Oracle Cloud
    if "oraclecloud.com" in domain:
        m = re.match(r"([a-z0-9-]+)\.fa\.", domain)
        if m and m.group(1) not in _ATS_NAMES:
            return m.group(1).replace("-", " ").title()

    # Amazon
    if "amazon.jobs" in domain:
        return "Amazon"

    # Tesla
    if "tesla.com" in domain:
        return "Tesla"

    # NVIDIA
    if "nvidia" in domain:
        return "Nvidia"

    return None


def extract_title_from_url(url):
    """Extract job title from URL path slug."""
    if not url:
        return None

    try:
        path = unquote(urlparse(url).path)
        segments = [s for s in path.split("/") if s and len(s) > 5]
        if not segments:
            return None

        # Take last meaningful segment (usually the job title slug)
        slug = segments[-1]

        # Remove job IDs (hex, numeric, mixed)
        slug = re.sub(r"^[a-f0-9-]{8,}[-_]?", "", slug)
        slug = re.sub(r"[-_][A-Z0-9]{5,}$", "", slug)  # trailing IDs like _REQ-4462
        slug = re.sub(r"[-_]R\d{5,}$", "", slug)
        slug = re.sub(r"[-_]JR\d{4,}$", "", slug)
        slug = re.sub(r"[-_]\d{6,}$", "", slug)

        # Convert slug to title
        title = slug.replace("-", " ").replace("_", " ").strip()

        # Remove noise words
        title = re.sub(r"(utm source|ref|simplify|apply|application)", "", title, flags=re.I).strip()

        if len(title) >= 8:
            return title.title()
    except Exception:
        pass

    return None


def _normalize(s):
    """Normalize string for comparison."""
    return re.sub(r"[^a-z0-9]", "", s.lower()) if s else ""


def _fuzzy_match(a, b):
    """Simple fuzzy match: check if normalized strings share significant overlap."""
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.8
    # Character overlap
    common = sum(1 for c in set(na) if c in nb)
    return common / max(len(set(na)), len(set(nb)))


def validate_job(job):
    """
    Validate and fix a job dict. Corrects company/title from URL if mismatched.
    Never rejects — only corrects. Returns the (possibly modified) job dict.
    
    Expected keys: company, title, url
    """
    url = job.get("url", "")
    hint_company = job.get("company", "Unknown")

    if not url or not url.startswith("http"):
        return job  # Can't validate without URL

    # Extract real company from URL
    url_company = extract_company_from_url(url)

    if url_company:
        hint_norm = _normalize(hint_company)
        url_norm = _normalize(url_company)

        # Check if they match
        similarity = _fuzzy_match(hint_company, url_company)

        if similarity < 0.4 and url_norm not in _ATS_NAMES:
            # Clear mismatch — URL company wins
            log.info(
                f"URL-COMPANY FIX: '{hint_company}' -> '{url_company}' "
                f"(similarity={similarity:.2f}, url={url[:60]})"
            )
            job["company"] = url_company
            job["_company_source"] = "url_validator"

            # Also re-extract title from URL if available
            url_title = extract_title_from_url(url)
            if url_title and len(url_title) > 8:
                old_title = job.get("title", "")
                # Only override if current title also seems wrong
                title_sim = _fuzzy_match(old_title, url_title)
                if title_sim < 0.3:
                    log.info(f"URL-TITLE FIX: '{old_title}' -> '{url_title}'")
                    job["title"] = url_title
                    job["_title_source"] = "url_validator"

        # Self-learning: cache this domain -> company mapping
        try:
            cache = _load_url_cache()
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace("www.", "")
            domain_key = domain.split(".")[0] if "myworkdayjobs" in domain else domain
            if url_company and domain_key not in cache:
                cache[domain_key] = url_company
                _save_url_cache(cache)
        except Exception:
            pass

    return job


def validate_job_integrity(job):
    """
    Final integrity check before writing to sheets.
    Returns (is_valid, reason) tuple.
    """
    company = job.get("company", "")
    title = job.get("title", "")
    url = job.get("url", "")

    if not company or company == "Unknown":
        return False, "Empty company name"
    if not title or title == "Unknown":
        return False, "Empty title"
    if not url or not url.startswith("http"):
        return False, "Invalid URL"
    if len(company) < 2:
        return False, f"Company too short: {company}"
    if len(title) < 3:
        return False, f"Title too short: {title}"
    # Check for obviously wrong data
    if company.lower() in _ATS_NAMES:
        return False, f"Company is ATS name: {company}"

    return True, "OK"
