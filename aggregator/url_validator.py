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


# Known Workday subdomain -> real company name
_WORKDAY_COMPANY_MAP = {
    "vst": "Vistra", "msigna": "MSIG USA", "haier": "GE Appliances",
    "edel": "Oracle", "ulse": "UL Solutions", "kbr": "KBR",
    "bloomenergy": "Bloom Energy", "thermofisher": "Thermo Fisher Scientific",
    "radiancetech": "Radiance Technologies", "cambiumlearning": "Cambium Learning",
    "geisinger": "Geisinger", "intel": "Intel", "cadence": "Cadence Design Systems",
    "aero": "AeroVironment", "rbc": "RBC", "aptiv": "Aptiv",
    "viavisolutions": "Viavi Solutions", "tutorperini": "Tutor Perini",
    "nvidia": "Nvidia",
}

# Known custom career site domains
_CUSTOM_CAREER_DOMAINS = {
    "mhicareers.com": "Mitsubishi Heavy Industries",
    "fahertybrand.com": "Faherty Brand",
    "amazon.jobs": "Amazon",
    "tesla.com": "Tesla",
}


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

    # Check custom career domains first (highest priority)
    for cd, cn in _CUSTOM_CAREER_DOMAINS.items():
        if cd in domain:
            return cn

    # Workday: check known company map BEFORE cache
    m = _WORKDAY_PATTERN.match(domain)
    if m:
        slug = m.group(1).lower()
        if slug in _WORKDAY_COMPANY_MAP:
            return _WORKDAY_COMPANY_MAP[slug]

    # Check URL-company cache
    cache = _load_url_cache()
    if "myworkdayjobs" in domain or "myworkdaysite" in domain:
        cache_key = domain.split(".")[0]
    elif any(ats in domain for ats in ["greenhouse", "lever", "ashby", "workable", "icims"]):
        path_parts = [p for p in path.split("/") if p and len(p) > 2]
        slug = path_parts[0] if path_parts else ""
        cache_key = f"{domain}/{slug}" if slug else None
    else:
        cache_key = domain
    if cache_key and cache_key in cache:
        return cache[cache_key]
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

    # (Amazon, Tesla, NVIDIA handled by _CUSTOM_CAREER_DOMAINS above)

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


def _is_authoritative_match(url):
    """Check if URL company comes from a KNOWN mapping (high confidence only).
    Only returns True for Workday known map and custom career domains.
    Path-based ATS (Greenhouse, Lever, Ashby) use fuzzy matching instead."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        # Custom career domains — always authoritative
        for cd in _CUSTOM_CAREER_DOMAINS:
            if cd in domain:
                return True
        # Workday known map — always authoritative
        m = _WORKDAY_PATTERN.match(domain)
        if m and m.group(1).lower() in _WORKDAY_COMPANY_MAP:
            return True
    except Exception:
        pass
    return False


def _normalize(s):
    """Normalize string for comparison."""
    return re.sub(r"[^a-z0-9]", "", s.lower()) if s else ""


def _fuzzy_match(a, b):
    """Smart fuzzy match for company names.
    
    Key insight: URL slugs often append/prepend words to company names.
    'gelbergroup' contains 'gelber', so they match.
    'astranis' does NOT contain 'sieve', so they don't match.
    """
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    # Substring check: if one is fully contained in the other, it's a match
    if na in nb or nb in na:
        return 0.9
    # Check if the shorter string is a prefix of the longer
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    if longer.startswith(shorter):
        return 0.85
    # Levenshtein-based: for similar-length strings
    if abs(len(na) - len(nb)) <= 2:
        dist = _edit_distance(na, nb)
        max_len = max(len(na), len(nb))
        if max_len > 0:
            ratio = 1.0 - (dist / max_len)
            return ratio
    # Low similarity for completely different strings
    # Only count if significant character sequences overlap
    common_len = _longest_common_substring_len(na, nb)
    return common_len / max(len(na), len(nb))


def _edit_distance(s1, s2):
    """Levenshtein distance."""
    if len(s1) < len(s2):
        return _edit_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[len(s2)]


def _longest_common_substring_len(a, b):
    """Length of longest common substring."""
    if not a or not b:
        return 0
    m, n = len(a), len(b)
    prev = [0] * (n + 1)
    best = 0
    for i in range(1, m + 1):
        curr = [0] * (n + 1)
        for j in range(1, n + 1):
            if a[i-1] == b[j-1]:
                curr[j] = prev[j-1] + 1
                best = max(best, curr[j])
        prev = curr
    return best


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

        # Check if URL company is from an authoritative source (known map)
        is_authoritative = _is_authoritative_match(url)

        # Check if they match
        similarity = _fuzzy_match(hint_company, url_company)

        if (is_authoritative and hint_norm != url_norm) or (similarity < 0.4 and url_norm not in _ATS_NAMES):
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

        # Self-learning: cache this URL pattern -> company mapping
        try:
            cache = _load_url_cache()
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace("www.", "")
            # For Workday: use subdomain as key (unique per company)
            if "myworkdayjobs" in domain or "myworkdaysite" in domain:
                cache_key = domain.split(".")[0]
            # For path-based ATS: use domain + company slug as key
            elif any(ats in domain for ats in ["greenhouse", "lever", "ashby", "workable", "icims"]):
                path_parts = [p for p in parsed.path.split("/") if p and len(p) > 2]
                slug = path_parts[0] if path_parts else ""
                cache_key = f"{domain}/{slug}" if slug else None
            else:
                cache_key = domain
            if url_company and cache_key and cache_key not in cache:
                cache[cache_key] = url_company
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
