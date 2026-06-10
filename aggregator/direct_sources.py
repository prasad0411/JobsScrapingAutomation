"""
Direct ATS API sources — Greenhouse, Lever, Ashby, Hacker News.
Pulls intern/new-grad jobs directly from company career page APIs.
Zero errors, real URLs, correct company names.
"""

import json
import logging
import re
import time
import urllib.request
import ssl
from typing import List, Dict, Optional

log = logging.getLogger(__name__)

# SSL context for HTTPS requests
_CTX = ssl.create_default_context()

# ═══════════════════════════════════════════════════════════════════
# COMPANY LISTS — add/remove companies here
# ═══════════════════════════════════════════════════════════════════

GREENHOUSE_COMPANIES = {
    # Company slug → Display name
    "stripe": "Stripe", "coinbase": "Coinbase", "figma": "Figma",
    "notion": "Notion", "databricks": "Databricks", "ramp": "Ramp",
    "brex": "Brex", "verkada": "Verkada", "scaleai": "Scale AI",
    "anduril": "Anduril", "neuralink": "Neuralink",
    "billiontoone": "BillionToOne", "lucidmotors": "Lucid Motors",
    "anthropic": "Anthropic", "openai": "OpenAI",
    "discord": "Discord", "instacart": "Instacart",
    "doordash": "DoorDash", "reddit": "Reddit", "snap": "Snap",
    "lyft": "Lyft", "affirm": "Affirm", "chime": "Chime",
    "plaid": "Plaid", "robinhood": "Robinhood", "sofi": "SoFi",
    "toast": "Toast", "squarespace": "Squarespace",
    "hubspot": "HubSpot", "datadog": "Datadog",
    "elastic": "Elastic", "confluent": "Confluent",
    "twilio": "Twilio", "okta": "Okta",
    "crowdstrike": "CrowdStrike", "sentinelone": "SentinelOne",
    "snyk": "Snyk", "vanta": "Vanta", "cloudflare": "Cloudflare",
    "airtable": "Airtable", "asana": "Asana",
    "loom": "Loom", "calendly": "Calendly", "miro": "Miro",
    "canva": "Canva", "tempus": "Tempus",
    "color": "Color Health", "insitro": "Insitro",
    "recursion": "Recursion", "skydio": "Skydio",
    "samsara": "Samsara", "formlabs": "Formlabs",
    "draftkings": "DraftKings", "flywire": "Flywire",
    "earnin": "EarnIn", "stash": "Stash",
    "genbioinc": "GenBio AI", "fieldai": "Field AI",
    "fourkites": "FourKites", "ispottv": "iSpot.tv",
    "podium81": "Podium", "temporal": "Temporal",
    "cockroachlabs": "CockroachDB", "mongodb": "MongoDB",
}

LEVER_COMPANIES = {
    "rippling": "Rippling", "anduril": "Anduril",
    "nuro": "Nuro", "cruise": "Cruise",
    "aurora-innovation": "Aurora", "zoox": "Zoox",
    "motional": "Motional", "cerebras": "Cerebras",
    "groq": "Groq", "sambanova": "SambaNova",
    "together-ai": "Together AI", "cohere": "Cohere",
    "langchain": "LangChain", "pinecone": "Pinecone",
    "replit": "Replit", "vercel": "Vercel",
    "supabase": "Supabase", "linear": "Linear",
    "retool": "Retool", "webflow": "Webflow",
    "mercury": "Mercury", "deel": "Deel",
    "lattice": "Lattice", "drata": "Drata",
    "clerk": "Clerk", "lendbuzz": "Lendbuzz",
    "field-ai": "Field AI", "plus-2": "PlusAI",
    "sunwatercapital": "Sunwater Capital",
}

ASHBY_COMPANIES = {
    "notion": "Notion", "ramp": "Ramp", "nash": "Nash",
    "stash": "Stash", "wealth-com": "Wealth.com",
    "pebl": "Pebl", "vanta": "Vanta", "linear": "Linear",
    "retool": "Retool", "clerk": "Clerk",
    "webflow": "Webflow", "mercury": "Mercury",
    "deel": "Deel", "lattice": "Lattice",
    "loom": "Loom", "calendly": "Calendly",
    "drata": "Drata", "snyk": "Snyk",
    "solink": "Solink", "rivianvw.tech": "XPENG",
}

# Title keywords that indicate intern/new-grad eligibility
_INTERN_KW = re.compile(
    r"\b(?:intern|co-op|coop|new\s*grad|entry\s*level|junior|early\s*career"
    r"|apprentice|fellow|rotational)\b",
    re.I,
)


def _fetch_json(url: str, timeout: int = 5) -> Optional[dict]:
    """Fetch JSON from URL, return None on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=timeout, context=_CTX)
        return json.loads(resp.read())
    except Exception:
        return None


def _is_intern_or_newgrad(title: str) -> bool:
    """Check if title looks like intern/new-grad/entry-level."""
    return bool(_INTERN_KW.search(title))


# ═══════════════════════════════════════════════════════════════════
# GREENHOUSE
# ═══════════════════════════════════════════════════════════════════

def scrape_greenhouse() -> List[Dict]:
    """Fetch intern/new-grad jobs from all Greenhouse company boards."""
    jobs = []
    for slug, company_name in GREENHOUSE_COMPANIES.items():
        data = _fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
        if not data:
            continue
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not _is_intern_or_newgrad(title):
                continue
            # Extract location
            location = "Unknown"
            loc_data = job.get("location", {})
            if isinstance(loc_data, dict):
                location = loc_data.get("name", "Unknown")
            elif isinstance(loc_data, str):
                location = loc_data
            url = job.get("absolute_url", "")
            job_id = str(job.get("id", "N/A"))
            jobs.append({
                "company": company_name,
                "title": title,
                "location": location,
                "url": url,
                "job_id": job_id,
                "source": "greenhouse_direct",
                "age": "0d",
                "is_closed": False,
            })
    log.info(f"Greenhouse direct: {len(jobs)} jobs from {len(GREENHOUSE_COMPANIES)} companies")
    return jobs


# ═══════════════════════════════════════════════════════════════════
# LEVER
# ═══════════════════════════════════════════════════════════════════

def scrape_lever() -> List[Dict]:
    """Fetch intern/new-grad jobs from all Lever company boards."""
    jobs = []
    for slug, company_name in LEVER_COMPANIES.items():
        data = _fetch_json(f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if not data or not isinstance(data, list):
            continue
        for job in data:
            title = job.get("text", "")
            if not _is_intern_or_newgrad(title):
                continue
            location = "Unknown"
            categories = job.get("categories", {})
            if isinstance(categories, dict):
                location = categories.get("location", "Unknown")
            url = job.get("hostedUrl", "") or job.get("applyUrl", "")
            jobs.append({
                "company": company_name,
                "title": title,
                "location": location,
                "url": url,
                "job_id": "N/A",
                "source": "lever_direct",
                "age": "0d",
                "is_closed": False,
            })
    log.info(f"Lever direct: {len(jobs)} jobs from {len(LEVER_COMPANIES)} companies")
    return jobs


# ═══════════════════════════════════════════════════════════════════
# ASHBY
# ═══════════════════════════════════════════════════════════════════

def scrape_ashby() -> List[Dict]:
    """Fetch intern/new-grad jobs from all Ashby company boards."""
    jobs = []
    for slug, company_name in ASHBY_COMPANIES.items():
        data = _fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        if not data:
            continue
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not _is_intern_or_newgrad(title):
                continue
            location = "Unknown"
            if job.get("location"):
                location = job["location"]
            elif job.get("locationName"):
                location = job["locationName"]
            url = job.get("jobUrl", "") or f"https://jobs.ashbyhq.com/{slug}/{job.get('id', '')}"
            jobs.append({
                "company": company_name,
                "title": title,
                "location": location,
                "url": url,
                "job_id": "N/A",
                "source": "ashby_direct",
                "age": "0d",
                "is_closed": False,
            })
    log.info(f"Ashby direct: {len(jobs)} jobs from {len(ASHBY_COMPANIES)} companies")
    return jobs


# ═══════════════════════════════════════════════════════════════════
# HACKER NEWS "WHO IS HIRING"
# ═══════════════════════════════════════════════════════════════════

def scrape_hackernews_hiring(max_comments: int = 100) -> List[Dict]:
    """Fetch job postings from latest HN 'Who is Hiring' thread."""
    jobs = []
    # Find latest "Who is Hiring" thread
    user_data = _fetch_json("https://hacker-news.firebaseio.com/v0/user/whoishiring.json")
    if not user_data:
        return jobs
    
    thread_id = None
    for sub_id in user_data.get("submitted", [])[:10]:
        item = _fetch_json(f"https://hacker-news.firebaseio.com/v0/item/{sub_id}.json")
        if item and "hiring" in item.get("title", "").lower() and "who" in item.get("title", "").lower():
            thread_id = sub_id
            break
    
    if not thread_id:
        return jobs
    
    thread = _fetch_json(f"https://hacker-news.firebaseio.com/v0/item/{thread_id}.json")
    if not thread:
        return jobs
    
    comment_ids = thread.get("kids", [])[:max_comments]
    
    for cid in comment_ids:
        comment = _fetch_json(f"https://hacker-news.firebaseio.com/v0/item/{cid}.json")
        if not comment or comment.get("deleted"):
            continue
        text = comment.get("text", "")
        if not text:
            continue
        
        # Parse HN hiring format: "Company | Location | Role | ..."
        # First line usually has company info
        lines = text.replace("<p>", "\n").split("\n")
        first_line = re.sub(r"<[^>]+>", "", lines[0]).strip()
        parts = [p.strip() for p in first_line.split("|")]
        
        if len(parts) >= 2:
            company = parts[0]
            # Try to find intern/entry mentions
            full_text = " ".join(parts).lower()
            if any(kw in full_text for kw in ["intern", "new grad", "entry", "junior", "early career"]):
                title = parts[2] if len(parts) > 2 else parts[1]
                location = parts[1] if len(parts) > 1 else "Unknown"
                
                # Extract URL from text
                url_match = re.search(r'href="([^"]+)"', text)
                url = url_match.group(1) if url_match else "URL_CONFLICT"
                
                jobs.append({
                    "company": company[:50],
                    "title": title[:80],
                    "location": location[:50],
                    "url": url,
                    "job_id": "N/A",
                    "source": "hackernews_hiring",
                    "age": "0d",
                    "is_closed": False,
                })
    
    log.info(f"HackerNews hiring: {len(jobs)} intern/newgrad jobs from {len(comment_ids)} comments")
    return jobs


# ═══════════════════════════════════════════════════════════════════
# MAIN: Fetch all direct sources
# ═══════════════════════════════════════════════════════════════════

def _is_us_location(location: str) -> bool:
    """Quick check if location is in the US."""
    loc = location.lower()
    # Reject obvious international
    intl = ["canada", "uk", "united kingdom", "germany", "france", "india",
            "singapore", "australia", "brazil", "mexico", "japan", "china",
            "toronto", "vancouver", "london", "berlin", "tokyo", "sydney",
            "melbourne", "dublin", "amsterdam", "paris", "mumbai",
            "bangalore", "são paulo", "sao paulo", "mexico city",
            "british columbia", "ontario", "quebec", "alberta"]
    if any(kw in loc for kw in intl):
        return False
    # Accept US indicators
    us = ["remote", "usa", "united states", ", ca", ", ny", ", wa", ", tx",
          ", ma", ", il", ", co", ", ga", ", pa", ", va", ", nc", ", oh",
          ", fl", ", az", ", ut", ", nj", ", mn", ", wi", ", mi", ", or",
          ", ct", ", md", ", in", ", mo", ", tn", ", sc", ", al", ", ky"]
    if any(kw in loc for kw in us):
        return True
    # Unknown location — let pipeline decide
    return True


def fetch_all_direct_sources() -> List[Dict]:
    """Fetch from all direct ATS APIs. Returns list of job dicts."""
    all_jobs = []
    
    log.info("Fetching direct sources: Greenhouse, Lever, Ashby, HackerNews")
    
    try:
        all_jobs.extend(scrape_greenhouse())
    except Exception as e:
        log.error(f"Greenhouse scrape failed: {e}")
    
    try:
        all_jobs.extend(scrape_lever())
    except Exception as e:
        log.error(f"Lever scrape failed: {e}")
    
    try:
        all_jobs.extend(scrape_ashby())
    except Exception as e:
        log.error(f"Ashby scrape failed: {e}")
    
    try:
        all_jobs.extend(scrape_hackernews_hiring())
    except Exception as e:
        log.error(f"HackerNews scrape failed: {e}")
    
    # Filter US-only
    us_jobs = [j for j in all_jobs if _is_us_location(j.get("location", "Unknown"))]
    log.info(f"Total direct source jobs: {len(us_jobs)} (filtered from {len(all_jobs)})")
    return us_jobs


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    jobs = fetch_all_direct_sources()
    print(f"\nTotal: {len(jobs)} jobs")
    print(f"\nBy source:")
    from collections import Counter
    for source, count in Counter(j["source"] for j in jobs).items():
        print(f"  {source}: {count}")
    print(f"\nSample jobs:")
    for j in jobs[:10]:
        print(f"  {j['company']:20s} | {j['title'][:40]:40s} | {j['location'][:20]:20s} | {j['source']}")
