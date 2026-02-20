#!/usr/bin/env python3
"""
Provider-Aware Email Verification
  - MX lookup: detect Google Workspace / Microsoft 365 / Other
  - Google: gxlu endpoint (free, definitive)
  - Microsoft: GetCredentialType (free, definitive)
  - Caches all results to avoid repeat lookups
"""
import os, json, time, random, logging, hashlib

log = logging.getLogger(__name__)

try:
    import dns.resolver
    _DNS = True
except ImportError:
    _DNS = False

try:
    import requests
except ImportError:
    requests = None

_LOCAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".local")
MX_CACHE_FILE = os.path.join(_LOCAL, "mx_cache.json")
EMAIL_VERIFY_CACHE_FILE = os.path.join(_LOCAL, "email_verify_cache.json")

_GOOGLE_KW = ("google", "gmail", "aspmx", "googlemail")
_MICROSOFT_KW = ("outlook", "microsoft", "hotmail", "office365", "protection.outlook")
_SKIP_GXLU = {"gmail.com", "google.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com"}
_GXLU_DELAY = 2.0
_MS365_DELAY = 3.0
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


class ProviderVerifier:
    def __init__(self):
        os.makedirs(_LOCAL, exist_ok=True)
        self._mx_cache = self._load(MX_CACHE_FILE)
        self._email_cache = self._load(EMAIL_VERIFY_CACHE_FILE)
        self._catchall = {}
        self._session = requests.Session() if requests else None

    @staticmethod
    def _load(path):
        if os.path.exists(path):
            try:
                return json.load(open(path))
            except Exception:
                pass
        return {}

    @staticmethod
    def _save(path, data):
        try:
            json.dump(data, open(path, "w"), indent=2)
        except Exception:
            pass

    # ── MX Provider Detection ─────────────────────────────────

    def get_provider(self, domain):
        domain = domain.lower().strip()
        if not domain:
            return "other"
        if domain in self._mx_cache:
            return self._mx_cache[domain]
        provider = self._mx_lookup(domain)
        self._mx_cache[domain] = provider
        self._save(MX_CACHE_FILE, self._mx_cache)
        if provider != "other":
            log.info(f"MX detected: {domain} -> {provider}")
        return provider

    def _mx_lookup(self, domain):
        if not _DNS:
            return self._mx_fallback(domain)
        try:
            answers = dns.resolver.resolve(domain, "MX", lifetime=10)
            for rdata in answers:
                host = str(rdata.exchange).lower().rstrip(".")
                if any(kw in host for kw in _GOOGLE_KW):
                    return "google"
                if any(kw in host for kw in _MICROSOFT_KW):
                    return "microsoft"
            return "other"
        except dns.resolver.NXDOMAIN:
            return "other"
        except dns.resolver.NoAnswer:
            return "other"
        except dns.resolver.NoNameservers:
            return "other"
        except Exception:
            return self._mx_fallback(domain)

    @staticmethod
    def _mx_fallback(domain):
        import subprocess
        try:
            result = subprocess.run(
                ["dig", "+short", "MX", domain],
                capture_output=True, text=True, timeout=10,
            )
            output = result.stdout.lower()
            if any(kw in output for kw in _GOOGLE_KW):
                return "google"
            if any(kw in output for kw in _MICROSOFT_KW):
                return "microsoft"
        except Exception:
            pass
        return "other"

    # ── Email Verification Router ─────────────────────────────

    def verify_email(self, email, domain=None):
        if not email or "@" not in email:
            return "unknown"
        email_lower = email.lower().strip()
        if domain is None:
            domain = email_lower.split("@")[1]
        domain = domain.lower().strip()

        if email_lower in self._email_cache:
            return self._email_cache[email_lower]

        if domain in _SKIP_GXLU:
            return "unknown"

        provider = self.get_provider(domain)

        if provider == "google":
            return "unknown"
        elif provider == "microsoft":
            result = self._verify_microsoft(email_lower)
        else:
            return "unknown"

        if result in ("exists", "not_exists"):
            self._email_cache[email_lower] = result
            self._save(EMAIL_VERIFY_CACHE_FILE, self._email_cache)

        return result

    # ── Google Workspace (gxlu) ───────────────────────────────

    def _verify_google(self, email, domain):
        if self._is_google_catchall(domain):
            log.debug(f"Google catch-all: {domain}")
            return "unknown"
        return self._gxlu_request(email)

    def _is_google_catchall(self, domain):
        if domain in self._catchall:
            return self._catchall[domain]
        canary_hash = hashlib.md5(domain.encode()).hexdigest()[:8]
        canary = f"zxqtest{canary_hash}fake@{domain}"
        result = self._gxlu_request(canary)
        is_catchall = (result == "exists")
        self._catchall[domain] = is_catchall
        if is_catchall:
            log.info(f"Catch-all detected (Google): {domain}")
        return is_catchall

    def _gxlu_request(self, email):
        if not self._session:
            return "unknown"
        try:
            time.sleep(_GXLU_DELAY)
            resp = self._session.get(
                "https://mail.google.com/mail/gxlu",
                params={"email": email},
                headers={
                    "User-Agent": random.choice(_USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                timeout=10,
                allow_redirects=False,
            )
            cookies = resp.headers.get("Set-Cookie", "")
            if "COMPASS" in cookies:
                return "exists"
            return "not_exists"
        except Exception as e:
            log.debug(f"gxlu error: {email}: {e}")
            return "unknown"

    # ── Microsoft 365 ─────────────────────────────────────────

    def _verify_microsoft(self, email):
        if not self._session:
            return "unknown"
        try:
            time.sleep(_MS365_DELAY)
            resp = self._session.post(
                "https://login.microsoftonline.com/common/GetCredentialType",
                json={"Username": email},
                headers={
                    "User-Agent": random.choice(_USER_AGENTS),
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return "unknown"
            data = resp.json()
            ifexists = data.get("IfExistsResult", -1)
            if ifexists == 0:
                return "exists"
            elif ifexists == 1:
                return "not_exists"
            else:
                log.debug(f"ms365 code {ifexists} for {email}")
                return "unknown"
        except Exception as e:
            log.debug(f"ms365 error: {email}: {e}")
            return "unknown"

    # ── Pattern Discovery ─────────────────────────────────────

    # ── Website Pattern Mining ─────────────────────────────────

    _GENERIC = {"info", "hello", "press", "sales", "support", "contact",
                "admin", "help", "team", "hr", "jobs", "careers", "office",
                "marketing", "media", "security", "privacy", "legal",
                "feedback", "billing", "noreply", "no-reply", "webmaster"}

    def mine_website_pattern(self, domain):
        """Scrape company website for personal emails to learn the domain pattern."""
        if not self._session:
            return None
        domain = domain.lower().strip()

        # Check if already mined (cache key: "mined_<domain>")
        cache_key = f"mined_{domain}"
        if cache_key in self._mx_cache:
            return self._mx_cache[cache_key]

        import re
        urls = [
            f"https://{domain}/about",
            f"https://{domain}/team",
            f"https://{domain}/about-us",
            f"https://{domain}/contact",
            f"https://{domain}/leadership",
            f"https://{domain}/people",
            f"https://{domain}/",
        ]
        found = []
        for url in urls:
            try:
                time.sleep(1)
                r = self._session.get(
                    url,
                    headers={"User-Agent": random.choice(_USER_AGENTS)},
                    timeout=8,
                    allow_redirects=True,
                )
                emails = re.findall(
                    rf"[a-zA-Z0-9_.+-]+@{re.escape(domain)}", r.text
                )
                found.extend(emails)
            except Exception:
                continue

        # Filter: remove generic emails, keep personal ones
        personal = []
        for e in set(found):
            local = e.split("@")[0].lower()
            # Skip generic
            if local in self._GENERIC:
                continue
            # Skip if local part has no separator (likely generic like "info")
            # Personal emails typically have . _ or are flast pattern (3+ chars)
            if "." in local or "_" in local or "-" in local or len(local) >= 5:
                personal.append(e.lower())

        pattern = None
        if personal:
            # Try to detect pattern from first personal email found
            best = personal[0]
            local = best.split("@")[0]
            # Common pattern detection
            if "." in local:
                parts = local.split(".")
                if len(parts) == 2 and len(parts[0]) > 1 and len(parts[1]) > 1:
                    pattern = "{first}.{last}"
            elif "_" in local:
                parts = local.split("_")
                if len(parts) == 2 and len(parts[0]) > 1 and len(parts[1]) > 1:
                    pattern = "{first}_{last}"
            elif "-" in local:
                parts = local.split("-")
                if len(parts) == 2 and len(parts[0]) > 1 and len(parts[1]) > 1:
                    pattern = "{first}-{last}"
            if pattern:
                log.info(f"Website mining: {domain} -> {pattern} (from {best})")

        # Cache result (even None, to avoid re-scraping)
        self._mx_cache[cache_key] = pattern
        self._save(MX_CACHE_FILE, self._mx_cache)
        return pattern

    def discover_pattern(self, parsed, domain):
        provider = self.get_provider(domain)
        if provider != "microsoft":
            return None, None

        f = parsed.get("fa", "").lower()
        la = parsed.get("lc", "").lower()
        fi = parsed.get("fi", "").lower()

        if not f or not la:
            return None, None

        candidates = [
            (f"{f}.{la}@{domain}", "{first}.{last}"),
            (f"{fi}{la}@{domain}", "{f}{last}"),
            (f"{f}{la}@{domain}", "{first}{last}"),
            (f"{f}_{la}@{domain}", "{first}_{last}"),
            (f"{f}@{domain}", "{first}"),
        ]

        for email, pattern in candidates:
            result = self.verify_email(email, domain)
            if result == "exists":
                log.info(f"Pattern discovered: {domain} -> {pattern} (via {email})")
                return email, pattern
            elif result == "unknown":
                break
        return None, None
