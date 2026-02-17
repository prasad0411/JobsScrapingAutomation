import os
#!/usr/bin/env python3
"""Outreach Pipeline — Email Finder (cache → Reacher → API cascade)."""

import re, time, logging, requests
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from outreach.outreach_config import (
    CLEARBIT_URL,
    TLDS,
    REACHER_URL,
    REACHER_FROM,
    REACHER_TIMEOUT,
    REACHER_WORKERS,
    APIS,
    API_TIMEOUT,
    API_RETRIES,
    HUNTER_CONF,
    key,
)
from outreach.outreach_data import NameParser, PatternCache, Credits

log = logging.getLogger(__name__)
try:
    import dns.resolver
    _DNS = True
except ImportError:
    _DNS = False


class Finder:
    def __init__(self, credits: Credits):
        self.cr = credits
        self.pc = PatternCache()
        self._reacher = None
        self._dom = {}

    @staticmethod
    def _extract_name_from_linkedin_url(linkedin_url):
        """Extract full name from LinkedIn URL slug. e.g. /in/chira-kingpin/ → Chira Kingpin"""
        if not linkedin_url:
            return None
        import re
        m = re.search(r"linkedin\.com/in/([a-zA-Z0-9-]+)", linkedin_url)
        if not m:
            return None
        slug = m.group(1)
        # Remove trailing numbers (LinkedIn IDs like madeline-batista-72930372)
        slug = re.sub(r"-\d{5,}$", "", slug)
        # Convert slug to name
        parts = [p.capitalize() for p in slug.split("-") if p and len(p) > 1]
        if len(parts) >= 2:
            return " ".join(parts)
        return None

    def find(self, name, company, linkedin="", job_url_domain=""):
        r = {"email": "", "source": "", "status": "Failed", "error": ""}

        # If name is incomplete (single name or initial), try LinkedIn URL
        parsed = NameParser.parse(name)
        if parsed and (parsed["single"] or (parsed["last"] and len(parsed["last"]) <= 2)):
            li_name = self._extract_name_from_linkedin_url(linkedin)
            if li_name:
                log.info(f"Enriched name: '{name}' → '{li_name}' (from LinkedIn URL)")
                parsed = NameParser.parse(li_name)
                name = li_name

        if not parsed:
            r["error"] = "Cannot parse name"
            return r
        if parsed["single"] and not linkedin:
            r["status"] = "Manual Review"
            r["error"] = f"Single name '{name}'"
            return r
        # Check retry tracker — skip if failed 3+ times on same domain
        retry_key = company.strip().lower()
        retries = self._load_retries()
        if retry_key in retries and retries[retry_key].get("attempts", 0) >= 3:
            old_domain = retries[retry_key].get("domain", "")
            r["error"] = f"Skipped (3+ failures on {old_domain}). Add override in .local/domain_overrides.json"
            r["status"] = "Manual Review"
            log.debug(f"Skipping {company}: {r['error']}")
            return r

        # Priority 1: domain_overrides.json
        # Priority 2: Job URL domain
        # Priority 3: Clearbit
        override_domain = self._get_override(company)
        if override_domain:
            domains = [override_domain]
            log.info(f"Domain override: {company} → {override_domain}")
        elif job_url_domain:
            domains = [job_url_domain]
            log.info(f"Job URL domain: {company} → {job_url_domain}")
        else:
            domains = self._resolve(company)
        if not domains:
            r["status"] = "Manual Review"
            r["error"] = f"No domain for '{company}'"
            return self._apis(parsed, "", linkedin, r) if linkedin else r
        if self._rok():
            for d in domains:
                email = self.pc.gen_single(parsed, d)
                if email and self._verify(email) == "safe":
                    r.update(email=email, source="cache", status="Valid")
                    return r
        if self._rok():
            # Catch-all detection: test canary email
            if self._is_catchall(domains[0]):
                log.debug(f"Catch-all detected: {domains[0]} — skipping Reacher patterns")
            else:
                pr = self._psearch(parsed, domains)
                if pr["status"] in ("Valid", "Manual Review"):
                    if pr["email"] and pr["status"] == "Valid":
                        self.pc.detect(pr["email"], parsed)
                        self._clear_retry(retry_key)
                    return pr
        result = self._apis(parsed, domains[0], linkedin, r)
        if result["email"] and result["status"] == "Valid":
            self.pc.detect(result["email"], parsed)
            self._clear_retry(retry_key)
        else:
            self._track_retry(retry_key, domains[0] if domains else "", result.get("error", ""))
        return result

    def _resolve(self, company):
        if not company:
            return []
        k = company.strip().lower()
        if k in self._dom:
            return self._dom[k]
        doms = []
        try:
            resp = requests.get(
                CLEARBIT_URL, params={"query": company}, timeout=API_TIMEOUT
            )
            if resp.status_code == 200:
                doms = [x["domain"] for x in resp.json()[:3] if x.get("domain")]
        except:
            pass
        if not doms:
            clean = self._clean(company)
            if clean:
                cands = set()
                ns, hy = clean.replace(" ", ""), clean.replace(" ", "-")
                for t in TLDS:
                    cands.update([f"{ns}{t}", f"{hy}{t}"])
                if "&" in company:
                    na = (
                        clean.replace("&", "")
                        .replace("  ", " ")
                        .strip()
                        .replace(" ", "")
                    )
                    av = (
                        clean.replace("&", "and")
                        .replace("  ", " ")
                        .strip()
                        .replace(" ", "")
                    )
                    cands.update([f"{na}.com", f"{av}.com"])
                doms = [d for d in cands if self._mx(d)][:3]
        if not doms:
            clean = self._clean(company)
            if clean:
                d = f"{clean.replace(' ','')}.com"
                if self._mx(d):
                    doms = [d]
        unique = list(dict.fromkeys(d.lower() for d in doms))
        self._dom[k] = unique
        return unique

    @staticmethod
    def _clean(name):
        c = name.strip()
        for s in [
            r"\s*,?\s*inc\.?$",
            r"\s*,?\s*llc\.?$",
            r"\s*,?\s*ltd\.?$",
            r"\s*,?\s*corp\.?$",
            r"\s*,?\s*co\.?$",
            r"\s*,?\s*corporation$",
            r"\s*,?\s*company$",
            r"\s*,?\s*technologies$",
            r"\s*,?\s*technology$",
            r"\s*,?\s*group$",
            r"\s*,?\s*holdings$",
            r"\s*,?\s*solutions$",
        ]:
            c = re.sub(s, "", c, flags=re.IGNORECASE)
        return re.sub(r"[^a-zA-Z0-9\s\-&]", "", c).strip().lower()

    @staticmethod
    @lru_cache(maxsize=500)
    def _mx(domain):
        if _DNS:
            try:
                dns.resolver.resolve(domain, "MX")
                return True
            except:
                pass
            try:
                dns.resolver.resolve(domain, "A")
                return True
            except:
                return False
        else:
            import socket

            try:
                socket.getaddrinfo(domain, None)
                return True
            except:
                return False

    def _psearch(self, parsed, domains):
        r = {"email": "", "source": "pattern", "status": "Failed", "error": ""}
        if parsed["single"]:
            r["error"] = "Single name"
            return r
        pa, pb, pc = NameParser.gen_phased(parsed, domains)
        for e in pa:
            if self._verify(e) == "safe":
                r.update(email=e, status="Valid")
                return r
        hit = self._par(pb)
        if hit:
            r.update(**hit)
            return r
        hit = self._par(pc)
        if hit:
            r.update(**hit)
            return r
        r["error"] = "No valid via patterns"
        return r

    def _par(self, emails):
        if not emails:
            return None
        valid, risky, stop = [], [], False

        def chk(e):
            return e, self._verify(e)

        with ThreadPoolExecutor(max_workers=REACHER_WORKERS) as ex:
            futs = {ex.submit(chk, e): e for e in emails}
            for fut in as_completed(futs):
                if stop:
                    break
                try:
                    e, v = fut.result()
                    if v == "safe":
                        valid.append(e)
                        stop = True
                    elif v == "risky":
                        risky.append(e)
                except:
                    pass
            for f in futs:
                f.cancel()
        if len(valid) == 1:
            return {
                "email": valid[0],
                "source": "pattern",
                "status": "Valid",
                "error": "",
            }
        if len(valid) > 1:
            return {
                "email": valid[0],
                "source": "pattern",
                "status": "Manual Review",
                "error": f"Catch-all:{len(valid)}",
            }
        if risky:
            return {
                "email": risky[0],
                "source": "pattern",
                "status": "Manual Review",
                "error": f"Risky:{risky[0]}",
            }
        return None

    def _verify(self, email):
        try:
            resp = requests.post(
                REACHER_URL,
                json={"to_email": email, "from_email": REACHER_FROM},
                timeout=REACHER_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json().get("is_reachable", "unknown")
        except:
            pass
        return "unknown"

    @staticmethod
    def _get_override(company):
        """Check domain_overrides.json for manual domain corrections."""
        try:
            if os.path.exists(OVERRIDES_FILE):
                overrides = json.load(open(OVERRIDES_FILE))
                key = company.strip()
                # Try exact match first, then case-insensitive
                if key in overrides:
                    return overrides[key]
                for k, v in overrides.items():
                    if k.lower() == key.lower():
                        return v
        except:
            pass
        return ""

    @staticmethod
    def _load_retries():
        try:
            if os.path.exists(RETRY_FILE):
                return json.load(open(RETRY_FILE))
        except:
            pass
        return {}

    @staticmethod
    def _track_retry(company_key, domain, error):
        retries = EmailFinder._load_retries()
        if company_key not in retries:
            retries[company_key] = {"attempts": 0, "domain": domain, "error": ""}
        retries[company_key]["attempts"] += 1
        retries[company_key]["domain"] = domain
        retries[company_key]["error"] = error[:200]
        try:
            json.dump(retries, open(RETRY_FILE, "w"), indent=2)
        except:
            pass

    @staticmethod
    def _clear_retry(company_key):
        retries = EmailFinder._load_retries()
        if company_key in retries:
            del retries[company_key]
            try:
                json.dump(retries, open(RETRY_FILE, "w"), indent=2)
            except:
                pass

    def _is_catchall(self, domain):
        """Test if domain accepts all emails (catch-all)."""
        if not self._rok():
            return False
        try:
            canary = f"xq7z9k2m8p@{domain}"
            resp = requests.post(
                REACHER_URL,
                json={"to_email": canary, "from_email": "test@example.org"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                reachable = data.get("is_reachable", "unknown")
                if reachable == "safe":
                    log.info(f"Catch-all domain: {domain}")
                    return True
        except:
            pass
        return False

    def _rok(self):
        if self._reacher is not None:
            return self._reacher
        try:
            resp = requests.get(REACHER_URL.replace("/v0/check_email", "/"), timeout=3)
            self._reacher = resp.status_code in (200, 404, 405)
        except:
            # Auto-start Docker Desktop + Reacher on macOS
            log.info("Reacher not running — attempting to start Docker Desktop...")
            try:
                import subprocess, platform

                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                compose_file = os.path.join(root, "docker-compose.yml")

                # Step 1: Check if Docker daemon is running
                daemon_running = False
                try:
                    dc = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
                    daemon_running = dc.returncode == 0
                except:
                    pass

                # Step 2: If daemon not running, launch Docker Desktop (macOS)
                if not daemon_running and platform.system() == "Darwin":
                    log.info("Docker daemon not running. Launching Docker Desktop...")
                    print("  Starting Docker Desktop...")
                    subprocess.run(["open", "-a", "Docker"], capture_output=True, timeout=10)
                    import time
                    for attempt in range(30):
                        time.sleep(2)
                        try:
                            check = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
                            if check.returncode == 0:
                                log.info(f"Docker Desktop ready after {(attempt+1)*2}s")
                                print(f"  Docker Desktop ready ({(attempt+1)*2}s)")
                                daemon_running = True
                                break
                        except:
                            pass
                    if not daemon_running:
                        log.warning("Docker Desktop did not start within 60s")
                        print("  Docker Desktop did not start within 60s")

                # Step 3: Start Reacher container
                if daemon_running:
                    compose_args = ["docker", "compose"]
                    if os.path.exists(compose_file):
                        compose_args.extend(["-f", compose_file])
                    compose_args.extend(["up", "-d"])
                    result = subprocess.run(compose_args, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        log.info("Reacher container started. Waiting 5s...")
                        import time; time.sleep(5)
                        try:
                            resp = requests.get(REACHER_URL.replace("/v0/check_email", "/"), timeout=3)
                            self._reacher = resp.status_code in (200, 404, 405)
                            if self._reacher:
                                log.info("Reacher is now running")
                                print("  Reacher email verifier ready")
                                return self._reacher
                        except:
                            pass
                    else:
                        log.warning(f"Docker compose failed: {result.stderr[:500]}")

            except FileNotFoundError:
                log.warning("Docker not found. Install Docker Desktop from docker.com")
            except subprocess.TimeoutExpired:
                log.warning("Docker command timed out")
            except Exception as e:
                log.warning(f"Docker auto-start failed: {e}")
            self._reacher = False
            log.warning("Reacher unavailable. Pattern-based email verification disabled.")
        return self._reacher

    def _apis(self, p, dom, li, r):
        for fn in [
            lambda: self._apollo(p, dom, li),
            lambda: self._hunter(p, dom) if dom else None,
            lambda: self._snov(p, dom) if dom else None,
        ]:
            res = fn()
            if res and res["status"] in ("Valid", "Manual Review"):
                return res
        r["error"] = "All methods exhausted"
        return r

    def _req(self, method, url, **kw):
        kw.setdefault("timeout", API_TIMEOUT)
        for i in range(API_RETRIES):
            try:
                resp = requests.request(method, url, **kw)
                if resp.status_code == 429:
                    time.sleep(2**i)
                    continue
                return resp
            except requests.Timeout:
                if i < API_RETRIES - 1:
                    time.sleep(2**i)
            except:
                break
        return None

    def _apollo(self, p, dom, li):
        r = {"email": "", "source": "apollo", "status": "Failed", "error": ""}
        if self.cr.avail("apollo") <= 0:
            r["error"] = "Apollo exhausted"
            return r
        k = key("APOLLO_API_KEY")
        if not k:
            r["error"] = "Apollo key missing"
            return r
        payload = {"api_key": k}
        if li:
            payload["linkedin_url"] = li
        else:
            payload.update(first_name=p["fa"], last_name=p["la"], organization_name=dom)
        try:
            resp = self._req("POST", APIS["apollo"]["url"], json=payload)
            if resp and resp.status_code == 200:
                per = resp.json().get("person") or {}
                email = per.get("email", "")
                self.cr.use("apollo")
                if email:
                    r["email"] = email
                    r["status"] = (
                        "Valid"
                        if per.get("email_status") == "verified"
                        else "Manual Review"
                    )
                    return r
                r["error"] = "Apollo: no email"
        except Exception as e:
            r["error"] = f"Apollo:{str(e)[:80]}"
        return r

    def _hunter(self, p, dom):
        r = {"email": "", "source": "hunter", "status": "Failed", "error": ""}
        if self.cr.avail("hunter") <= 0:
            r["error"] = "Hunter exhausted"
            return r
        k = key("HUNTER_API_KEY")
        if not k:
            r["error"] = "Hunter key missing"
            return r
        try:
            resp = self._req(
                "GET",
                APIS["hunter"]["url"],
                params={
                    "domain": dom,
                    "first_name": p["fa"],
                    "last_name": p["la"],
                    "api_key": k,
                },
            )
            if resp and resp.status_code == 200:
                d = resp.json().get("data") or {}
                email, conf = d.get("email", ""), d.get("confidence", 0)
                self.cr.use("hunter")
                if email:
                    r["email"] = email
                    r["status"] = "Valid" if conf >= HUNTER_CONF else "Manual Review"
                    return r
                r["error"] = "Hunter: no email"
        except Exception as e:
            r["error"] = f"Hunter:{str(e)[:80]}"
        return r

    def _snov(self, p, dom):
        r = {"email": "", "source": "snov", "status": "Failed", "error": ""}
        if self.cr.avail("snov") <= 0:
            r["error"] = "Snov exhausted"
            return r
        sk, uid = key("SNOV_API_KEY"), key("SNOV_USER_ID")
        if not sk or not uid:
            r["error"] = "Snov key missing"
            return r
        try:
            resp = self._req(
                "POST",
                APIS["snov"]["url"],
                json={
                    "firstName": p["fa"],
                    "lastName": p["la"],
                    "domain": dom,
                    "userId": uid,
                    "secret": sk,
                },
            )
            if resp and resp.status_code == 200:
                ems = resp.json().get("emails", [])
                self.cr.use("snov")
                if ems:
                    best = next((e for e in ems if e.get("status") == "valid"), ems[0])
                    r["email"] = best.get("email", "")
                    r["status"] = (
                        "Valid" if best.get("status") == "valid" else "Manual Review"
                    )
                    return r
                r["error"] = "Snov: no emails"
        except Exception as e:
            r["error"] = f"Snov:{str(e)[:80]}"
        return r
