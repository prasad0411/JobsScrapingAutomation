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
    log.warning("pip install dnspython for better MX checks")


class Finder:
    def __init__(self, credits: Credits):
        self.cr = credits
        self.pc = PatternCache()
        self._reacher = None
        self._dom_cache = {}

    def find(self, name, company, linkedin=""):
        r = {"email": "", "source": "", "status": "Failed", "error": ""}
        parsed = NameParser.parse(name)
        if not parsed:
            r["error"] = "Cannot parse name"
            return r
        if parsed["single"] and not linkedin:
            r["status"] = "Manual Review"
            r["error"] = f"Single name '{name}'"
            return r

        domains = self._resolve(company)
        if not domains:
            r["status"] = "Manual Review"
            r["error"] = f"No domain for '{company}'"
            return self._apis(parsed, "", linkedin, r) if linkedin else r

        # 1: Pattern cache hit → 1 check
        if self._rok():
            for d in domains:
                email = self.pc.gen_single(parsed, d)
                if email and self._verify(email) == "safe":
                    r.update(email=email, source="cache", status="Valid")
                    return r

        # 2: 3-phase pattern search
        if self._rok():
            pr = self._psearch(parsed, domains)
            if pr["status"] in ("Valid", "Manual Review"):
                if pr["email"] and pr["status"] == "Valid":
                    self.pc.detect(pr["email"], parsed)
                return pr

        # 3: API cascade
        result = self._apis(parsed, domains[0], linkedin, r)
        if result["email"] and result["status"] == "Valid":
            self.pc.detect(result["email"], parsed)
        return result

    # === Domain Resolution ===
    def _resolve(self, company):
        if not company:
            return []
        k = company.strip().lower()
        if k in self._dom_cache:
            return self._dom_cache[k]
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
        self._dom_cache[k] = unique
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

    # === Pattern Search ===
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

    # === Reacher ===
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

    def _rok(self):
        if self._reacher is not None:
            return self._reacher
        try:
            resp = requests.get(REACHER_URL.replace("/v0/check_email", "/"), timeout=3)
            self._reacher = resp.status_code in (200, 404, 405)
        except:
            self._reacher = False
            log.warning("Reacher not running. cd outreach && docker compose up -d")
        return self._reacher

    # === API Cascade ===
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
