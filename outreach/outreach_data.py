#!/usr/bin/env python3
"""Outreach Pipeline — Data Layer (Sheets, Credits, NameParser, PatternCache)."""

import os, re, json, time, datetime, logging, unicodedata
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from outreach.outreach_config import (
    SPREADSHEET,
    VALID_TAB,
    OUTREACH_TAB,
    SHEETS_CREDS,
    O_HEADERS,
    C,
    V_COMPANY,
    V_TITLE,
    V_JOBID,
    V_LOCATION,
    V_RESUME,
    SHEET_PAUSE,
    CREDITS_FILE,
    APIS,
    MAX_DAILY,
    PATTERNS_FILE,
    PAT_A,
    PAT_B,
    PAT_C,
    STRIP_PRE,
    STRIP_SUF,
    STATE_TO_TIMEZONE,
    TZ_DISPLAY,
    SEND_HOUR,
)

log = logging.getLogger(__name__)


class Sheets:
    _resume_cache = None
    _location_cache = None

    def __init__(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SHEETS_CREDS, scope)
        self.ss = gspread.authorize(creds).open(SPREADSHEET)
        self._ensure()

    def _ensure(self):
        try:
            self.ws = self.ss.worksheet(OUTREACH_TAB)
        except gspread.exceptions.WorksheetNotFound:
            self.ws = self.ss.add_worksheet(OUTREACH_TAB, rows=500, cols=len(O_HEADERS))
            log.info(f"Created '{OUTREACH_TAB}'")

        row1 = self.ws.row_values(1)
        if not row1 or row1[0] != O_HEADERS[0]:
            self._retry(
                self.ws.update,
                values=[O_HEADERS],
                range_name=f"A1:{_cl(len(O_HEADERS)-1)}1",
                value_input_option="RAW",
            )

            try:
                end = _cl(len(O_HEADERS) - 1)
                self.ws.format(
                    f"A1:{end}1",
                    {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Times New Roman",
                            "fontSize": 13,
                            "bold": True,
                        },
                        "backgroundColor": {
                            "red": 0.698,
                            "green": 0.898,
                            "blue": 0.698,
                        },
                    },
                )
            except:
                pass

            try:
                end = _cl(len(O_HEADERS) - 1)
                self.ws.format(
                    f"A2:{end}2000",
                    {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                    },
                )
            except:
                pass

            try:
                self.ss.batch_update(
                    {
                        "requests": [
                            {
                                "setDataValidation": {
                                    "range": {
                                        "sheetId": self.ws.id,
                                        "startRowIndex": 1,
                                        "endRowIndex": 2000,
                                        "startColumnIndex": 0,
                                        "endColumnIndex": len(O_HEADERS),
                                    },
                                    "rule": None,
                                }
                            },
                
                        ]
                    }
                )
            except:
                pass

            try:
                widths = []
                sizes = {
                    0: 60,
                    1: 200,
                    2: 250,
                    3: 100,
                    4: 140,
                    5: 150,
                    6: 180,
                    7: 140,
                    8: 150,
                    9: 180,
                    10: 200,
                    11: 140,
                    12: 200,
                }
                for i in range(len(O_HEADERS)):
                    widths.append(
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": self.ws.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": i,
                                    "endIndex": i + 1,
                                },
                                "properties": {"pixelSize": sizes.get(i, 150)},
                                "fields": "pixelSize",
                            }
                        }
                    )
                self.ss.batch_update({"requests": widths})
            except:
                pass

            try:
                self.ss.batch_update(
                    {
                        "requests": [
                            {
                                "updateSheetProperties": {
                                    "properties": {
                                        "sheetId": self.ws.id,
                                        "gridProperties": {"frozenRowCount": 1},
                                    },
                                    "fields": "gridProperties.frozenRowCount",
                                }
                            }
                        ]
                    }
                )
            except:
                pass

            self._p()
        # Always apply body formatting (runs every session, not just creation)
        try:
            end = _cl(len(O_HEADERS) - 1)
            self.ws.format(
                f"A2:{end}2000",
                {
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                },
            )
            self._p()
        except:
            pass

    def pull(self):
        try:
            valid = self.ss.worksheet(VALID_TAB)
        except gspread.exceptions.WorksheetNotFound:
            log.error(f"'{VALID_TAB}' not found")
            return 0

        vdata = valid.get_all_values()
        self._p()
        odata = self.ws.get_all_values()
        self._p()

        existing = set()
        for r in odata[1:]:
            r = _pad(r)
            co = r[C["company"]].strip().lower()
            ti = r[C["title"]].strip().lower()
            jid = r[C["job_id"]].strip().lower()
            if jid and jid != "n/a":
                existing.add(("jid", jid))
            if co:
                existing.add(("co_ti", f"{co}||{ti}"))

        new = []
        sr = len(odata)

        for row in vdata[1:]:
            while len(row) <= max(V_COMPANY, V_TITLE, V_JOBID):
                row = list(row) + [""]
            co = row[V_COMPANY].strip()
            ti = row[V_TITLE].strip() if len(row) > V_TITLE else ""
            jid = row[V_JOBID].strip() if len(row) > V_JOBID else ""
            if not co:
                continue
            jid_clean = jid.lower() if jid and jid.lower() != "n/a" else ""
            if jid_clean and ("jid", jid_clean) in existing:
                continue
            if not jid_clean and ("co_ti", f"{co.lower()}||{ti.lower()}") in existing:
                continue
            sr += 1
            nr = [""] * len(O_HEADERS)
            nr[C["sr"]] = str(sr - 1)
            nr[C["company"]] = co
            nr[C["title"]] = ti
            nr[C["job_id"]] = jid
            new.append(nr)
            if jid_clean:
                existing.add(("jid", jid_clean))
            existing.add(("co_ti", f"{co.lower()}||{ti.lower()}"))

        if new:
            start = len(odata) + 1
            end_row = start + len(new) - 1
            end_col = _cl(len(O_HEADERS) - 1)
            self._retry(
                self.ws.update,
                values=new,
                range_name=f"A{start}:{end_col}{end_row}",
                value_input_option="RAW",
            )
            self._p()
            # Format new rows: Times New Roman 13, centered
            try:
                self.ws.format(
                    f"A{start}:{end_col}{end_row}",
                    {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                    },
                )
            except:
                pass
        return len(new)

    def rows_for_extraction(self):
        data = self.ws.get_all_values()
        self._p()

        ecache = {}
        for r in data[1:]:
            r = _pad(r)
            co = r[C["company"]].strip().lower()
            for nk, ek in [
                (C["hm_name"], C["hm_email"]),
                (C["rec_name"], C["rec_email"]),
            ]:
                n, e = r[nk].strip().lower(), r[ek].strip()
                if n and e:
                    ecache[(n, co)] = e

        rows = []
        for i, r in enumerate(data[1:], start=2):
            r = _pad(r)
            co = r[C["company"]].strip()
            if not co:
                continue
            hn, rn = r[C["hm_name"]].strip(), r[C["rec_name"]].strip()
            he, re_ = r[C["hm_email"]].strip(), r[C["rec_email"]].strip()
            err = ""  # Error Log column removed — errors in .local/outreach.log

            hn_list = [n.strip() for n in hn.split(",") if n.strip()] if hn else []
            rn_list = [n.strip() for n in rn.split(",") if n.strip()] if rn else []
            hn = hn_list[0] if hn_list else ""
            rn = rn_list[0] if rn_list else ""

            need_h = bool(hn) and not he and "HM:" not in err
            need_r = bool(rn) and not re_ and "REC:" not in err

            if need_h:
                dup = ecache.get((hn.lower(), co.lower()))
                if dup:
                    self.write_email(i, "hm", dup, "cache")
                    need_h = False
            if need_r:
                dup = ecache.get((rn.lower(), co.lower()))
                if dup:
                    self.write_email(i, "rec", dup, "cache")
                    need_r = False

            if need_h or need_r:
                rows.append(
                    {
                        "row": i,
                        "co": co,
                        "title": r[C["title"]].strip(),
                        "jid": r[C["job_id"]].strip(),
                        "hn": hn,
                        "hli": r[C["hm_li"]].strip(),
                        "rn": rn,
                        "rli": r[C["rec_li"]].strip(),
                        "need_h": need_h,
                        "need_r": need_r,
                        "he": he,
                        "re": re_,
                    }
                )
        return rows

    def write_email(self, row, ct, email, source):
        try:
            col = C["hm_email"] if ct == "hm" else C["rec_email"]
            self._retry(self.ws.update_acell, f"{_cl(col)}{row}", email)
            log.info(f"Row {row} {ct}: {email} (via {source})")
            self._p()
        except Exception as e:
            log.error(f"write_email row {row}: {e}")

    def write_send_at(self, row, send_at_text, sent_date_text=""):
        try:
            self._retry(self.ws.update_acell, f"{_cl(C['send_at'])}{row}", send_at_text)
            self._p()
            if sent_date_text:
                self._retry(self.ws.update_acell, f"{_cl(C['sent_dt'])}{row}", sent_date_text)
                self._p()
        except Exception as e:
            log.error(f"write_send_at row {row}: {e}")

    def write_error(self, row, msg):
        """Errors logged to .local/outreach.log only — no sheet column."""
        log.debug(f"Row {row}: {msg}")

    def append_error(self, row, msg):
        """Errors logged to .local/outreach.log only — no sheet column."""
        log.debug(f"Row {row}: {msg}")

    def get_resume_type(self, company, title):
        if Sheets._resume_cache is None:
            try:
                valid = self.ss.worksheet("Valid Entries")
                rows = valid.get_all_values()
                self._p()
                Sheets._resume_cache = {}
                for row in rows[1:]:
                    if len(row) > V_RESUME:
                        key = (
                            row[V_COMPANY].strip().lower(),
                            row[V_TITLE].strip().lower(),
                        )
                        r = row[V_RESUME].strip()
                        Sheets._resume_cache[key] = r if r in ("SDE", "ML") else "SDE"
            except:
                Sheets._resume_cache = {}
        return Sheets._resume_cache.get(
            (company.strip().lower(), title.strip().lower()), "SDE"
        )

    def get_location(self, company, title):
        if Sheets._location_cache is None:
            try:
                valid = self.ss.worksheet("Valid Entries")
                rows = valid.get_all_values()
                self._p()
                Sheets._location_cache = {}
                for row in rows[1:]:
                    if len(row) > 8:
                        key = (
                            row[V_COMPANY].strip().lower(),
                            row[V_TITLE].strip().lower(),
                        )
                        Sheets._location_cache[key] = row[8].strip()
            except:
                Sheets._location_cache = {}
        return Sheets._location_cache.get(
            (company.strip().lower(), title.strip().lower()), ""
        )

    def get_job_url_domain(self, company, title):
        """Extract domain from Job URL in Valid Entries (column F, index 5)."""
        if not hasattr(Sheets, "_url_domain_cache") or Sheets._url_domain_cache is None:
            try:
                valid = self.ss.worksheet("Valid Entries")
                rows = valid.get_all_values()
                self._p()
                Sheets._url_domain_cache = {}
                for row in rows[1:]:
                    if len(row) > 5 and row[5].strip().startswith("http"):
                        key = (row[2].strip().lower(), row[3].strip().lower())
                        try:
                            from urllib.parse import urlparse
                            parsed = urlparse(row[5].strip())
                            domain = parsed.netloc.lower()
                            # Strip common prefixes
                            for prefix in ["www.", "jobs.", "careers.", "career.", "apply.", "recruiting.", "boards.greenhouse.io", "job-boards.greenhouse.io"]:
                                if domain.startswith(prefix) and domain != prefix.rstrip("."):
                                    domain = domain[len(prefix):]
                                    break
                            # Skip generic job boards — not the company domain
                            generic = {"lever.co", "greenhouse.io", "workday.com", "myworkdayjobs.com",
                                       "smartrecruiters.com", "icims.com", "ultipro.com", "taleo.net",
                                       "jobvite.com", "breezy.hr", "ashbyhq.com", "bamboohr.com",
                                       "jazz.co", "recruitee.com", "simplify.jobs", "linkedin.com",
                                       "indeed.com", "ziprecruiter.com", "glassdoor.com", "jobright.ai"}
                            if not any(g in domain for g in generic):
                                Sheets._url_domain_cache[key] = domain
                        except:
                            pass
            except:
                Sheets._url_domain_cache = {}
        return Sheets._url_domain_cache.get(
            (company.strip().lower(), title.strip().lower()), ""
        )

    def compute_send_at(self, location):
        """Compute send time at 10 AM in company's timezone, display in EST.
        Returns (send_at_str, sent_date_str) tuple."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            try:
                from backports.zoneinfo import ZoneInfo
            except ImportError:
                return self._fallback_send_at()

        try:
            tz_name = "US/Eastern"
            state_code = None

            if location:
                m = re.search(r",\s*([A-Z]{2})\b", location)
                if m:
                    state_code = m.group(1)
                if not state_code:
                    try:
                        from aggregator.config import FULL_STATE_NAMES

                        for name, code in FULL_STATE_NAMES.items():
                            if name in location.lower():
                                state_code = code
                                break
                    except:
                        pass
                if state_code and state_code in STATE_TO_TIMEZONE:
                    tz_name = STATE_TO_TIMEZONE[state_code]

            tz = ZoneInfo(tz_name)
            now = datetime.datetime.now(tz)
            target = now.replace(hour=SEND_HOUR, minute=0, second=0, microsecond=0)

            if now.hour >= SEND_HOUR:
                target += datetime.timedelta(days=1)
            while target.weekday() >= 5:
                target += datetime.timedelta(days=1)

            # Convert to EST for display
            est = ZoneInfo("US/Eastern")
            target_est = target.astimezone(est)
            h = target_est.hour
            ampm = "AM" if h < 12 else "PM"
            dh = h % 12 or 12
            send_at = target_est.strftime("%a %b %d, ") + f"{dh}:{target_est.minute:02d} {ampm} ET"
            sent_date = target_est.strftime("%b %d, %Y")
            return send_at, sent_date
        except Exception as e:
            log.debug(f"Timezone calc failed: {e}")
            return self._fallback_send_at()

    @staticmethod
    def _fallback_send_at():
        now = datetime.datetime.now()
        target = now.replace(hour=10, minute=0) + datetime.timedelta(days=1)
        while target.weekday() >= 5:
            target += datetime.timedelta(days=1)
        return target.strftime("%a %b %d, 10:00 AM ET"), target.strftime("%b %d, %Y")

    @staticmethod
    def _retry(func, *args, retries=3, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    wait = 2 ** (attempt + 1)
                    log.warning(f"Sheets rate limit, retrying in {wait}s...")
                    time.sleep(wait)
                elif attempt < retries - 1:
                    time.sleep(1)
                else:
                    raise

    def _p(self):
        time.sleep(SHEET_PAUSE)


class Credits:
    def __init__(self):
        self._d = {}
        self._load()

    def _default(self):
        t = datetime.datetime.now().strftime("%Y-%m-%d")
        d = {
            n: {"lim": c["limit"], "used": 0, "reset": t, "ok": True}
            for n, c in APIS.items()
        }
        d["gmail"] = {"lim": MAX_DAILY, "used": 0, "reset": t, "ok": True}
        return d

    def _load(self):
        if os.path.exists(CREDITS_FILE):
            try:
                self._d = json.load(open(CREDITS_FILE))
                self._auto_reset()
                return
            except:
                pass
        self._d = self._default()
        self._save()

    def _save(self):
        try:
            json.dump(self._d, open(CREDITS_FILE, "w"), indent=2)
        except:
            pass

    def _auto_reset(self):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        ch = False
        g = self._d.get("gmail", {})
        if g.get("reset") != today:
            self._d["gmail"] = {"lim": MAX_DAILY, "used": 0, "reset": today, "ok": True}
            ch = True
        for n in APIS:
            e = self._d.get(n, {})
            lr = e.get("reset", "")
            if lr:
                try:
                    ld = datetime.datetime.strptime(lr, "%Y-%m-%d")
                    now = datetime.datetime.now()
                    if now.month != ld.month or now.year != ld.year:
                        self._d[n] = {
                            "lim": APIS[n]["limit"],
                            "used": 0,
                            "reset": today,
                            "ok": True,
                        }
                        ch = True
                except:
                    pass
            if n not in self._d:
                self._d[n] = {
                    "lim": APIS[n]["limit"],
                    "used": 0,
                    "reset": today,
                    "ok": True,
                }
                ch = True
        if "gmail" not in self._d:
            self._d["gmail"] = {"lim": MAX_DAILY, "used": 0, "reset": today, "ok": True}
            ch = True
        if ch:
            self._save()

    def avail(self, p):
        e = self._d.get(p, {})
        return max(0, e.get("lim", 0) - e.get("used", 0)) if e.get("ok", True) else 0

    def use(self, p):
        e = self._d.setdefault(p, {"lim": 0, "used": 0, "reset": "", "ok": True})
        e["used"] = e.get("used", 0) + 1
        if e["used"] >= e.get("lim", 0):
            e["ok"] = False
        self._save()

    def gmail_left(self):
        return self.avail("gmail")

    def use_gmail(self):
        self.use("gmail")

    def report(self):
        lines = []
        for n, e in self._d.items():
            a = max(0, e.get("lim", 0) - e.get("used", 0))
            lines.append(
                f"  {n:12s} {a:>4d}/{e.get('lim',0)} {'OK' if e.get('ok') else 'EXHAUSTED'}"
            )
        return "\n".join(lines)

    def reset_all(self):
        self._d = self._default()
        self._save()


class NameParser:
    @staticmethod
    def parse(name):
        if not name or not name.strip():
            return None
        n = name.strip()
        if "," in n:
            parts = [p.strip() for p in n.split(",", 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                if parts[1].lower().rstrip(".") not in STRIP_SUF:
                    n = f"{parts[1]} {parts[0]}"
        ws = n.split()
        while ws and ws[0].lower().rstrip(".") in STRIP_PRE:
            ws = ws[1:]
        while ws and ws[-1].lower().rstrip(".") in STRIP_SUF:
            ws = ws[:-1]
        if not ws:
            return None
        first = ws[0]
        single = len(ws) == 1
        last = "" if single else (ws[1] if len(ws) == 2 else " ".join(ws[1:]))
        fa, la = _ascii(first), _ascii(last) if last else ""
        lc = re.sub(r"[^a-z]", "", la.lower()) if la else ""
        return {
            "full": name.strip(),
            "first": first,
            "last": last,
            "fa": fa,
            "la": la,
            "lc": lc,
            "fi": fa[0].lower() if fa else "",
            "li": la[0].lower() if la else "",
            "single": single,
            "hyph": "-" in first,
            "multi": " " in last if last else False,
        }

    @staticmethod
    def gen_phased(parsed, domains):
        if not parsed or not domains:
            return [], [], []
        if parsed["single"]:
            return [f"{parsed['fa'].lower()}@{d}" for d in domains], [], []
        f, la, fi, li = parsed["fa"].lower(), parsed["lc"], parsed["fi"], parsed["li"]

        def build(pats):
            s = set()
            for p in pats:
                lp = (
                    p.replace("{first}", f)
                    .replace("{last}", la)
                    .replace("{f}", fi)
                    .replace("{l}", li)
                )
                if lp and len(lp) >= 2:
                    s.add(lp)
            return s

        pa, pb, pc = build(PAT_A), build(PAT_B), build(PAT_C)
        ex = set()
        if parsed["hyph"]:
            nh, dh = f.replace("-", ""), f.replace("-", ".")
            bi = "".join(p[0] for p in f.split("-") if p)
            ex.update(
                [
                    f"{nh}.{la}",
                    f"{dh}.{la}",
                    f"{bi}{la}",
                    f"{bi}.{la}",
                    f"{f.split('-')[0]}.{la}",
                ]
            )
        if parsed["multi"]:
            pts = parsed["la"].lower().split()
            fin = re.sub(r"[^a-z]", "", pts[-1])
            part = {"van", "von", "de", "del", "di", "la", "le", "el", "al", "bin"}
            np_ = [re.sub(r"[^a-z]", "", p) for p in pts if p not in part]
            if fin:
                ex.update([f"{f}.{fin}", f"{fi}{fin}"])
            if np_:
                j = "".join(np_)
                ex.update([f"{f}.{j}", f"{fi}{j}"])
        if len(f) > 6 or len(la) > 8:
            ex.update([f"{f[:3]}.{la}", f"{f}.{la[:4]}", f"{fi}{la[:6]}"])
        pb.update({lp for lp in ex if lp and len(lp) >= 2})

        def emails(lps):
            return [f"{lp}@{d}" for d in domains for lp in lps]

        return emails(pa), emails(pb), emails(pc)


_SEED = {
    "google.com": "{first}.{last}",
    "meta.com": "{first}.{last}",
    "amazon.com": "{f}{last}",
    "apple.com": "{first}_{last}",
    "microsoft.com": "{first}.{last}",
    "netflix.com": "{first}.{last}",
    "salesforce.com": "{first}.{last}",
    "stripe.com": "{first}.{last}",
    "uber.com": "{first}.{last}",
    "airbnb.com": "{first}.{last}",
    "figma.com": "{first}.{last}",
    "snowflake.com": "{first}.{last}",
    "servicenow.com": "{first}.{last}",
    "intuit.com": "{first}.{last}",
    "oracle.com": "{first}.{last}",
    "adobe.com": "{first}.{last}",
    "ibm.com": "{first}.{last}",
    "nvidia.com": "{first}.{last}",
    "jpmorgan.com": "{f}{last}",
    "goldmansachs.com": "{first}.{last}",
    "deloitte.com": "{first}{last}",
    "mckinsey.com": "{f}.{last}",
    "bcg.com": "{f}.{last}",
    "tesla.com": "{first}.{last}",
    "openai.com": "{first}.{last}",
    "databricks.com": "{first}.{last}",
    "palantir.com": "{first}.{last}",
    "tiktok.com": "{first}.{last}",
    "t-mobile.com": "{first}.{last}",
    "verizon.com": "{first}.{last}",
    "coinbase.com": "{first}.{last}",
    "cloudflare.com": "{first}.{last}",
    "twilio.com": "{first}.{last}",
    "spacex.com": "{first}.{last}",
    "intel.com": "{first}.{last}",
    "amd.com": "{first}.{last}",
    "pwc.com": "{first}.{last}",
    "accenture.com": "{first}.{last}",
    "citi.com": "{first}.{last}",
}


class PatternCache:
    def __init__(self):
        self._d = dict(_SEED)
        if os.path.exists(PATTERNS_FILE):
            try:
                self._d.update(json.load(open(PATTERNS_FILE)))
            except:
                pass

    def _save(self):
        try:
            json.dump(self._d, open(PATTERNS_FILE, "w"), indent=2)
        except:
            pass

    def get(self, domain):
        return self._d.get(domain.lower())

    def store(self, domain, pat):
        self._d[domain.lower()] = pat
        self._save()

    def detect(self, email, parsed):
        if not email or "@" not in email or not parsed:
            return None
        local, dom = email.split("@")[0].lower(), email.split("@")[1].lower()
        f, la, fi, li = parsed["fa"].lower(), parsed["lc"], parsed["fi"], parsed["li"]
        for p in PAT_A + PAT_B + PAT_C:
            gen = (
                p.replace("{first}", f)
                .replace("{last}", la)
                .replace("{f}", fi)
                .replace("{l}", li)
            )
            if gen == local:
                self.store(dom, p)
                return p
        return None

    def gen_single(self, parsed, domain):
        p = self.get(domain)
        if not p or not parsed:
            return None
        f, la, fi, li = parsed["fa"].lower(), parsed["lc"], parsed["fi"], parsed["li"]
        lp = (
            p.replace("{first}", f)
            .replace("{last}", la)
            .replace("{f}", fi)
            .replace("{l}", li)
        )
        return f"{lp}@{domain}" if lp and len(lp) >= 2 else None


def _cl(idx):
    r = ""
    i = idx
    while i >= 0:
        r = chr(i % 26 + ord("A")) + r
        i = i // 26 - 1
    return r


def _pad(row):
    return (
        list(row) + [""] * (len(O_HEADERS) - len(row))
        if len(row) < len(O_HEADERS)
        else row
    )


def _ascii(text):
    try:
        n = unicodedata.normalize("NFKD", text)
        a = n.encode("ASCII", "ignore").decode("ASCII")
        return a if a else text
    except:
        return text
