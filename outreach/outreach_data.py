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
)

log = logging.getLogger(__name__)

# ============================================================================
# SHEETS
# ============================================================================


class Sheets:
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
            # Write headers
            self.ws.update(
                values=[O_HEADERS],
                range_name=f"A1:{_cl(len(O_HEADERS)-1)}1",
                value_input_option="RAW",
            )

            # Header style — #b2e5b2 light green
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
                        },  # #b2e5b2
                    },
                )
            except:
                pass

            # All data rows — center aligned, Times New Roman 13
            try:
                end = _cl(len(O_HEADERS) - 1)
                self.ws.format(
                    f"A2:{end}500",
                    {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                    },
                )
            except:
                pass

            # Column widths — wide enough for 100 chars
            try:
                col_widths = []
                for i in range(len(O_HEADERS)):
                    col_widths.append(
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": self.ws.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": i,
                                    "endIndex": i + 1,
                                },
                                "properties": {"pixelSize": 250},
                                "fields": "pixelSize",
                            }
                        }
                    )
                # Narrower for specific columns
                # F(5)=HM LinkedIn, H(7)=Rec LinkedIn → 150px (URLs, not wide)
                # K(10)=Subject, L(11)=Body → 150px
                for i, w in [(0, 80), (3, 120), (5, 150), (7, 150), (10, 150), (11, 150), (12, 90), (13, 120), (14, 200)]:
                    col_widths.append(
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": self.ws.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": i,
                                    "endIndex": i + 1,
                                },
                                "properties": {"pixelSize": w},
                                "fields": "pixelSize",
                            }
                        }
                    )
                # (Email Body width handled in per-column loop above)
                self.ss.batch_update({"requests": col_widths})
            except:
                pass

            # Clear ALL data validation first (removes stale dropdowns)
            try:
                self.ss.batch_update(
                    {
                        "requests": [
                            {
                                "setDataValidation": {
                                    "range": {
                                        "sheetId": self.ws.id,
                                        "startRowIndex": 1,
                                        "endRowIndex": 500,
                                        "startColumnIndex": 0,
                                        "endColumnIndex": len(O_HEADERS),
                                    },
                                    "rule": None,
                                }
                            },
                            {
                                "setDataValidation": {
                                    "range": {
                                        "sheetId": self.ws.id,
                                        "startRowIndex": 1,
                                        "endRowIndex": 500,
                                        "startColumnIndex": C["send"],
                                        "endColumnIndex": C["send"] + 1,
                                    },
                                    "rule": {
                                        "condition": {
                                            "type": "ONE_OF_LIST",
                                            "values": [
                                                {"userEnteredValue": "Yes"},
                                                {"userEnteredValue": "No"},
                                            ],
                                        },
                                        "showCustomUi": True,
                                        "strict": False,
                                    },
                                }
                            },
                        ]
                    }
                )
            except:
                pass

            # Freeze header
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

    # ---- Pull from Valid Entries ----

    def get_resume_type(self, company, title):
        try:
            valid = self.ss.worksheet("Valid Entries")
            data = valid.get_all_values()
            self._p()
            for row in data[1:]:
                if len(row) > 9 and row[2].strip().lower() == company.strip().lower() and row[3].strip().lower() == title.strip().lower():
                    r = row[9].strip()
                    return r if r in ("SDE", "ML") else "SDE"
            return "SDE"
        except:
            return "SDE"

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

        # Build set of (company+title) already in outreach — for dedup
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
            # Pad short rows
            while len(row) <= max(V_COMPANY, V_TITLE, V_JOBID):
                row = list(row) + [""]

            co = row[V_COMPANY].strip()
            ti = row[V_TITLE].strip() if len(row) > V_TITLE else ""
            jid = row[V_JOBID].strip() if len(row) > V_JOBID else ""

            if not co:
                continue

            # Dedup: skip if already pulled
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

            # Track for dedup within this batch
            if jid_clean:
                existing.add(("jid", jid_clean))
            existing.add(("co_ti", f"{co.lower()}||{ti.lower()}"))

        if new:
            start = len(odata) + 1
            end_row = start + len(new) - 1
            end_col = _cl(len(O_HEADERS) - 1)
            self.ws.update(
                values=new,
                range_name=f"A{start}:{end_col}{end_row}",
                value_input_option="RAW",
            )
            self._p()
        return len(new)

    # ---- Read rows needing discovery ----
    def rows_for_discovery(self):
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
            err = r[C["error"]].strip()

            # Split comma-separated names (process each separately)
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
                        "hn_extra": hn_list[1:] if len(hn_list) > 1 else [],
                        "rn_extra": rn_list[1:] if len(rn_list) > 1 else [],
                    }
                )
        return rows

    # ---- Read rows needing send ----
    def rows_for_send(self):
        data = self.ws.get_all_values()
        self._p()
        rows = []
        for i, r in enumerate(data[1:], start=2):
            r = _pad(r)
            if r[C["send"]].strip().lower() != "yes":
                continue
            sent = r[C["sent_dt"]].strip()
            he, re_ = r[C["hm_email"]].strip(), r[C["rec_email"]].strip()
            h_ok = bool(he) and not sent
            r_ok = bool(re_) and not sent
            if h_ok or r_ok:
                rows.append(
                    {
                        "row": i,
                        "co": r[C["company"]].strip(),
                        "title": r[C["title"]].strip(),
                        "hn": r[C["hm_name"]].strip(),
                        "he": he,
                        "rn": r[C["rec_name"]].strip(),
                        "re": re_,
                        "subj": r[C["hm_subject"]].strip(),
                        "h_ok": h_ok,
                        "r_ok": r_ok,
                    }
                )
        return rows

    # ---- Writes ----
    def write_email(self, row, ct, email, source):
        try:
            col = C["hm_email"] if ct == "hm" else C["rec_email"]
            self.ws.update_acell(f"{_cl(col)}{row}", email)
            log.info(f"Row {row} {ct}: {email} (via {source})")
            self._p()
        except Exception as e:
            log.error(f"write_email row {row}: {e}")

    def write_subject_body(self, row, hm_subj, hm_body, rec_subj="", rec_body=""):
        try:
            s = _cl(C["hm_subject"])
            self.ws.update(
                values=[[hm_subj, hm_body, rec_subj, rec_body]],
                range_name=f"{s}{row}:{_cl(C["rec_body"])}{row}",
                value_input_option="RAW",
            )
            self._p()
        except Exception as e:
            log.error(f"write_subject_body row {row}: {e}")

    def write_sent(self, row, timestamp=""):
        try:
            # Format: "15 February, 2026"
            if not timestamp:
                timestamp = datetime.datetime.now().strftime("%d %B, %Y")
            self.ws.update_acell(f"{_cl(C['sent_dt'])}{row}", timestamp)
            self._p()
        except Exception as e:
            log.error(f"write_sent row {row}: {e}")

    def write_error(self, row, msg):
        try:
            existing = self.ws.acell(f"{_cl(C['error'])}{row}").value or ""
            ts = datetime.datetime.now().strftime("%m/%d %H:%M")
            tagged = f"[{ts}] {msg}"
            # If both HM and REC failed, combine into one line
            if existing and "All methods exhausted" in existing and "All methods exhausted" in msg:
                combined = f"[{ts}] HM+REC: All methods exhausted"
                self.ws.update_acell(f"{_cl(C['error'])}{row}", combined[:500])
            elif existing and msg not in existing:
                self.ws.update_acell(f"{_cl(C['error'])}{row}", f"{existing}; {tagged}"[:500])
            elif not existing:
                self.ws.update_acell(f"{_cl(C['error'])}{row}", tagged[:500])
            self._p()
        except:
            pass

    def append_error(self, row, msg):
        try:
            existing = self.ws.acell(f"{_cl(C['error'])}{row}").value or ""
            ts = datetime.datetime.now().strftime("%m/%d %H:%M")
            # Deduplicate: don't repeat same message
            if msg in existing:
                return
            new = f"{existing}; [{ts}] {msg}" if existing else f"[{ts}] {msg}"
            self.ws.update_acell(f"{_cl(C['error'])}{row}", new[:500])
            self._p()
        except:
            pass

    def _p(self):
        time.sleep(SHEET_PAUSE)


# ============================================================================
# CREDITS
# ============================================================================


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


# ============================================================================
# NAME PARSER
# ============================================================================


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


# ============================================================================
# PATTERN CACHE
# ============================================================================

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


# ============================================================================
# HELPERS
# ============================================================================


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
