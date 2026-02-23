#!/usr/bin/env python3
"""
Outreach Patches — Run from project root:
    python3 apply_outreach_fixes.py
    
Fixes:
1. Outreach ↔ Valid exact sync (preserving order, verbatim company names)
2. Bounce notes — simplified format
3. Bounce auto-retry with alternative patterns
4. Self-healing domain learning from bounces
"""

import ast, sys, os

# ============================================================
# PATCH 1: Rewrite pull() for exact sync + rewrite sync_with_valid()
# ============================================================

txt = open('outreach/outreach_data.py').read()

# Replace the entire pull() method
old_pull_start = '''    def pull(self):
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

        # Build Valid Entries position map for ordering new rows
        valid_position = {}
        for v_idx, row in enumerate(vdata[1:]):
            while len(row) <= max(V_COMPANY, V_TITLE, V_JOBID):
                row = list(row) + [""]
            co = row[V_COMPANY].strip().lower()
            ti = row[V_TITLE].strip().lower()
            jid = (row[V_JOBID].strip().lower() if len(row) > V_JOBID else "")
            jid_clean = jid if jid and jid != "n/a" else ""
            if jid_clean:
                valid_position[("jid", jid_clean)] = v_idx
            if co:
                valid_position[("co_ti", f"{co}||{ti}")] = v_idx

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
            # Get Valid Entries position for ordering
            v_pos = valid_position.get(
                ("jid", jid_clean) if jid_clean else ("co_ti", f"{co.lower()}||{ti.lower()}"),
                999999
            )
            nr = [""] * len(O_HEADERS)
            nr[C["company"]] = co
            nr[C["title"]] = ti
            nr[C["job_id"]] = jid
            new.append((v_pos, nr))
            if jid_clean:
                existing.add(("jid", jid_clean))
            existing.add(("co_ti", f"{co.lower()}||{ti.lower()}"))

        if new:
            # Sort by Valid Entries position to preserve order
            new.sort(key=lambda x: x[0])
            # Renumber sr sequentially starting after existing rows
            base_sr = len(odata)  # existing row count including header
            rows_to_write = []
            for i, (_, nr) in enumerate(new):
                nr[C["sr"]] = str(base_sr + i)
                rows_to_write.append(nr)
            start = len(odata) + 1
            end_row = start + len(rows_to_write) - 1
            end_col = _cl(len(O_HEADERS) - 1)
            self._retry(
                self.ws.update,
                values=rows_to_write,
                range_name=f"A{start}:{end_col}{end_row}",
                value_input_option="USER_ENTERED",
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
        return len(new)'''

new_pull = '''    def pull(self):
        """Full positional sync: mirror Valid sheet order, copy Company/Title/JobID verbatim."""
        try:
            valid = self.ss.worksheet(VALID_TAB)
        except gspread.exceptions.WorksheetNotFound:
            log.error(f"'{VALID_TAB}' not found")
            return 0

        vdata = valid.get_all_values()
        self._p()
        odata = self.ws.get_all_values()
        self._p()

        # Build outreach lookup: key → row data (preserve HM/Rec/emails/notes)
        outreach_by_key = {}
        for r in odata[1:]:
            r = _pad(r)
            co = r[C["company"]].strip().lower()
            ti = r[C["title"]].strip().lower()
            jid = r[C["job_id"]].strip().lower()
            # Key by job_id first, then company+title
            if jid and jid != "n/a":
                outreach_by_key[("jid", jid)] = list(r)
            if co:
                outreach_by_key[("co_ti", f"{co}||{ti}")] = list(r)

        # Build new outreach rows in Valid sheet order
        result_rows = []
        seen = set()
        for row in vdata[1:]:
            while len(row) <= max(V_COMPANY, V_TITLE, V_JOBID):
                row = list(row) + [""]
            co = row[V_COMPANY].strip()
            ti = row[V_TITLE].strip() if len(row) > V_TITLE else ""
            jid = row[V_JOBID].strip() if len(row) > V_JOBID else ""
            if not co:
                continue
            jid_clean = jid.lower() if jid and jid.lower() != "n/a" else ""

            # Dedup within this sync
            dedup_key = f"{co.lower()}||{ti.lower()}||{jid_clean}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            # Find existing outreach row
            existing = None
            if jid_clean:
                existing = outreach_by_key.get(("jid", jid_clean))
            if not existing:
                existing = outreach_by_key.get(("co_ti", f"{co.lower()}||{ti.lower()}"))

            if existing:
                # Preserve existing outreach data but update Company/Title/JobID verbatim from Valid
                nr = list(existing)
                while len(nr) < len(O_HEADERS):
                    nr.append("")
                nr[C["company"]] = co  # verbatim from Valid
                nr[C["title"]] = ti    # verbatim from Valid
                nr[C["job_id"]] = jid  # verbatim from Valid
            else:
                # New row — only copy shared columns
                nr = [""] * len(O_HEADERS)
                nr[C["company"]] = co
                nr[C["title"]] = ti
                nr[C["job_id"]] = jid

            result_rows.append(nr)

        # Assign sequential Sr. No.
        for i, nr in enumerate(result_rows):
            nr[C["sr"]] = str(i + 1)

        # Count changes
        old_count = len(odata) - 1 if len(odata) > 1 else 0
        new_count = len(result_rows)
        added = max(0, new_count - old_count)

        # Write entire outreach sheet (header + all rows) in one batch
        if result_rows:
            all_data = [O_HEADERS] + result_rows
            end_col = _cl(len(O_HEADERS) - 1)

            # Clear existing data below what we'll write (handles deletions)
            total_existing = len(odata)
            total_new = len(all_data)
            if total_existing > total_new:
                # Clear orphaned rows
                clear_start = total_new + 1
                clear_end = total_existing
                try:
                    clear_range = f"A{clear_start}:{end_col}{clear_end}"
                    self._retry(
                        self.ws.batch_clear, [clear_range]
                    )
                    self._p()
                except:
                    pass

            # Write all data
            self._retry(
                self.ws.update,
                values=all_data,
                range_name=f"A1:{end_col}{len(all_data)}",
                value_input_option="USER_ENTERED",
            )
            self._p()

            # Format data rows
            if new_count > 0:
                try:
                    self.ws.format(
                        f"A2:{end_col}{new_count + 1}",
                        {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"fontFamily": "Times New Roman", "fontSize": 13},
                        },
                    )
                except:
                    pass

            removed = max(0, old_count - new_count)
            if added > 0:
                print(f"  Sync: {added} new rows added from Valid Entries")
            if removed > 0:
                print(f"  Sync: {removed} orphaned rows removed")
            log.info(f"pull: {new_count} total rows ({added} new, {removed} removed)")

        return added'''

assert old_pull_start in txt, "ERROR: pull() method not found in outreach_data.py"
txt = txt.replace(old_pull_start, new_pull)
print("✓ pull() rewritten — exact positional sync with Valid sheet")

# Remove sync_with_valid since pull() now handles deletions
old_sync = '''    def sync_with_valid(self):
        """Delete outreach rows whose company+title no longer exist in Valid Entries."""
        try:
            valid = self.ss.worksheet(VALID_TAB)
            vdata = valid.get_all_values()
            self._p()
            valid_keys = set()
            for row in vdata[1:]:
                while len(row) <= max(V_COMPANY, V_TITLE):
                    row = list(row) + [""]
                co = row[V_COMPANY].strip().lower()
                ti = row[V_TITLE].strip().lower()
                if co:
                    valid_keys.add(f"{co}||{ti}")
            odata = self.ws.get_all_values()
            self._p()
            rows_to_delete = []
            for i, r in enumerate(odata[1:], start=2):
                r = _pad(r)
                co = r[C["company"]].strip().lower()
                ti = r[C["title"]].strip().lower()
                if co and f"{co}||{ti}" not in valid_keys:
                    rows_to_delete.append(i)
            # Delete bottom-up to preserve row indices
            for row_idx in sorted(rows_to_delete, reverse=True):
                self.ws.delete_rows(row_idx)
                self._p()
            if rows_to_delete:
                log.info(f"sync_with_valid: deleted {len(rows_to_delete)} orphaned outreach rows")
                print(f"  Sync: removed {len(rows_to_delete)} outreach rows no longer in Valid Entries")
        except Exception as e:
            log.error(f"sync_with_valid failed: {e}")'''

new_sync = '''    def sync_with_valid(self):
        """No-op: pull() now handles full sync including deletions."""
        pass'''

assert old_sync in txt, "ERROR: sync_with_valid() not found in outreach_data.py"
txt = txt.replace(old_sync, new_sync)
print("✓ sync_with_valid() replaced — pull() now handles everything")

# ============================================================
# PATCH 2: Simplified bounce notes + auto-retry
# ============================================================

old_bounce_note = '''    def write_bounce_note(self, row, ct, email, bounced_at):
        """Write bounce note to Notes column and clear the bad email cell."""
        try:
            # Write to Notes column
            notes_col = _cl(C["notes"])
            existing_note = self.ws.acell(f"{notes_col}{row}").value or ""
            self._p()
            bounce_entry = f"⚠️ Bounced ({ct.upper()}): {email} | {bounced_at}"
            if bounce_entry not in existing_note:
                new_note = f"{existing_note} | {bounce_entry}".strip(" |") if existing_note else bounce_entry
                self._retry(self.ws.update_acell, f"{notes_col}{row}", new_note)
                self._p()
            # Clear the bad email cell so finder retries
            email_col = C["hm_email"] if ct == "hm" else C["rec_email"]
            self._retry(self.ws.update_acell, f"{_cl(email_col)}{row}", "")
            self._p()
            log.info(f"Row {row}: bounce noted for {email}")
        except Exception as e:
            log.error(f"write_bounce_note row {row}: {e}")'''

new_bounce_note = '''    def write_bounce_note(self, row, ct, email, bounced_at):
        """Write simplified bounce note and clear the bad email cell."""
        try:
            notes_col = _cl(C["notes"])
            existing_note = self.ws.acell(f"{notes_col}{row}").value or ""
            self._p()

            # Check if both HM and Rec bounced on same date
            r = _pad(self.ws.row_values(row))
            self._p()
            other_ct = "rec" if ct == "hm" else "hm"
            other_email_col = C["rec_email"] if ct == "hm" else C["hm_email"]
            other_email = r[other_email_col].strip() if len(r) > other_email_col else ""

            # Build clean note
            if f"{other_ct.upper()} email bounced" in existing_note:
                # Other already bounced — update to combined
                new_note = f"HM and Rec emails bounced on {bounced_at}"
            else:
                label = "HM" if ct == "hm" else "Rec"
                new_note = f"{label} email bounced on {bounced_at}"

            # Preserve non-bounce notes
            old_parts = [p.strip() for p in existing_note.split("|") if p.strip()]
            non_bounce = [p for p in old_parts if "bounced" not in p.lower()]
            if non_bounce:
                new_note = " | ".join(non_bounce) + " | " + new_note

            self._retry(self.ws.update_acell, f"{notes_col}{row}", new_note)
            self._p()

            # Clear the bad email cell
            email_col = C["hm_email"] if ct == "hm" else C["rec_email"]
            self._retry(self.ws.update_acell, f"{_cl(email_col)}{row}", "")
            self._p()
            log.info(f"Row {row}: {ct} bounce noted for {email}")
        except Exception as e:
            log.error(f"write_bounce_note row {row}: {e}")'''

assert old_bounce_note in txt, "ERROR: write_bounce_note() not found"
txt = txt.replace(old_bounce_note, new_bounce_note)
print("✓ write_bounce_note() — simplified format")

# ============================================================
# PATCH 3: Enhanced flag_bounced_rows with auto-retry
# ============================================================

old_flag = '''    def flag_bounced_rows(self, bounced_emails: set):
        """Scan all Outreach rows, flag bounced emails in Notes, clear bad email cells."""
        if not bounced_emails:
            return 0
        bounced_lower = {e.lower().strip() for e in bounced_emails}
        try:
            data = self.ws.get_all_values()
            self._p()
            flagged = 0
            today = __import__('datetime').date.today().strftime("%b %d, %Y")
            for i, r in enumerate(data[1:], start=2):
                r = _pad(r)
                he = r[C["hm_email"]].strip()
                re_ = r[C["rec_email"]].strip()
                if he and he.lower().strip() in bounced_lower:
                    self.write_bounce_note(i, "hm", he, today)
                    flagged += 1
                if re_ and re_.lower().strip() in bounced_lower:
                    self.write_bounce_note(i, "rec", re_, today)
                    flagged += 1
            if flagged:
                log.info(f"flag_bounced_rows: flagged {flagged} bounced email(s)")
                print(f"  Bounce flags: {flagged} bad email(s) noted and cleared")
            return flagged
        except Exception as e:
            log.error(f"flag_bounced_rows failed: {e}")
            return 0'''

new_flag = '''    def flag_bounced_rows(self, bounced_emails: set):
        """Scan Outreach rows, flag bounces, auto-retry with alternative patterns."""
        if not bounced_emails:
            return 0
        bounced_lower = {e.lower().strip() for e in bounced_emails}
        try:
            data = self.ws.get_all_values()
            self._p()
            flagged = 0
            retried = 0
            today = __import__('datetime').date.today().strftime("%b %d, %Y")

            for i, r in enumerate(data[1:], start=2):
                r = _pad(r)
                he = r[C["hm_email"]].strip()
                re_ = r[C["rec_email"]].strip()
                co = r[C["company"]].strip()
                hn = r[C["hm_name"]].strip()
                rn = r[C["rec_name"]].strip()

                for ct, email, name in [("hm", he, hn), ("rec", re_, rn)]:
                    if not email or email.lower().strip() not in bounced_lower:
                        continue

                    self.write_bounce_note(i, ct, email, today)
                    flagged += 1

                    # Auto-retry: try alternative email pattern
                    if name and co and "@" in email:
                        new_email = self._retry_bounced_email(name, co, email)
                        if new_email and new_email.lower() != email.lower():
                            email_col = C["hm_email"] if ct == "hm" else C["rec_email"]
                            self._retry(self.ws.update_acell, f"{_cl(email_col)}{i}", new_email)
                            self._p()
                            # Update notes
                            notes_col = _cl(C["notes"])
                            existing = self.ws.acell(f"{notes_col}{i}").value or ""
                            self._p()
                            retry_note = f"Retried: {new_email}"
                            updated = f"{existing} | {retry_note}".strip(" |") if existing else retry_note
                            self._retry(self.ws.update_acell, f"{notes_col}{i}", updated)
                            self._p()
                            retried += 1
                            log.info(f"Row {i}: bounce retry {email} → {new_email}")

            if flagged:
                msg = f"  Bounce flags: {flagged} bad email(s) noted and cleared"
                if retried:
                    msg += f", {retried} retried with new pattern"
                log.info(msg)
                print(msg)
            return flagged
        except Exception as e:
            log.error(f"flag_bounced_rows failed: {e}")
            return 0

    def _retry_bounced_email(self, name, company, bounced_email):
        """Try alternative email patterns after a bounce. Returns new email or None."""
        try:
            from outreach.outreach_data import NameParser, PatternCache
            parsed = NameParser.parse(name)
            if not parsed or parsed["single"]:
                return None

            domain = bounced_email.split("@")[1] if "@" in bounced_email else ""
            if not domain:
                return None

            # Get the pattern that bounced
            pc = PatternCache()
            bounced_local = bounced_email.split("@")[0].lower()

            # Mark bounced pattern as failed for this domain
            failed_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                ".local", "failed_patterns.json"
            )
            failed = {}
            try:
                if os.path.exists(failed_file):
                    failed = json.load(open(failed_file))
            except:
                pass
            if domain not in failed:
                failed[domain] = []
            if bounced_local not in failed[domain]:
                failed[domain].append(bounced_local)
                try:
                    json.dump(failed, open(failed_file, "w"), indent=2)
                except:
                    pass

            # Generate alternatives, skipping the failed pattern
            from outreach.outreach_config import PAT_A, PAT_B
            f = parsed["fa"].lower()
            la = parsed["lc"]
            fi = parsed["fi"]
            li = parsed["li"]

            candidates = []
            for pat in PAT_A + PAT_B:
                local = (pat.replace("{first}", f).replace("{last}", la)
                           .replace("{f}", fi).replace("{l}", li))
                if local and local != bounced_local and local not in failed.get(domain, []):
                    candidates.append(f"{local}@{domain}")

            if not candidates:
                return None

            # Try to verify each candidate
            try:
                from outreach.outreach_provider import ProviderVerifier
                pv = ProviderVerifier()
                for candidate in candidates[:5]:  # Max 5 attempts
                    result = pv.verify_email(candidate, domain)
                    if result == "exists":
                        # Store the working pattern
                        new_local = candidate.split("@")[0]
                        new_pat = (pat for pat in PAT_A + PAT_B
                                   if pat.replace("{first}", f).replace("{last}", la)
                                       .replace("{f}", fi).replace("{l}", li) == new_local)
                        pat_str = next(new_pat, None)
                        if pat_str:
                            pc.store(domain, pat_str)
                        log.info(f"Bounce retry verified: {candidate}")
                        return candidate
            except Exception as e:
                log.debug(f"Bounce retry verification failed: {e}")

            # If no verification available, try statistical best guess
            # Skip the bounced pattern and return the next most common
            if candidates:
                log.info(f"Bounce retry (unverified): {candidates[0]}")
                return candidates[0]

        except Exception as e:
            log.debug(f"_retry_bounced_email failed: {e}")
        return None'''

assert old_flag in txt, "ERROR: flag_bounced_rows() not found"
txt = txt.replace(old_flag, new_flag)
print("✓ flag_bounced_rows() — auto-retry with alternative patterns")

open('outreach/outreach_data.py', 'w').write(txt)
print("✓ outreach_data.py — all patches applied")

# ============================================================
# PATCH 4: Update run_outreach.py — remove sync_with_valid call
#           (pull() now handles sync)
# ============================================================

txt = open('outreach/run_outreach.py').read()

# The sync_with_valid() call is now a no-op, but let's keep it clean
# Just verify the jud fix is in place
if "jud = sheets.get_job_url_domain" in txt and "if row[\"need_h\"]:" in txt:
    # Check jud is outside the if block
    lines = txt.split("\n")
    for i, line in enumerate(lines):
        if "jud = sheets.get_job_url_domain" in line:
            # Check the line above — should NOT be "if row["need_h"]:"
            if i > 0 and "if row[\"need_h\"]:" in lines[i-1]:
                print("WARNING: jud fix may not be applied correctly — check manually")
            else:
                print("✓ jud fix already applied correctly")
            break
else:
    print("⚠ Could not verify jud fix — check outreach/run_outreach.py manually")

# ============================================================
# VERIFY ALL FILES
# ============================================================

print("\n--- Syntax Verification ---")
for f in ['outreach/outreach_data.py', 'outreach/run_outreach.py', 'outreach/outreach_config.py',
          'outreach/outreach_finder.py', 'outreach/bounce_scanner.py']:
    try:
        ast.parse(open(f).read())
        print(f'✓ {f} — syntax OK')
    except SyntaxError as e:
        print(f'✗ {f} — SYNTAX ERROR: {e}')
        sys.exit(1)

print("\n=== ALL OUTREACH PATCHES APPLIED SUCCESSFULLY ===")