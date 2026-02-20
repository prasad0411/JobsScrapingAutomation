#!/usr/bin/env python3
"""Bounce Scanner — reads Gmail for delivery failure notifications."""

import os
import re
import json
import base64
import logging
import datetime

log = logging.getLogger(__name__)

BOUNCED_EMAILS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".local",
    "bounced_emails.json",
)


class BounceScanner:

    @staticmethod
    def load_bounced() -> dict:
        """Load persisted bounce cache. Returns {email: {bounced_at, subject}}."""
        try:
            if os.path.exists(BOUNCED_EMAILS_FILE):
                return json.load(open(BOUNCED_EMAILS_FILE))
        except Exception:
            pass
        return {}

    @staticmethod
    def save_bounced(cache: dict):
        try:
            json.dump(cache, open(BOUNCED_EMAILS_FILE, "w"), indent=2)
        except Exception as e:
            log.error(f"Failed to save bounce cache: {e}")

    @staticmethod
    def scan(gmail_service, days_back: int = 14) -> set:
        """
        Scan Gmail for bounce notifications from the last `days_back` days.
        Returns set of bounced email addresses (lowercase).
        Also persists new bounces to .local/bounced_emails.json.
        """
        bounced = BounceScanner.load_bounced()
        newly_found = set()

        try:
            after_date = (
                datetime.datetime.now() - datetime.timedelta(days=days_back)
            ).strftime("%Y/%m/%d")

            query = (
                f"after:{after_date} "
                "(from:mailer-daemon OR "
                'subject:"Delivery Status Notification" OR '
                'subject:"Mail delivery failed" OR '
                'subject:"Undeliverable" OR '
                'subject:"Delivery Failure" OR '
                'subject:"failure notice")'
            )

            result = (
                gmail_service.users()
                .messages()
                .list(userId="me", q=query, maxResults=100)
                .execute()
            )

            messages = result.get("messages", [])
            if not messages:
                log.info("Bounce scanner: no bounce messages found")
                return set(bounced.keys())

            log.info(
                f"Bounce scanner: checking {len(messages)} potential bounce messages"
            )

            for msg_meta in messages:
                msg_id = msg_meta["id"]
                try:
                    msg = (
                        gmail_service.users()
                        .messages()
                        .get(userId="me", id=msg_id, format="full")
                        .execute()
                    )

                    subject = BounceScanner._get_header(msg, "Subject") or ""
                    failed_email = BounceScanner._extract_failed_email(msg)

                    if failed_email:
                        email_lower = failed_email.lower()
                        if email_lower not in bounced:
                            bounced[email_lower] = {
                                "bounced_at": datetime.datetime.now().isoformat(),
                                "subject": subject[:200],
                                "msg_id": msg_id,
                            }
                            newly_found.add(email_lower)
                            log.info(f"Bounce detected: {email_lower} | {subject[:60]}")

                except Exception as e:
                    log.debug(f"Failed to process bounce msg {msg_id}: {e}")
                    continue

        except Exception as e:
            log.error(f"Bounce scanner failed: {e}")
            return set(bounced.keys())

        if newly_found:
            BounceScanner.save_bounced(bounced)
            log.info(f"Bounce scanner: {len(newly_found)} new bounces recorded")
            print(
                f"  Bounce scanner: {len(newly_found)} new bounce(s) → .local/bounced_emails.json"
            )
        else:
            log.info("Bounce scanner: no new bounces")

        return set(bounced.keys())

    @staticmethod
    def _get_header(msg: dict, name: str) -> str:
        headers = msg.get("payload", {}).get("headers", [])
        for h in headers:
            if h.get("name", "").lower() == name.lower():
                return h.get("value", "")
        return ""

    @staticmethod
    def _extract_failed_email(msg: dict) -> str:
        """
        Extract the failed recipient email from a bounce message.
        Three methods in order of reliability:
          1. DSN MIME part: "Final-Recipient: rfc822; email@domain"  (RFC 3464)
          2. Natural language patterns (Outlook/Gmail human-readable body)
          3. Email address near bounce error codes
        """
        payload = msg.get("payload", {})
        all_text = BounceScanner._collect_text_parts(payload)

        for text in all_text:
            # Method 1: RFC 3464 DSN standard header — most reliable
            m = re.search(
                r"Final-Recipient\s*:\s*rfc822\s*;\s*([\w.+%-]+@[\w.-]+\.\w+)",
                text,
                re.I,
            )
            if m:
                return m.group(1).strip()

            # Method 2: Outlook/Gmail natural language
            m = re.search(
                r"(?:your message to|message to)\s+<?([\w.+%-]+@[\w.-]+\.\w+)>?",
                text,
                re.I,
            )
            if m:
                return m.group(1).strip()

            # Method 3: Email address on its own line near SMTP error codes
            lines = text.split("\n")
            for i, line in enumerate(lines):
                line = line.strip()
                if re.match(r"^[\w.+%-]+@[\w.-]+\.\w+$", line):
                    context = " ".join(lines[max(0, i - 3) : i + 3]).lower()
                    if any(
                        kw in context
                        for kw in [
                            "550",
                            "5.1",
                            "not exist",
                            "no such user",
                            "unknown user",
                            "invalid",
                            "rejected",
                            "failed",
                            "does not exist",
                            "address not found",
                        ]
                    ):
                        return line

        # Method 4: Any email near bounce keywords in full body
        full_text = " ".join(all_text)
        full_lower = full_text.lower()
        bounce_keywords = [
            "couldn't be delivered",
            "could not be delivered",
            "delivery failed",
            "delivery failure",
            "not delivered",
            "undeliverable",
            "no such user",
            "user unknown",
            "address not found",
            "does not exist",
        ]
        for kw in bounce_keywords:
            idx = full_lower.find(kw)
            if idx >= 0:
                window = full_text[max(0, idx - 200) : idx + 200]
                emails = re.findall(r"[\w.+%-]+@[\w.-]+\.\w+", window)
                for email in emails:
                    el = email.lower()
                    # Skip bounce infrastructure addresses
                    if not any(
                        skip in el
                        for skip in [
                            "mailer-daemon",
                            "postmaster",
                            "noreply",
                            "no-reply",
                            "googlemail.com",
                            "google.com",
                            "microsoft.com",
                            "amazonses.com",
                            "bounce",
                            "donotreply",
                        ]
                    ):
                        return email

        return ""

    @staticmethod
    def _collect_text_parts(payload: dict) -> list:
        """Recursively collect all decoded text from a Gmail message payload."""
        texts = []
        body = payload.get("body", {})
        data = body.get("data", "")
        if data:
            try:
                decoded = base64.urlsafe_b64decode(data + "==").decode(
                    "utf-8", errors="replace"
                )
                texts.append(decoded)
            except Exception:
                pass
        for part in payload.get("parts", []):
            texts.extend(BounceScanner._collect_text_parts(part))
        return texts
