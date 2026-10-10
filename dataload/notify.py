"""Email through an SMTP relay (Brevo). A mail failure is logged and never stops the load."""
import os
import smtplib
import time
from email.message import EmailMessage
from pathlib import Path


class Notifier:
    def __init__(self, cfg, log):
        self.settings = cfg.email
        self.root = cfg.root
        self.log = log

    def send(self, subject, body, attach=None):
        s = self.settings
        subject = f"{s.get('subject_prefix', '[dataload]')} {subject}"
        outbox = s.get("outbox")
        if outbox:
            folder = Path(self.root / outbox)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / f"{time.time_ns()}.txt").write_text(f"Subject: {subject}\n\n{body}\n")
        if not s.get("enabled", False):
            return
        try:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = s["from"]
            msg["To"] = ", ".join(s["to"])
            msg.set_content(body)
            if attach and Path(attach).exists():
                msg.add_attachment(Path(attach).read_bytes(), maintype="text", subtype="html",
                                   filename=Path(attach).name)
            with smtplib.SMTP(s.get("host", "smtp-relay.brevo.com"), int(s.get("port", 587)), timeout=30) as smtp:
                smtp.starttls()
                smtp.login(os.environ["BREVO_SMTP_LOGIN"], os.environ["BREVO_SMTP_KEY"])
                smtp.send_message(msg)
        except Exception as e:  # noqa: BLE001 - any mail problem must not stop the load
            self.log(f"email not sent ({subject}): {e!r}")
