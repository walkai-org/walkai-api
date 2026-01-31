import mimetypes
import smtplib
from email.message import EmailMessage
from pathlib import Path

from app.core.config import get_settings

settings = get_settings()

HOST = settings.acs_smtp_host
PORT = settings.acs_smtp_port
USER = settings.acs_smtp_username
PWD = settings.acs_smtp_password
MAIL_FROM = settings.mail_from
_LOGO_PATH = Path(__file__).resolve().parents[1] / "assets" / "walkai_logo_final.png"
_LOGO_CID = "walkai-logo"


def _attach_logo(html_part: EmailMessage | None) -> None:
    if not _LOGO_PATH.exists():
        return
    if html_part is None:
        return
    data = _LOGO_PATH.read_bytes()
    mime_type, _ = mimetypes.guess_type(str(_LOGO_PATH))
    maintype, subtype = (mime_type or "image/png").split("/", 1)
    html_part.add_related(data, maintype=maintype, subtype=subtype, cid=_LOGO_CID)


def send_invitation_via_acs_smtp(to_email: str, link: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = "Invitation to walk:ai"
    msg["From"] = MAIL_FROM
    msg["To"] = to_email
    msg.set_content(
        f"Hi,\n\nYou have been invited to walk:ai. Accept your invitation here: {link}\n"
    )
    msg.add_alternative(
        f"""<p>Hi,</p>
            <p>You have been invited to <b>walk:ai</b>.</p>
            <p>Follow this link to continue: <a href="{link}">{link}</a></p>
            <p>If you did not expect this email, you can safely ignore it.</p>
            <p><img src="cid:{_LOGO_CID}" alt="walk:ai" style="max-width:160px;height:auto;" /></p>""",
        subtype="html",
    )
    _attach_logo(msg.get_body(preferencelist=("html",)))
    with smtplib.SMTP(HOST, PORT, timeout=20) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(USER, PWD)
        smtp.send_message(msg)


def send_password_reset_via_acs_smtp(to_email: str, link: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = "Reset your walk:ai password"
    msg["From"] = MAIL_FROM
    msg["To"] = to_email
    msg.set_content(
        "Hi,\n\nFollow this link to reset your walk:ai password: "
        f"{link}\n\nIf you did not request this, you can ignore this email.\n"
    )
    msg.add_alternative(
        f"""<p>Hi,</p>
            <p>Follow this link to reset your <b>walk:ai</b> password:</p>
            <p><a href="{link}">{link}</a></p>
            <p>If you did not request this, you can safely ignore this email.</p>
            <p><img src="cid:{_LOGO_CID}" alt="walk:ai" style="max-width:160px;height:auto;" /></p>""",
        subtype="html",
    )
    _attach_logo(msg.get_body(preferencelist=("html",)))
    with smtplib.SMTP(HOST, PORT, timeout=20) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(USER, PWD)
        smtp.send_message(msg)
