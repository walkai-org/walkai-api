import smtplib
from email.message import EmailMessage

from app.core.config import get_settings

settings = get_settings()

HOST = settings.acs_smtp_host
PORT = settings.acs_smtp_port
USER = settings.acs_smtp_username
PWD = settings.acs_smtp_password
MAIL_FROM = settings.mail_from


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
            <p>Follow this link to continue: <a href=\"{link}\">{link}</a></p>
            <p>If you did not expect this email, you can safely ignore it.</p>""",
        subtype="html",
    )
    with smtplib.SMTP(HOST, PORT, timeout=20) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(USER, PWD)
        smtp.send_message(msg)
