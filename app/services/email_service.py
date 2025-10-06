import os
import smtplib
from email.message import EmailMessage

HOST = os.getenv("ACS_SMTP_HOST", "smtp.azurecomm.net")
PORT = int(os.getenv("ACS_SMTP_PORT", "587"))
USER = os.getenv("ACS_SMTP_USERNAME")
PWD = os.getenv("ACS_SMTP_PASSWORD")
MAIL_FROM = os.getenv("MAIL_FROM")


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
