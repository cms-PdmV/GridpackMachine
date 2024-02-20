"""
Module that handles all email notifications
"""

import smtplib
import logging
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText


class EmailSender:
    """
    Email Sender allows to send emails to users using CERN SMTP server
    """

    def __init__(self, username, password, email_auth, production):
        self.logger = logging.getLogger()
        self.username = username
        self.password = password
        self.email_auth = email_auth
        self.production = production
        self.smtp = None

    def __setup_smtp(self):
        """
        Read credentials and connect to SMTP file
        """
        self.smtp = smtplib.SMTP(host="cernmx.cern.ch", port=25)
        self.smtp.ehlo()
        self.smtp.starttls()
        self.smtp.ehlo()
        if self.email_auth:
            self.smtp.login(self.username, self.password)

    def __close_smtp(self):
        """
        Close connection to SMTP server
        """
        self.smtp.quit()
        self.smtp = None

    def send(self, subject, body, recipients, files=None):
        """
        Send email
        """
        body = body.strip()
        body += "\n\nSincerely,\nGridpack Extravaganza Machine"
        ccs = [
            "PdmV Service Account <pdmvserv@cern.ch>",
            "CMS Automatic Background Production <ppd-auto-bkg@cern.ch>",
        ]
        # Create a fancy email message
        message = MIMEMultipart()
        if not self.production:
            message["Subject"] = f"[Gridpack-DEV] {subject}"
        else:
            message["Subject"] = f"[Gridpack] {subject}"

        message["From"] = "PdmV Service Account <pdmvserv@cern.ch>"
        message["To"] = ", ".join(recipients)
        message["Cc"] = ", ".join(ccs)
        # Set body text
        message.attach(MIMEText(body))
        if files:
            for path in files:
                attachment = MIMEBase("application", "octet-stream")
                with open(path, "rb") as attachment_file:
                    attachment.set_payload(attachment_file.read())

                file_name = path.split("/")[-1]
                encoders.encode_base64(attachment)
                attachment.add_header(
                    "Content-Disposition", f'attachment; filename="{file_name}"'
                )
                message.attach(attachment)

        self.logger.info('Will send "%s" to %s', message["Subject"], message["To"])
        self.__setup_smtp()
        try:
            self.smtp.sendmail(message["From"], recipients + ccs, message.as_string())
        except Exception as ex:
            self.logger.error(ex)
        finally:
            self.__close_smtp()
