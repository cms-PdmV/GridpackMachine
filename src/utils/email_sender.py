"""
Module that handles all email notifications
"""
import smtplib
import logging
import json
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from src.utils.config import Config


class EmailSender:
    """
    Email Sender allows to send emails to users using CERN SMTP server
    """

    def __init__(self, credentials):
        self.logger = logging.getLogger()
        self.credentials = credentials
        self.smtp = None

    def __setup_smtp(self):
        """
        Read credentials and connect to SMTP file
        """
        if ":" not in self.credentials:
            with open(self.credentials) as json_file:
                credentials = json.load(json_file)
        else:
            credentials = {}
            credentials["username"] = self.credentials.split(":")[0]
            credentials["password"] = self.credentials.split(":")[1]

        self.logger.info("Credentials loaded successfully: %s", credentials["username"])
        self.smtp = smtplib.SMTP(host="smtp.cern.ch", port=587)
        # self.smtp.connect()
        self.smtp.ehlo()
        self.smtp.starttls()
        self.smtp.ehlo()
        self.smtp.login(credentials["username"], credentials["password"])

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
        ccs = ["PdmV Service Account <pdmvserv@cern.ch>"]
        # Create a fancy email message
        message = MIMEMultipart()
        if Config.get("dev"):
            message["Subject"] = "[Gridpack-DEV] %s" % (subject)
        else:
            message["Subject"] = "[Gridpack] %s" % (subject)

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
                    "Content-Disposition", 'attachment; filename="%s"' % (file_name)
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
