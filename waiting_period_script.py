import os
import sys
import logging
import configparser
import paramiko
import zipfile
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path) # Load environment variables from .env file

class Send_Email:
    """ Class to send generic email notifications for each of the data exchanges."""
    def __init__(self):
        self.sender_email = os.getenv("SMTP_USER")
        self.receiver_email = os.getenv("SMTP_RECEIVER")
        self.smtp_server = os.getenv("SMTP_SERVER")
        self.output_filename = None
        self.remote_path = None
        self.error_message = None
        self.header = "Waiting Period"
        self.site = None

    def email(self, output_filename, remote_path):
        """Send success email notification."""
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = f"{self.header} Daily Extract"

        body = f"{self.header} successfully extracted from the SFTP Server."
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(self.smtp_server, 587) as server:
            server.starttls()
            server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())

    def email_err(self, output_filename, remote_path, error_message):
        """Send error email notification."""
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = f"{self.header} Daily Extract"

        body = f"{self.header} failed to extract to the SFTP Server.\nError Message: {self.error_message}"
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(self.smtp_server, 587) as server:
            server.starttls()
            server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())


class WaitingPeriodExtractor(Send_Email):
    def __init__(self, config_path, log_path):
        super().__init__()
        self.config_path = Path(config_path)
        self.log_path = log_path
        self.setup_logging()
        self.load_config()

    def setup_logging(self):
        logging.basicConfig(
            filename=self.log_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def log(self, message):
        print(message)
        logging.info(message)

    def load_config(self):
        if not self.config_path.exists():
            self.log(f"Config file not found at {self.config_path}")
            sys.exit(1)

        config = configparser.ConfigParser()
        config.read(self.config_path)

        self.hostname = config.get('sftp', 'host')
        self.port = config.getint('sftp', 'port')
        self.username = config.get('sftp', 'username')
        self.password = config.get('sftp', 'password')
        self.remote_folder = '/outbox'
        self.local_download_folder = os.path.join(self.config_path.parent, 'extracts')

    def connect_sftp(self):
        try:
            transport = paramiko.Transport((self.hostname, self.port))
            transport.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            self.log("Connected to SFTP successfully.")
            return sftp, transport
        except Exception as e:
            self.log(f"SFTP Connection failed: {e}")
            self.error_message = str(e)
            Send_Email.email_err(self, self.local_download_folder, self.remote_folder, self.error_message)
            sys.exit(1)

    def download_latest_zip(self, sftp):
        zip_files = [f for f in sftp.listdir(self.remote_folder) if f.endswith('.zip')]
        if not zip_files:
            self.log("No zip files found in remote /outbox folder.")
            sftp.close()
            sys.exit(0)

        latest_zip = max(zip_files, key=lambda x: sftp.stat(os.path.join(self.remote_folder, x)).st_mtime)
        self.log(f"Latest ZIP file detected: {latest_zip}")

        local_zip_path = os.path.join(self.local_download_folder, latest_zip)
        try:
            sftp.get(os.path.join(self.remote_folder, latest_zip), local_zip_path)
            self.log(f"Downloaded ZIP to {local_zip_path}")
        except Exception as e:
            self.log(f"Failed to download ZIP: {e}")
            self.error_message = str(e)
            Send_Email.email_err(self, self.local_download_folder, self.remote_folder, self.error_message)
            sftp.close()
            sys.exit(1)

        return local_zip_path

    def extract_zip(self, zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.local_download_folder)
            extracted_files = zip_ref.namelist()

        flatfiles = [f for f in extracted_files if f.lower().endswith('.txt')]
        if not flatfiles:
            self.log("No flat file (.txt) found inside the ZIP.")
            os.remove(zip_path)
            sys.exit(0)

        flatfile_name = flatfiles[0]
        flatfile_path = os.path.join(self.local_download_folder, flatfile_name)
        self.log(f"Flat file extracted: {flatfile_name}")
        return flatfile_path

    def parse_flatfile(self, flatfile_path):
        beneficiaries = []
        underwriting_rules = []

        with open(flatfile_path, 'r') as file:
            for line in file:
                if len(line) < 10:
                    continue
                record_type = line[9]
                if record_type == '2':
                    beneficiaries.append({
                        'sequenceNumber': line[0:9].strip(),
                        'memberNo': line[10:19].strip(),
                        'beneficiaryCode': line[19:21].strip(),
                        'firstName': line[21:45].strip(),
                        'surname': line[45:69].strip(),
                        'initials': line[69:73].strip(),
                        'dateOfBirth': line[73:81].strip(),
                        'identificationNumber': line[81:97].strip()
                    })
                elif record_type == '3':
                    underwriting_rules.append({
                        'sequenceNumber': line[0:9].strip(),
                        'memberNo': line[10:19].strip(),
                        'beneficiaryCode': line[19:21].strip(),
                        'underwritingRuleType': line[21:25].strip(),
                        'underwritingRuleCode': line[25:33].strip(),
                        'startDate': line[33:41].strip(),
                        'endDate': line[41:49].strip(),
                        'narrative': line[49:229].strip(),
                        'referenceNo': line[229:247].strip()
                    })

        return beneficiaries, underwriting_rules

    def save_to_excel(self, beneficiaries, underwriting_rules):
        output_excel = os.path.join(self.local_download_folder, 'Waiting_Period_Extract.xlsx')
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            if beneficiaries:
                pd.DataFrame(beneficiaries).to_excel(writer, sheet_name='beneficiaries_flatfile', index=False)
            if underwriting_rules:
                pd.DataFrame(underwriting_rules).to_excel(writer, sheet_name='underwriting_rules_flatfile', index=False)
        self.log(f"Extracted Type 2 and 3 datasets saved to {output_excel}")

    def cleanup(self, zip_path, flatfile_path):
        try:
            os.remove(zip_path)
            os.remove(flatfile_path)
            self.log("Temporary files cleaned up.")
        except Exception as e:
            self.log(f"Cleanup warning: {e}")
            self.error_message = str(e)
            Send_Email.email_err(self, self.local_download_folder, self.remote_folder, self.error_message)

    def run(self):
        sftp, transport = self.connect_sftp()
        zip_path = self.download_latest_zip(sftp)
        sftp.close()
        transport.close()

        flatfile_path = self.extract_zip(zip_path)
        beneficiaries, underwriting_rules = self.parse_flatfile(flatfile_path)
        self.save_to_excel(beneficiaries, underwriting_rules)
        self.cleanup(zip_path, flatfile_path)
        self.log("Process completed successfully.")
        Send_Email.email(self, self.local_download_folder, self.remote_folder)



# Run the extractor
extractor = WaitingPeriodExtractor(
    config_path=r"config.ini",
    log_path=r"waiting_period_extract.log"
)
extractor.run()
