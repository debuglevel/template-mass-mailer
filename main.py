import email
import logging.config
from pprint import pprint
from typing import Optional, List, Dict
import csv
import pandas
from jinja2 import Template
import smtplib
import ssl
import logging
import uuid
from uuid import UUID
from datetime import datetime
from tinydb import TinyDB, Query
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_mail_entries(csv_filename) -> List[Dict]:
    logger.debug(f"Getting entries from {csv_filename}...")

    mail_entries = []

    logger.debug(f"Reading CSV {csv_filename}...")
    df = pandas.read_csv(csv_filename)
    for index, row in df.iterrows():
        logger.debug(f"Converting row {index} to dictionary...")
        row_dict = row.to_dict()
        mail_entries.append(row_dict)

    logger.debug(f"Got {len(mail_entries)} entries from {csv_filename}")
    return mail_entries


def get_mail_body(template, mail_entry) -> str:
    logger.debug(f"Getting mail body...")

    with open('template.jinja2') as file:
        template = Template(file.read())

    mail_body = template.render(x=mail_entry)

    return mail_body


def send_mail(mail_address, mail_body, smtp_configuration):
    logger.debug(f"Sending mail to {mail_address}...")

    msg = email.message.Message()
    msg['From'] = smtp_configuration["from"]
    msg['To'] = mail_address
    msg['Subject'] = smtp_configuration["subject"]
    msg.add_header('Content-Type', 'text')
    msg.set_payload(mail_body)

    context = ssl.create_default_context()
    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_configuration["server"], smtp_configuration["port"])
        server.ehlo()  # check connection
        # server.starttls(context=context)  # Secure the connection
        server.ehlo()  # check connection
        # server.login(smtp_configuration["username"], smtp_configuration["password"])

        # Send email here
        server.sendmail(smtp_configuration["from"], mail_address, msg.as_string())
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()


def is_mail_sent(mail_address) -> bool:
    logger.debug(f"Getting if mail is sent...")

    logger.debug(f"Opening TinyDB...")
    database = TinyDB('database.json')

    query = Query()

    x = database.get(query.email == mail_address)

    if x is None:
        return False
    else:
        return True


def set_mail_sent(mail_address):
    logger.debug(f"Setting mail as sent...")

    database = TinyDB('database.json')
    dictionary = {"email": mail_address, "status": "ok"}
    database.insert(dictionary)


def get_smtp_configuration():
    with open('smtp-config.json') as f:
        data = f.read()
        return json.loads(data)


def main():
    logger.debug(f"Running main...")

    csv_filename = "list.csv"
    mail_entries = get_mail_entries(csv_filename)

    template = "template.jinja2"

    smtp_configuration = get_smtp_configuration()

    for mail_entry in mail_entries:
        if not is_mail_sent(mail_entry['email']):
            mail_address = mail_entry['email']
            mail_body = get_mail_body(template, mail_entry)
            send_mail(mail_address, mail_body, smtp_configuration)
            set_mail_sent(mail_entry['email'])
        else:
            logger.debug(f"Not sending mail to {mail_entry['email']}, as already in database.")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
