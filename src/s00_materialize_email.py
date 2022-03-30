
import mailbox
import bs4
import pandas as pd
import json
import base64
import math
import textdistance as td
import os
import copy
import re
import hashlib
from uuid import uuid4
pd.set_option('display.max_columns', None)
from pathlib import Path
from datetime import datetime
import numpy as np
import time
from eml_unpack import EMLExtractor

import structlog

logger = structlog.getLogger(__name__)


def fixEmailFormatting(text):
    text = text.replace("=\r\n", "")
    text = text.replace("\r\n", "")
    text = re.sub("\s+", " ", text)
    text = text.strip()
    return text


def dropSelectionArtifacts(subj_text):
    subj_parts = re.split("\d{1} of \d{1} ", subj_text)
    ret = subj_parts[len(subj_parts) - 1]
    return ret


def get_html_text(html):
    try:
        return bs4.BeautifulSoup(html, 'lxml').body.get_text(' ', strip=True)
    except AttributeError:  # message contents empty
        return None


class GmailMboxMessage():
    def __init__(self, email_data):
        if not isinstance(email_data, mailbox.mboxMessage):
            raise TypeError('Variable must be type mailbox.mboxMessage')
        self.email_data = email_data

    def parse_email(self, write_attachments=False):
        # email meta
        self.email_labels = self.email_data['X-Gmail-Labels']
        self.email_date = self.email_data['Date']
        self.email_from = self.email_data['From']
        self.email_to = self.email_data['To']

        # subject
        tsubject = self.email_data['Subject']
        tsubject = fixEmailFormatting(tsubject)
        tsubject = dropSelectionArtifacts(tsubject)
        self.email_subject = tsubject

        # contents
        self.email_contents = self.read_email_payload()

        # text
        self.text = self.get_raw_text()

        # TODO this is Liuben ID. need to be
        #hash_string = self.email_from + self.email_subject + self.text
        #this_hash = hashlib.md5(hash_string.encode()).hexdigest()
        #self.id = this_hash
        uid = self.email_data.get('Message-ID').strip().split('@')[0].replace('<', '')
        self.uid = uid

        # attachments
        self.attachments = self.get_attachments(write_attachments)

    def get_raw_text(self):
        text = [x[2] for x in self.email_contents if x[0] == "text/html"]
        if len(text) == 0:
            text = [x[2] for x in self.email_contents if x[0] == "text/plain"]

        text = text[0]
        text = fixEmailFormatting(text)
        return text

    def read_email_payload(self):
        email_payload = self.email_data.get_payload()
        if self.email_data.is_multipart():
            email_messages = list(self._get_email_messages(email_payload))
        else:
            email_messages = [email_payload]
        return [self._read_email_text(msg) for msg in email_messages]

    def read_email_from(self):
        email_payload = self.email_data.get_payload()
        if self.email_data.is_multipart():
            email_messages = list(self._get_email_messages(email_payload))
        else:
            email_messages = [email_payload]
        return [msg['From'] for msg in email_messages]

    def _get_email_messages(self, email_payload):
        for msg in email_payload:
            if isinstance(msg, (list, tuple)):
                for submsg in self._get_email_messages(msg):
                    yield submsg
            elif msg.is_multipart():
                for submsg in self._get_email_messages(msg.get_payload()):
                    yield submsg
            else:
                yield msg

    def _read_email_text(self, msg):
        content_type = 'NA' if isinstance(msg, str) else msg.get_content_type()
        encoding = 'NA' if isinstance(msg, str) else msg.get('Content-Transfer-Encoding', 'NA')
        if 'text/plain' in content_type and 'base64' not in encoding:
            msg_text = msg.get_payload()
        elif 'text/plain' in content_type and 'base64' in encoding:
            base64_message = msg.get_payload()
            base64_bytes = base64_message.encode('utf-8')
            message_bytes = base64.b64decode(base64_bytes)
            msg_text = message_bytes.decode('utf-8')
        elif 'text/html' in content_type and 'base64' not in encoding:
            msg_text = get_html_text(msg.get_payload())
        elif 'text/html' in content_type and 'base64' in encoding:
            base64_message = msg.get_payload()
            base64_bytes = base64_message.encode('utf-8')
            message_bytes = base64.b64decode(base64_bytes)
            raw_msg_text = message_bytes.decode('utf-8')
            msg_text = get_html_text(raw_msg_text)
        elif content_type == 'NA':
            msg_text = get_html_text(msg)
        else:
            msg_text = None
        return (content_type, encoding, msg_text)

    def get_attachments(self, write_attachments=False):
        filenames = []
        for part in self.email_data.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-disposition') is None:
                continue
            filename = part.get_filename()

            if bool(filename):
                filename = fixEmailFormatting(filename)
                filenames.append(filename)
                filepath = os.path.join(ATTACHMENT_DIR, self.uid, filename)
                payload = part.get_payload(decode=True)
                if payload is not None and write_attachments:
                    if not os.path.isdir(os.path.dirname(filepath)):
                        os.makedirs(os.path.dirname(filepath))
                    with open(filepath, 'wb')as f:
                        f.write(payload)

        return filenames

    def save_email_body(self):
        filepath = os.path.join(ATTACHMENT_DIR, self.uid, 'email_body.html')
        text = None
        for el in self.email_contents:
            if el[0] == 'text/plain':
                text = el[-1].replace('\n', '<br>')
        html = f'''<html>
<body>
<p><strong>Email Labels:</strong> {self.email_labels}</p>
<hr>
<header>
    <p><strong>From:</strong> {self.email_from}</p>
    <p><strong>To:</strong> {self.email_to}</p>
    <p><strong>Subject:</strong> {self.email_subject}</p>
    <p><strong>Date:</strong> {self.email_date}</p>
</header>
<hr>
<main>
<p>{text}</p>
</main>
</body>
</html>'''
        with open(filepath, 'w') as f:
            f.write(html)


def process_eml_file(base_file: str):

    mbox_obj = mailbox.email.message_from_file(open(base_file))
    emails = dict(mbox_obj.items())
    try:
        email_data = GmailMboxMessage(mbox_obj)
        email_data.parse_email(write_attachments=True)
    except Exception as ex:
        print(ex)


def process_mbox_file(base_file: str):
    mbox_obj = mailbox.mbox(base_file)

    num_entries = len(mbox_obj)
    print(num_entries)

    # emails_parsed = [None for x in range(num_entries)]
    emails_parsed = []
    for i in range(num_entries):
        try:
            msg = mbox_obj[i]
            email_data = GmailMboxMessage(msg)
            email_data.parse_email(write_attachments=True)
            # email_data.save_email_body()
            o = vars(email_data)
            del o['email_data']
            emails_parsed.append(o)
        except Exception as ex:
            print(ex)

    with open('json_data.json', 'w') as outfile:
        json.dump(emails_parsed, outfile)


def _process_mbox_file(base_file: str):
    # load mailbox data
    mbox_obj = mailbox.mbox(base_file)

    num_entries = len(mbox_obj)
    print(num_entries)

    # parse mailbox data
    emails_parsed = [None for x in range(num_entries)]
    for i in range(num_entries):
        try:
            msg = mbox_obj[i]
            fname = 'AS8PR08MB6055A823D0B781863ECACC3AD4639'
            extractor = EMLExtractor(msg, fname)# fn.split("/")[-1])

            attachments = extractor.documents
            if extractor.body_document:
                attachments.append(extractor.body_document)

            import os

            dirname = f'/Users/todorlubenov/cytora_data/bulk_datas/test/my_pdf_renders/{fname}'
            os.makedirs(dirname, exist_ok=True)
            for attachment in attachments:
                # if attachment.content_type == ''
                with open(f"{dirname}/{attachment.name}", "wb") as f:
                    f.write(attachment.content)
            '''
            print(f'{i+1} / {num_entries}')
            email_obj = mbox_obj[i]
            email_data = GmailMboxMessage(email_obj)
            email_data.parse_email(write_attachments=True)
            email_data.save_email_body()
            emails_parsed[i] = email_data
            '''
        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    # source_file = '/Users/todorlubenov/cytora_data/emails/PoC/Fleet_email_submissions/100. EXTERNAL FW Mr John Keenan ta Keenan Properties Q008718975.msg.eml'


    # ATTACHMENT_DIR = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/attachments/'
    # source_file = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/_Manual__TO_DO.mbox'


    ATTACHMENT_DIR = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_financial_done/attachments/'
    source_file = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_financial_done/Financial_DONE.mbox'

    # ATTACHMENT_DIR = '/Users/todorlubenov/cytora_data/bulk_datas/attachments/'
    # source_file = '/Users/todorlubenov/cytora_data/bulk_datas/_Manual__TO_DO.mbox'

    suff = Path(source_file).suffix.lower()
    if suff == '.eml':
        process_eml_file(source_file)

    if suff == '.msg':
        print('Convert to eml and call process_eml_file')

    if suff == '.mbox':
        process_mbox_file(source_file)
        print('open it and iter using process_eml_file')


