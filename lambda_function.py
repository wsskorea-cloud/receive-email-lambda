import json
import os

import requests
import datetime
import eml_parser
import boto3
import traceback
import logging
from ldap3 import ALL, Connection, Server


def getReceiptInSnsNotification(event) -> dict:
    return json.loads(event['Records'][0]['Sns']['Message'])['receipt']


def getRecipients(receipt) -> list:
    return receipt['recipients']


def getS3Object(receipt) -> (str, str):
    action = receipt['action']
    bucketName = action['bucketName']
    objectKey = action['objectKey']

    return bucketName, objectKey


def downloadS3Object(bucketName, objectKey) -> str:
    file_location = '/tmp/' + objectKey

    try:
        s3 = boto3.client('s3')
        s3.download_file(bucketName, objectKey, file_location)

    except Exception as e:
        logging.exception(e)
        traceback.print_exc()

    else:
        return file_location


def emailParser(location) -> dict:
    def json_serial(obj):
        if isinstance(obj, datetime.datetime):
            serial = obj.isoformat()

            return serial

    try:
        with open(location, 'rb') as file:
            raw_email = file.read()

        ep = eml_parser.EmlParser(include_raw_body=True, include_attachment_data=True)
        parsed_eml = ep.decode_email_bytes(raw_email)

        parsed_eml = json.dumps(parsed_eml, default=json_serial)
        parsed_eml = json.loads(parsed_eml)

    except Exception as e:
        logging.exception(e)
        traceback.print_exc()

    else:
        return parsed_eml


def connectLdap():
    try:
        host = os.getenv('LDAP_HOST')
        port = os.getenv('LDAP_PORT')
        dn = os.getenv('LDAP_DN')
        password = os.getenv('LDAP_PASSWORD')
        baseDC = os.getenv('LDAP_BASEDC')

        address = '{}:{}'.format(host, port)
        server = Server(address, get_info=ALL)
        conn = Connection(server, dn, password, auto_bind=True)

    except Exception as e:
        logging.exception(e)
        traceback.print_exc()

    else:
        return conn, baseDC


class CreateBounceMail:
    def __init__(self, bounceList, recipients, emailFile):
        self.bounceList = bounceList
        self.emailFile = emailFile
        self.recipients = recipients
        self.bodyDict = self.createBodyDict()

    def createBodyDict(self):
        bodyDict = {
            'html': '',
            'text': ''
        }

        for body in self.emailFile['body']:
            if body['content_type'] == 'text/plain':
                bodyDict['text'] = body['content']

            elif body['content_type'] == 'text/html':
                bodyDict['html'] = body['content']

        return bodyDict

    def createRecipientsList(self):
        toList = self.emailFile['header'].get('to', [])
        ccList = self.emailFile['header'].get('cc', [])

        recipientsSet = set(self.recipients)
        toSet = set(toList)
        ccSet = set(ccList)

        recipientsSet = recipientsSet - toSet
        recipientsSet = recipientsSet - ccSet

        bccList = list(recipientsSet)

        return toList, ccList, bccList

    def createUnknownUsers(self):
        resultBounceList = []

        for email in self.bounceList:
            resultBounceList.append('{}({})'.format(email.split('@')[0], email))

        return ', '.join(resultBounceList)

    def createOriginalEmailFrom(self):
        return self.emailFile['header']['from']

    def createOriginalEmailDatetime(self):
        return self.emailFile['header']['date']

    def createOriginalEmailRecipientsHtml(self):
        toList, ccList, bccList = self.createRecipientsList()

        return 'To: {}<br>Cc: {}<br>Bcc: {}'.format(', '.join(toList), ', '.join(ccList), ', '.join(bccList))

    def createOriginalEmailRecipientsText(self):
        toList, ccList, bccList = self.createRecipientsList()

        return 'To: {}\nCc:{}\nBcc:{}'.format(', '.join(toList), ', '.join(ccList), ', '.join(bccList))

    def createOriginalEmailSubject(self):
        return self.emailFile['header']['subject']

    def createOriginalEmailContentsHtml(self):
        if self.bodyDict['html']:
            return self.bodyDict['html']

        else:
            return self.bodyDict['text']

    def createOriginalEmailContentsText(self):
        return self.bodyDict['text']


def sendEmail(recipients, emailFile):
    conn, baseDc = connectLdap()
    bounceList = []

    try:
        for recipient in recipients:
            conn.search(baseDc, '(mail={})'.format(recipient))

            if len(conn.entries):  # Exist User
                # TODO: Send to Postfix using LMTP
                print(conn.entries)  # Send Email using LMTP

            else:  # Non-Exist User
                print(conn.entries)  # Send Bounce Email
                bounceList.append(recipient)

        if bounceList:
            bounce = CreateBounceMail(bounceList, recipients, emailFile)
            unknownUsers = bounce.createUnknownUsers()
            originalEmailFrom = bounce.createOriginalEmailFrom()
            originalEmailDatetime = bounce.createOriginalEmailDatetime()
            originalEmailRecipientsHtml = bounce.createOriginalEmailRecipientsHtml()
            originalEmailRecipientsText = bounce.createOriginalEmailRecipientsText()
            originalEmailSubject = bounce.createOriginalEmailSubject()
            originalEmailContentsHtml = bounce.createOriginalEmailContentsHtml()
            originalEmailContentsText = bounce.createOriginalEmailContentsText()

            templateData = {
                'unknown_users': unknownUsers,
                'original_email_from': originalEmailFrom,
                'original_email_datetime': originalEmailDatetime,
                'original_email_recipients_html': originalEmailRecipientsHtml,
                'original_email_recipients_text': originalEmailRecipientsText,
                'original_email_subject': originalEmailSubject,
                'original_email_contents_html': originalEmailContentsHtml,
                'original_email_contents_text': originalEmailContentsText
            }

            client = boto3.client('ses')
            client.send_templated_email(
                Source='no-reply@wsskorea.cloud',
                Destination={
                    'ToAddresses': [emailFile['header']['from']]
                },
                Template='Bounce',
                TemplateData=json.dumps(templateData)
            )

        else:  # All users existed
            pass

    except Exception as e:
        logging.exception(e)
        traceback.print_exc()

    finally:
        conn.unbind()


def lambda_handler(event, context):
    receipt = getReceiptInSnsNotification(event)
    recipients = getRecipients(receipt)
    bucketName, objectKey = getS3Object(receipt)
    emailFileLocation = downloadS3Object(bucketName, objectKey)
    emailFile = emailParser(emailFileLocation)

    sendEmail(recipients, emailFile)

    # SLACK_URL = 'https://hooks.slack.com/services/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'
    # payload = {
    #     # 'text': json.dumps({
    #     # 'bucket_name': bucketName,
    #     # 'object_key': objectKey,
    #     # 'email_file': emailFile
    #     # })
    #     'text': json.dumps(event)
    # }

    # response = requests.post(
    #     SLACK_URL,
    #     json=payload
    # )

    return {
        # 'statusCode': response.status_code,
        # 'body': response.text
        'statusCode': 200,
        'body': json.dumps({'result': 'OK'})
    }
