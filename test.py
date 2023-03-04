import os
import json
import re
from datetime import datetime

emptyHash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
signedHeadersGeneric = 'host;x-amz-content-sha256;x-amz-date;x-amz-security-token';
signedHeadersCustomOrigin = 'host;x-amz-cf-id;x-amz-content-sha256;x-amz-date;x-amz-security-token';
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')


def lambda_handler(event, context):
    request = event['Records'][0]['cf']['request'];

    originType = ''
    if hasattr(request['origin'], 's3'):
        originType = 's3'
    elif hasattr(request['origin'], 'custom'):
        originType = 'custom'
    else:
        raise Exception('Unexpected origin type. Expected \'s3\' or \'custom\'. Got: ' + json.dumps(request['origin']))

    sigv4Options = {
        'method': request['method'],
        'path': request['origin'][originType]['path'] + request['uri'],
        'credentials': {
            'accessKeyId': AWS_ACCESS_KEY_ID,
            'secretAccessKey': AWS_SECRET_ACCESS_KEY,
            'sessionToken': AWS_SESSION_TOKEN
        },
        'host': request['headers']['host'][0]['value'],
        'xAmzCfId': event['Records'][0]['cf']['config']['requestId'],
        'originType': originType
    }
    signature = signV4(sigv4Options)

    for header in signature:
        request['headers'][header.lower()] = [{
            'key': header,
            'value': str(signature[header])
        }]

    return request


def signV4(options: object):
    region = options['host'].split('.')[2]
    date = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    canonicalHeaders = ''
    signedHeaders = ''

    if options['originType'] == 's3':
        canonicalHeaders = '\n'.join([
            'host:' + options['host'],
            'x-amz-content-sha256:' + emptyHash,
            'x-amz-date:' + date,
            'x-amz-security-token:' + options['credentials']['sessionToken']
        ])
        signedHeaders = signedHeadersGeneric
    else:
        canonicalHeaders = '\n'.join([
            'host:' + options['host'],
            'x-amz-cf-id' + options['xAmzCfId'],
            'x-amz-content-sha256:' + emptyHash,
            'x-amz-date:' + date,
            'x-amz-security-token:' + options['credentials']['sessionToken']
        ])
        signedHeaders = signedHeadersCustomOrigin

    canonicalURI = encodeRfc3986()


def encodeRfc3986(urlEncodedStr: str):
    return re.sub('/[!\'()*]/g', '', urlEncodedStr)


if __name__ == '__main__':
    # 20220821T233523Z
    date = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    print(date)
