"""                 ~~~Get recent fed speeches~~~

This script finds recent speeches posted on the site of the Federal Reserve,
compares them to a list of speeches that have already been pulled (stored in
S3) and loads new speeches into an S3 bucket.

Dependencies:
- Python3
- Requests
- Boto3

"""
import requests
import boto3
import re


def create_s3_session(creds_path):
    """ Creates the session with S3."""

    creds = open(creds_path, 'r').readlines()
    creds = [f.strip() for f in creds]

    # Extract access and secret keys
    acc_key = creds[1][creds[1].find(' = ') + 3:]
    sec_key = creds[2][creds[2].find(' = ') + 3:]

    # Launch session
    session = boto3.Session(aws_access_key_id=acc_key,
                            aws_secret_access_key=sec_key)
    s3 = session.resource('s3')

    return s3


def get_recent_dls(s3, bucket, recent_files_txt):
    """Gets the list of speeches we've already downloaded.

    Args:
        s3 (s3 session)
        bucket (str): bucket name in s3
        recent_files_txt (str): file containing the list of speeches
    """

    # Find the list of recently downloaded files
    obj = s3.Object(bucket, recent_files_txt)
    body = obj.get()['Body'].read()
    downloaded = body.split(b'\n')

    # Clean the list
    downloaded = [str(i).replace("b'", "") for i in downloaded]
    downloaded = [j.replace("'", "") for j in downloaded]

    return obj, downloaded


def get_new_speeches(downloaded):
    """Gets a list of the speeches that need to be downloaded."""

    # Get the feed list from the fed site
    feed_request = requests.get(
        'https://www.federalreserve.gov/feeds/speeches.xml'
    )
    feed_text = feed_request.text
    # feed_content = feed_request.content

    # Find links by searching the text for open and closed <link> tags
    starts = [s.start() + 15 for s in re.finditer(
        '<link><!\[CDATA\[', feed_text)]
    ends = [e.start() for e in re.finditer(']]></link>', feed_text)]

    # Combine start and ends into a list of links
    links = []
    for i, item in enumerate(starts):
        links.append(feed_text[starts[i]:ends[i]])

    download = [l for l in links if l not in downloaded]

    return links, download


def download_new_speeches(s3, download, bucket, folder):
    """Get the speeches and put them into S3."""
    for i, item in enumerate(download):

        # Find the name from the URL
        idx = item.find('speech')
        name = item[idx + 7:]

        # Get the speech
        r = requests.get(item)
        data = r.content

        # Put it in S3
        s3_f = s3.Object(bucket, '{}/{}'.format(folder, name))
        s3_f.put(Body=data)


if __name__ == '__main__':
    s3 = create_s3_session('<PATH TO AWS CREDENTIALS>')
    obj, downloaded = get_recent_dls(s3,
                                     '<BUCKET NAME>',
                                     '<FILE WITH RECENT DOWNLOADS>')
    links, download = get_new_speeches(downloaded)
    download_new_speeches(s3, download, '<BUCKET NAME>', '<FOLDER NAME>')

    # Housekeeping: put new list of downloaded speeches in s3
    files_str = '\n'.join(links)
    obj.put(Body=files_str)
