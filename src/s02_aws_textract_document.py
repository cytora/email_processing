
# Analyzes text in a document stored in an S3 bucket. Display polygon box around text and angled text
from typing import List, Dict
import boto3
import io
from io import BytesIO
import sys
import os
import datetime
import json
import math
import time
from PIL import Image, ImageDraw, ImageFont


session = boto3.Session(
    aws_access_key_id='AKIATALN65K7CDEA7UPA',
    aws_secret_access_key='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',
    region_name='eu-west-1'
)


def ShowBoundingBox(draw, box, width, height, boxColor):
    left = width * box['Left']
    top = height * box['Top']
    draw.rectangle([left ,top, left + (width * box['Width']), top +(height * box['Height'])], outline=boxColor)


def ShowSelectedElement(draw, box, width, height, boxColor):
    left = width * box['Left']
    top = height * box['Top']
    draw.rectangle([left, top, left + (width * box['Width']), top + (height * box['Height'])], fill=boxColor)


# Displays information about a block returned by text detection and text analysis
def DisplayBlockInformation(block):
    print('Id: {}'.format(block['Id']))
    if 'Text' in block:
        print('    Detected: ' + block['Text'])
    print('    Type: ' + block['BlockType'])

    if 'Confidence' in block:
        print('    Confidence: ' + "{:.2f}".format(block['Confidence']) + "%")

    if block['BlockType'] == 'CELL':
        print("    Cell information")
        print("        Column:" + str(block['ColumnIndex']))
        print("        Row:" + str(block['RowIndex']))
        print("        Column Span:" + str(block['ColumnSpan']))
        print("        RowSpan:" + str(block['ColumnSpan']))

    if 'Relationships' in block:
        print('    Relationships: {}'.format(block['Relationships']))
    print('    Geometry: ')
    print('        Bounding Box: {}'.format(block['Geometry']['BoundingBox']))
    print('        Polygon: {}'.format(block['Geometry']['Polygon']))

    if block['BlockType'] == "KEY_VALUE_SET":
        print('    Entity Type: ' + block['EntityTypes'][0])

    if block['BlockType'] == 'SELECTION_ELEMENT':
        print('    Selection element detected: ', end='')

        if block['SelectionStatus'] == 'SELECTED':
            print('Selected')
        else:
            print('Not selected')

    if 'Page' in block:
        print('Page: ' + block['Page'])
    print()


def process_text_analysis(bucket: str, s3_conn: session.resource('s3'), document: str):
    stats = {}
    nower = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')

    # Get the document from S3
    s3_object = s3_conn.Object(bucket, document)
    s3_response = s3_object.get()

    stream = io.BytesIO(s3_response['Body'].read())
    image = Image.open(stream)

    # Analyze the document
    client = boto3.client('textract')

    # process using S3 object
    response = client.analyze_document(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}},
        FeatureTypes=["TABLES", "FORMS"]
    )

    dst_folder = '/'.join(document.split('/')[0:-2])
    dst_folder = f'{dst_folder}/aws_textract_document'
    dst = f"{dst_folder}/{document.split('/')[-1].split('.')[0]}.json"

    result = s3_conn.meta.client.put_object(Body=json.dumps(response), Bucket=bucket, Key=dst)

    res = result.get('ResponseMetadata')
    if res.get('HTTPStatusCode') == 200:
        stats['upload_json_state'] = 200
        stats['upload_json_state_msg'] = 'File Uploaded Successfully'
    else:
        stats['upload_json_state'] = res.get('HTTPStatusCode')
        stats['upload_json_state_msg'] = 'File Uploaded Unsuccessfully'

    dest_tmp_file = os.path.join('results', f'data_{nower}')
    with open(f'{dest_tmp_file}.json', 'w') as outfile:
        json.dump(response, outfile)

    # Get the text blocks
    blocks = response['Blocks']
    width, height = image.size
    draw = ImageDraw.Draw(image)
    stats['msg'] = {}
    stats['msg']['init'] = 'Detected Document Text'

    # Create image showing bounding box/polygon the detected lines/text
    stats['msgs'] = []
    for block in blocks:
        stats['msgs'].append('Type: ' + block['BlockType'])
        # DisplayBlockInformation(block)
        stats['msgs'].append('Type: ' + block['BlockType'])
        if block['BlockType'] != 'PAGE':
            stats['msgs'].append('Detected: ' + block['BlockType'])
            stats['msgs'].append('Confidence: ' + "{:.2f}".format(block['Confidence']) + "%")
        stats['msgs'].append('Id: {}'.format(block['Id']))
        if 'Relationships' in block:
            stats['msgs'].append('Relationships: {}'.format(block['Relationships']))
        stats['msgs'].append('Bounding Box: {}'.format(block['Geometry']['BoundingBox']))
        stats['msgs'].append('Polygon: {}'.format(block['Geometry']['Polygon']))

        draw = ImageDraw.Draw(image)
        if block['BlockType'] == "KEY_VALUE_SET":
            if block['EntityTypes'][0] == "KEY":
                ShowBoundingBox(draw, block['Geometry']['BoundingBox'], width, height, 'red')
            else:
                ShowBoundingBox(draw, block['Geometry']['BoundingBox'], width, height, 'green')
        if block['BlockType'] == 'TABLE':
            ShowBoundingBox(draw, block['Geometry']['BoundingBox'], width, height, 'blue')
        if block['BlockType'] == 'CELL':
            ShowBoundingBox(draw, block['Geometry']['BoundingBox'], width, height, 'yellow')
        if block['BlockType'] == 'SELECTION_ELEMENT':
            if block['SelectionStatus'] == 'SELECTED':
                ShowSelectedElement(draw, block['Geometry']['BoundingBox'], width, height, 'blue')
        if block['BlockType'] == "WORD":
            draw.line([(width * block['Geometry']['Polygon'][0]['X'],
                        height * block['Geometry']['Polygon'][0]['Y']),
                       (width * block['Geometry']['Polygon'][3]['X'],
                        height * block['Geometry']['Polygon'][3]['Y'])], fill='green',
                      width=2)
            draw.line([(width * block['Geometry']['Polygon'][1]['X'],
                        height * block['Geometry']['Polygon'][1]['Y']),
                       (width * block['Geometry']['Polygon'][2]['X'],
                        height * block['Geometry']['Polygon'][2]['Y'])], fill='red',
                      width=2)

                # uncomment to draw polygon for all Blocks
        if block['BlockType'] == "LINE":
            points = []
            for polygon in block['Geometry']['Polygon']:
                points.append((width * polygon['X'], height * polygon['Y']))
            draw.polygon((points), outline='black')
            # Uncomment to draw bounding box
            box = block['Geometry']['BoundingBox']
            left = width * box['Left']
            top = height * box['Top']
            draw.rectangle([left, top, left + (width * box['Width']), top + (height * box['Height'])],
                           outline='blue')

        #points = []
        #for polygon in block['Geometry']['Polygon']:
        #    points.append((width * polygon['X'], height * polygon['Y']))
        #    draw.polygon((points), outline='blue')

    # Display the image
    dst = f"{dst_folder}/{document.split('/')[-1].split('.')[0]}.png"

    # Save the image to an in-memory file
    in_mem_file = io.BytesIO()
    image.save(in_mem_file, "PNG")
    in_mem_file.seek(0)

    image.save(f"{dest_tmp_file}.png", "PNG")
    result = s3_conn.meta.client.put_object(Body=in_mem_file, Bucket=bucket, Key=dst)
    # result = s3_conn.upload_fileobj(in_mem_file, bucket, dst)

    res = result.get('ResponseMetadata')
    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')

    stats['msg']['end'] = len(blocks)
    return stats


def get_png_images(bucket: str, s3_conn: session.resource('s3'), fldr: str) -> List:
    bucket = s3_conn.Bucket(bucket)
    pngs = []
    for obj in bucket.objects.filter(Prefix=fldr):
        if obj.key.split('/')[-1].split('.')[-1] == 'png' and obj.key.split('/')[
            -1].split('.')[0].split('-')[-1] in ['1', '2',
                                                '01', '02',
                                                '001', '002']:
            pngs.append(obj.key)
    pngs_srs = []
    for p in pngs:
        if 'page_thumbnails' in p:
            pngs_srs.append(p)
    return pngs_srs


def main():
    bucket = 'email-science-data'
    s3_connection = session.resource('s3')
    # fldr = 'axa-poc/Financial_Lines_att_images/'
    fldr = 'allianz_uk_unclassified/attachments_att_images/'
    # fldr = 'axa-poc/Fleet_email_submissions_att_images/'
    # fldr = 'axa-poc/EmailsforPOC_Cytora/PPPGroundTruthEmails_att_images'

    nower = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')

    arr = get_png_images(bucket, s3_connection, fldr)

    rr = []
    total = len(arr)
    s0 = time.perf_counter()
    for index, e in enumerate(arr):
        s = time.perf_counter()
        try:
            res = process_text_analysis(bucket, s3_connection, e)
            res['file'] = e
            res['exec_time_seconds'] = time.perf_counter() - s
        except Exception as ex:
            res = {}
            res['file'] = e
            res['exec_time_seconds'] = time.perf_counter() - s
            res['err'] = str(ex)
            print('error ------------------------------------')
            print(ex)
            print('error ------------------------------------')

        rr.append(res)
        print(f'{index+1}/{total} processing file {e} cost: {time.perf_counter() - s} seconds')
    print(f'total processing costs: {time.perf_counter() - s0} seconds')

    with open(f'all_stats_{nower}.json', 'w') as outfile:
        json.dump(rr, outfile)


if __name__ == "__main__":
    main()

