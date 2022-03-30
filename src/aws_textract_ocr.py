# from boto3 import client
import datetime
import boto3
import io
from io import BytesIO
import sys
import os
import json
import time
import psutil
import time
import math
from typing import List, Dict

from PIL import Image, ImageDraw, ImageFont


session = boto3.Session(
    aws_access_key_id='AKIATALN65K7CDEA7UPA',
    aws_secret_access_key='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',
    region_name='eu-west-1'
)


def process_text_detection(bucket: str, s3_conn: session.resource('s3'), document: str):
    stats = {}
    nower = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')

    # Get the document from S3
    # s3_connection = session.resource('s3')
    # boto3.resource('s3')
    s3_object = s3_conn.Object(bucket, document)
    s3_response = s3_object.get()

    stream = io.BytesIO(s3_response['Body'].read())
    image = Image.open(stream)

    # Detect text in the document
    client = boto3.client('textract')
    # process using image bytes

    # process using S3 object
    response = client.detect_document_text(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}})

    dst_folder = '/'.join(document.split('/')[0:-2])
    dst_folder = f'{dst_folder}/aws_textract_ocr'
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
        if block['BlockType'] != 'PAGE':
            stats['msgs'].append('Detected: ' + block['Text'])
            stats['msgs'].append('Confidence: ' + "{:.2f}".format(block['Confidence']) + "%")
        stats['msgs'].append('Id: {}'.format(block['Id']))
        if 'Relationships' in block:
            stats['msgs'].append('Relationships: {}'.format(block['Relationships']))
        stats['msgs'].append('Bounding Box: {}'.format(block['Geometry']['BoundingBox']))
        stats['msgs'].append('Polygon: {}'.format(block['Geometry']['Polygon']))
        draw = ImageDraw.Draw(image)
        # Draw WORD - Green -  start of word, red - end of word
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

            # Draw box around entire LINE
        if block['BlockType'] == "LINE":
            points = []

            for polygon in block['Geometry']['Polygon']:
                points.append((width * polygon['X'], height * polygon['Y']))

            draw.polygon((points), outline='black')

            # Uncomment to draw bounding box
            box=block['Geometry']['BoundingBox']
            left = width * box['Left']
            top = height * box['Top']
            draw.rectangle([left,top, left + (width * box['Width']), top + (height * box['Height'])], outline='blue')

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
            -1].split('.')[0].split('-')[-1] in ['5', '6', '7', '8', '9',
                                                 '05', '06', '07', '08', '09', '10',
                                                 '005', '006', '007', '008', '009', '010']:
            pngs.append(obj.key)
    return pngs
'''
    # if obj.key.split('/')[-1].split('.')[-1] == 'png' and obj.key.split('/')[-1].split('.')[0].split('-')[
    #        -1] in ['1', '2', '3', '4', '5', '6', '7', '8', '9',
    #                '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    #                '001', '002', '003', '004','005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020']:
'''


if __name__ == '__main__':
    bucket = 'email-science-data'
    s3_connection = session.resource('s3')
    # fldr = 'axa-poc/Financial_Lines_att_images/'
    fldr = 'email_annotation/markel_manual_todo/attachments_att_images'
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
            res = process_text_detection(bucket, s3_connection, e)
            res['file'] = e
            res['exec_time_seconds'] = time.perf_counter() - s
        except Exception as ex:
            res = {}
            res['file'] = e
            res['exec_time_seconds'] = time.perf_counter() - s
            res['err'] = str(ex)

        rr.append(res)
        print(f'{index+1}/{total} processing file {e} cost: {time.perf_counter() - s} seconds')
    print(f'total processing costs: {time.perf_counter() - s0} seconds')


    with open(f'all_stats_{nower}.json', 'w') as outfile:
        json.dump(rr, outfile)


