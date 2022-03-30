
# !pip install pdf2image
# !pip install --upgrade pip
# !pip install joblib

from typing import Dict, List

import os
import sys
import time
import json
import pickle
import pathlib
import shutil
import datetime
from datetime import datetime

import numpy as np
import pandas as pd

from skimage.exposure import histogram
from skimage import io
from skimage.measure import shannon_entropy

import pytesseract
from PIL import Image

from pdf2image import convert_from_path, convert_from_bytes

from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)

# sys.path.insert(0, '../')
# from src.doc2pdf import get_office_cli_path, convert


__version__ = '0.2.0'

import platform, subprocess, tempfile, os, shutil


def get_platform():
    """Returna a string identifying the current platform.

    Returns:
        str: A string identifying the current platform.
    """
    return platform.system()


def is_windows():
    """Returns true if the current platform is Windows.

    Returns:
        bool: A boolean indicating if the current platform is Windows.
    """
    return get_platform().upper() == 'Windows'.upper()


def is_linux():
    """Returns true if the current platform is Linux.

    Returns:
        bool: A boolean indicating if the current platform is Linux.
    """
    return get_platform().upper() == 'Linux'.upper()


def is_osx():
    """Returns true if the current platform is OSX (MacOS).

    Returns:
        bool: A boolean indicating if the current platform is OSX (MacOS).
    """
    return get_platform().upper() == 'Darwin'.upper()


def get_office_cli_path():
    """Returns the path to the LibreOffice command line interface.

    Returns:
        str: The path to the command line interface.
    """
    if is_windows():
        if not os.path.exists('C:\Program Files\LibreOffice\program\soffice.exe'):
            raise Exception(f'Could not find LibreOffice. Is it installed?')
        return 'C:\Program Files\LibreOffice\program\soffice'
    elif is_linux():
        if not os.path.exists('/usr/bin/soffice'):
            raise Exception(f'Could not find LibreOffice. Is it installed?')
        return '/usr/bin/soffice'
    elif is_osx():
        if not os.path.exists('/Applications/LibreOffice.app/Contents/MacOS/soffice'):
            raise Exception(f'Could not find LibreOffice.app. Is it installed?')
        return '/Applications/LibreOffice.app/Contents/MacOS/soffice'
    else:
        raise Exception('Unsupported platform')


def convert(in_file, out_file=None):
    """Converts a file to PDF.

    Args:
        in_file (str): The path to the input file.
        out_file (str): The path to the output file, same path to in_file if not passes.
    """

    if not out_file:
        outdir = os.path.dirname(in_file)
    else:
        outdir = tempfile.gettempdir()

    command_line = get_office_cli_path()
    subprocess.call([
        command_line,
        '--headless',
        '--convert-to',
        'pdf',
        '--outdir',
        outdir,
        in_file
    ])

    if out_file:
        out_temp_file_arr = os.path.splitext(in_file)
        out_temp_file_name = f'{os.path.basename(out_temp_file_arr[0])}.pdf'
        out_file = f'{os.path.splitext(out_file)[0]}.pdf'
        shutil.move(os.path.join(outdir, out_temp_file_name), out_file)


def filename_normalizer(fname: str, suffix: str):
    tmp = f'{fname}{suffix}'.lower()
    chars = "!@#$%^&*()[]{};:,./<>?\|`~-=_+'\"_"
    tmp = tmp.translate({ord(c): "_" for c in chars})
    tmp = tmp.replace(' ', '_')

    return tmp


def get_suff_files_list_of_objects(base_uri: str, suffix: str) -> List:
    exts = {}
    suff_files = []

    for root, folder, files in os.walk(base_uri):
        for file in files:
            ext = pathlib.Path(file).suffix
            if exts.get(ext, None):
                exts[ext] += 1
            else:
                exts[ext] = 1
            if pathlib.Path(file).suffix.lower() == suffix:
                obj = {
                    'file_name': pathlib.Path(file).stem,
                    'extension': suffix,
                    'root_path': root.replace(base_uri, ''),
                    'abs_path': os.path.join(root, file),
                    'st_size_bytes': pathlib.Path(os.path.join(root, file)).stat().st_size,
                }
                suff_files.append(obj)
    return suff_files


def create_form_folder(dest_folder: str, form_object: Dict):
    s = os.getcwd()
    print(s)
    # sub = pathlib.PurePath(form_object['root_path']).name
    sub = form_object['root_path']

    sub_folder = f'{dest_folder}/{sub}'
    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)

    dpath = f"{form_object['file_name_normalized']}"
    if form_object.get('abs_path', None):
        if not os.path.exists(f'{sub_folder}/{dpath}'):
            os.makedirs(f'{sub_folder}/{dpath}')
        os.chdir(f'{sub_folder}/{dpath}')

        if not os.path.exists('source_form'):
            os.makedirs('source_form')
        # copy source file to dest folder in dedicated subfolder "source_form"

        # create folder for store converted images
        if not os.path.exists('page_images'):
            os.makedirs('page_images')

        # create folder for thumbnails
        if not os.path.exists('page_thumbnails'):
            os.makedirs('page_thumbnails')

        # create folder for experiments
        if not os.path.exists('experiments'):
            os.makedirs('experiments')

        # create folder for features
        if not os.path.exists('features'):
            os.makedirs('features')

        # make a copy of the source form to destination form folder
        src = form_object['abs_path']
        dst = f"{sub_folder}/{dpath}/source_form/{form_object['file_name_normalized']}{form_object['extension']}"
        shutil.copy(src, dst)
    os.chdir(s)
    return dpath


def generate_images_from_pdf(srs_pdf_path: str, dest_folder: str, props={}):
    convert_from_path(srs_pdf_path,
                      dpi=props.get('DPI', 250),
                      output_folder=dest_folder,
                      grayscale=True,
                      output_file=props.get('fname', 'cytora_init'),
                      fmt="png"
                      )
    form_meta = {
        'num_pages': 0,
        'pages_extent': {},
        'pages_hist': {},
        'pages_': {}
    }
    return form_meta


def get_thumbnails(arr: List, uri: str):
    for el in arr:
        p = f"{uri}{el['root_path']}/{el['dpath']}/page_thumbnails"
        files = list(next(os.walk(p))[2])
        el['img_files_thumbnails'] = files


def get_min_max(arr: List):
    mina, maxa = 10, 0
    sz = 0
    for e in arr:
        if e['imagery']['num_pages'] > maxa:
            maxa = e['imagery']['num_pages']
        if e['imagery']['num_pages'] < mina:
            mina = e['imagery']['num_pages']
        if e['st_size_bytes'] > sz:
            sz = e['st_size_bytes']
    return mina, maxa, sz


def get_oner_page(arr: List):
    for e in arr:
        if e['imagery']['num_pages'] == 1:
            print(e)


def get_img_df(arr: List, uri: str) -> pd.DataFrame:
    res = []
    for e in arr:
        obj = {
            'form_path': f"{uri}{e['dpath']}/source_form/{e['file_name_normalized']}",
            'form_size_kb': e['st_size_bytes'] / 1024,
            'form_num_pages': e['imagery']['num_pages'],
            'page_height': e['first_page_height'],
            'page_width': e['first_page_width'],
            'page_layers': e['first_page_layers'],
            'img_shannon_2': e['shannon_entropy_2'],
            'img_mean': e['img_mean'],
            'img_median': e['img_median'],
            'img_std': e['img_std'],
            'img_variance': e['img_variance']
        }
        res.append(obj)
    res_df = pd.DataFrame(res)
    return res_df


def get_df(arr: List):
    width_arr = []
    height_arr = []
    shannon_arr = []
    mean_arr = []
    median_arr = []
    std_arr = []
    var_arr = []

    for e in arr:
        width_arr.append(e['first_page_width'])
        height_arr.append(e['first_page_height'])
        shannon_arr.append(e['shannon_entropy_2'])
        mean_arr.append(e['img_mean'])
        median_arr.append(e['img_median'])
        std_arr.append(e['img_std'])
        var_arr.append(e['img_variance'])
    d = pd.DataFrame({'img_mean': mean_arr,
                      'img_median': median_arr,
                      'img_shannon_2': shannon_arr,
                      'img_std': std_arr,
                      'img_var': var_arr,
                      'img_width': width_arr,
                      'img_height': height_arr}
                     )
    return d


def process_email(el, base_destination, suffix):
    el['file_name_normalized'] = filename_normalizer(el['file_name'], suffix)
    el['dpath'] = create_form_folder(base_destination, el)

    if suffix in ('.doc', '.docx', '.odt'):
        # s = time.perf_counter()
        # srs = pathlib.PurePath(el['root_path']).name
        srs = el['root_path']
        srs_file = f"{base_destination}/{srs}/{el['dpath']}/source_form/{el['dpath']}{suffix}"
        dst_file = f"{base_destination}/{srs}/{el['dpath']}/source_form/{el['dpath']}.pdf"
        convert(srs_file, dst_file)
        # print(f'processing time is {time.perf_counter() - s} seconds')

    try:
        sub = el['root_path']
        el['imagery'] = generate_images_from_pdf(
            f"{base_destination}/{sub}/{el['dpath']}/source_form/{el['file_name_normalized']}.pdf",
            f"{base_destination}/{sub}/{el['dpath']}/page_thumbnails", {'DPI': 300})
    except Exception as ex:
        el['error'] = str(ex)

    # get thumbnails
    p = f"{base_destination}{el['root_path']}/{el['dpath']}/page_thumbnails"
    files = list(next(os.walk(p))[2])
    el['img_files_thumbnails'] = files

    # get num pages
    try:
        el['imagery']['num_pages'] = len(el['img_files_thumbnails'])
    except Exception as ex:
        print(ex)
        print(el)

    # sort thumbnails
    try:
        el['img_files_thumbnails'].sort()
    except Exception as ex:
        print(ex)
        print(el)

    # augment object
    try:
        f_page = el['img_files_thumbnails'][0]
        p = f"{base_destination}{el['root_path']}/{el['dpath']}/page_thumbnails/{f_page}"
        first_page_image = io.imread(p)
        el['first_page_height'], el['first_page_width'], el['first_page_layers'] = first_page_image.shape
        #el['first_page_hist'], el['first_page_hist_centers'] = histogram(first_page_image)
        el['shannon_entropy_2'] = shannon_entropy(first_page_image, base=2)
        el['img_mean'] = np.mean(first_page_image)
        el['img_median'] = np.median(first_page_image)
        el['img_std'] = np.std(first_page_image)
        el['img_variance'] = np.var(first_page_image)
        el['img_average'] = np.average(first_page_image)
    except Exception as ex:
        print(ex)
        print(el)


def processing(base_destination: str, base_uri: str, suffix: str):
    doc_files = get_suff_files_list_of_objects(base_uri, suffix)
    print(len(doc_files))
    ss = time.perf_counter()

    for el in doc_files:
        s = time.perf_counter()
        try:

            process_email(el, base_destination, suffix)
            meta_file = f"{base_destination}{el['root_path']}/{el['file_name_normalized']}_meta.json"
            with open(meta_file, 'w') as outfile:
                json.dump(el, outfile)
        except Exception as ex:
            print(ex)
        print(f"file {el['root_path']}-{el['file_name_normalized']} has been processed for ~ {time.perf_counter() - s} seconds")
    print(f"overall execution of the {suffix} costs ~ {time.perf_counter() - ss} seconds")


if __name__ == '__main__':
    # ATTACHMENT_DIR = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/attachments/'
    # source_file = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/_Manual__TO_DO.mbox'

    # base_destination = '/Users/todorlubenov/cytora_data/emails/PoC/connect_att_images/'

    # Where customer PDF files are stored
    # base_uri = '/Users/todorlubenov/cytora_data/emails/PoC/connect/'
    # base_destination = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/attachments_att_images/'
    # base_uri = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_todo/attachments/'

    # base_destination = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_financial_done/attachments_att_images/'
    # base_uri = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_financial_done/attachments/'

    base_destination = '/Users/todorlubenov/cytora_data/bulk_emails_processing/allianz_uk_unclassified/attachments_att_images/'
    base_uri = '/Users/todorlubenov/cytora_data/bulk_emails_processing/allianz_uk_unclassified/attachments/'
    # suffix = '.docx'
    # suffix = '.doc'
    # suffix = '.pdf'

    # base_destination = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_done/attachments_att_images/'
    # base_uri = '/Users/todorlubenov/cytora_data/bulk_emails_processing/markel_manual_done/attachments/'
    # suffix = '.doc'
    # suffix = '.docx'

    suffix = '.pdf'
    suff = suffix[1:]
    processing(base_destination, base_uri, suffix)


